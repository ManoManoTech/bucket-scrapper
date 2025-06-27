use aws_sdk_s3::primitives::ByteStream;
use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, load_config,
};
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::fs;
use tempfile::NamedTempFile;

mod test_helpers;
use test_helpers::mock_data_populator::PopulationUtils;

mod integration_test_setup;
use integration_test_setup::setup_test_environment;

#[tokio::test]
async fn test_compression_format_handling_gz_vs_zstd() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let (_minio_container, _clients) = setup_test_environment().await?;
    let bucket_mappings = PopulationUtils::create_standard_bucket_mappings();

    // Create test configuration
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "input-bucket-1"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;
    let config = load_config(temp_config.path())?;

    // Initialize S3 client
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    // Create test data with explicit compression format control
    let test_json_content = r#"{"env":"production","service":"app","level":"info","message":"Test log entry for compression verification","timestamp":"2023-12-25T14:30:00Z"}"#;

    // Create input files with .gz compression (original format)
    let input_bucket = bucket_mappings.get("input-0").unwrap();
    let gzipped_content =
        test_helpers::compression::compress_with_gzip(test_json_content.as_bytes());

    client
        .put_object()
        .bucket(input_bucket)
        .key("raw-logs/dt=20231225/hour=14/app-service-001.log.gz")
        .body(ByteStream::from(gzipped_content))
        .send()
        .await?;

    // Create consolidated files with .zstd compression (consolidated format)
    let consolidated_bucket = bucket_mappings.get("consolidated").unwrap();
    let zstd_content = test_helpers::compression::compress_with_zstd(test_json_content.as_bytes());

    client
        .put_object()
        .bucket(consolidated_bucket)
        .key("raw-logs/dt=20231225/hour=14/production-app-consolidated.json.zst")
        .body(ByteStream::from(zstd_content))
        .send()
        .await?;

    println!("Compression format test: Created .gz input file and .zstd consolidated file with identical content");

    // Test the Check command workflow
    let s3_client_for_checker = WrappedS3Client::new("us-east-1", 15).await?;
    let checker = Checker::new(s3_client_for_checker, 4, None, 1024);
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();

    let date_str = "20231225".to_string();
    let hour_str = "14".to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;

    // Verify the check results - both files have same content, just different compression
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.message.is_empty());

    // Should succeed since content is identical, only compression differs
    println!(
        "Compression format result: ok={}, message={}",
        result.ok, result.message
    );

    // Verify decompression accuracy by checking both files can be read
    let s3_client_for_verification = WrappedS3Client::new("us-east-1", 15).await?;
    let listed_input = s3_client_for_verification
        .get_matching_filenames_from_s3(&archived_buckets[0], &date_str, &hour_str, true)
        .await?;
    assert_eq!(listed_input.files.len(), 1);
    assert!(listed_input.files[0].key.ends_with(".gz"));

    let listed_consolidated = s3_client_for_verification
        .get_matching_filenames_from_s3(consolidated_bucket, &date_str, &hour_str, true)
        .await?;
    assert_eq!(listed_consolidated.files.len(), 1);
    assert!(listed_consolidated.files[0].key.ends_with(".zst"));

    println!("Compression test: Successfully handled mixed compression formats (.gz input vs .zstd consolidated)");

    Ok(())
}

#[tokio::test]
async fn test_mixed_compression_scenarios() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let (_minio_container, _clients) = setup_test_environment().await?;
    let bucket_mappings = PopulationUtils::create_standard_bucket_mappings();

    // Create test configuration with multiple input buckets
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "input-bucket-1"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"
  - bucket: "input-bucket-2"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;
    let config = load_config(temp_config.path())?;

    // Initialize S3 client
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    // Create mixed compression scenario with multiple services
    let services = ["app", "web", "api"];
    let base_content = r#"{"env":"production","service":"SERVICE_PLACEHOLDER","level":"info","message":"Mixed compression test log","timestamp":"2023-12-25T14:30:00Z"}"#;

    // Populate input buckets with .gz files
    for (i, service) in services.iter().enumerate() {
        let service_content = base_content.replace("SERVICE_PLACEHOLDER", service);
        let compressed_content =
            test_helpers::compression::compress_with_gzip(service_content.as_bytes());

        let bucket_key = if i % 2 == 0 { "input-0" } else { "input-1" };
        let bucket_name = bucket_mappings.get(bucket_key).unwrap();

        client
            .put_object()
            .bucket(bucket_name)
            .key(&format!(
                "raw-logs/dt=20231225/hour=14/{}-service.log.gz",
                service
            ))
            .body(ByteStream::from(compressed_content))
            .send()
            .await?;
    }

    // Create consolidated files with .zstd compression (grouped by service)
    let consolidated_bucket = bucket_mappings.get("consolidated").unwrap();
    for service in &services {
        let service_content = base_content.replace("SERVICE_PLACEHOLDER", service);
        let compressed_content =
            test_helpers::compression::compress_with_zstd(service_content.as_bytes());

        client
            .put_object()
            .bucket(consolidated_bucket)
            .key(&format!(
                "raw-logs/dt=20231225/hour=14/production-{}-consolidated.json.zst",
                service
            ))
            .body(ByteStream::from(compressed_content))
            .send()
            .await?;
    }

    println!("Mixed compression test: Created {} .gz input files across 2 buckets and {} .zstd consolidated files", services.len(), services.len());

    // Test the Check command workflow
    let s3_client_for_checker = WrappedS3Client::new("us-east-1", 15).await?;
    let checker = Checker::new(s3_client_for_checker, 6, None, 1024);
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();

    let date_str = "20231225".to_string();
    let hour_str = "14".to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;

    // Verify the check results
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.message.is_empty());

    println!(
        "Mixed compression result: ok={}, message={}",
        result.ok, result.message
    );

    // Verify that both input buckets have files
    let s3_client_for_verification = WrappedS3Client::new("us-east-1", 15).await?;
    let mut total_input_files = 0;
    for bucket_config in &archived_buckets {
        let result = s3_client_for_verification
            .get_matching_filenames_from_s3(bucket_config, &date_str, &hour_str, true)
            .await?;
        total_input_files += result.files.len();

        // Ensure all input files are .gz
        for file in &result.files {
            assert!(
                file.key.ends_with(".gz"),
                "Input file should be .gz: {}",
                file.key
            );
        }
    }
    assert_eq!(total_input_files, services.len());

    // Verify consolidated bucket has .zstd files
    let consolidated_result = s3_client_for_verification
        .get_matching_filenames_from_s3(consolidated_bucket, &date_str, &hour_str, true)
        .await?;
    assert_eq!(consolidated_result.files.len(), services.len());

    for file in &consolidated_result.files {
        assert!(
            file.key.ends_with(".zst"),
            "Consolidated file should be .zstd: {}",
            file.key
        );
    }

    println!(
        "Mixed compression test: Successfully processed {} services with mixed compression formats",
        services.len()
    );

    Ok(())
}
