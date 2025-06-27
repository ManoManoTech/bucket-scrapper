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
async fn test_service_specific_log_volume_patterns() -> Result<(), Box<dyn std::error::Error>> {
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

    // Create realistic service-specific volume patterns
    let service_configs = vec![
        ("app", 15, "High-volume application logs with many requests"),
        ("web", 8, "Medium-volume web server logs"),
        ("api", 12, "API gateway logs with various endpoints"),
        ("db", 3, "Low-volume database logs"),
        ("cache", 2, "Very low-volume cache service logs"),
    ];

    let input_bucket = bucket_mappings.get("input-0").unwrap();
    let consolidated_bucket = bucket_mappings.get("consolidated").unwrap();

    // Create input files with realistic volume differences per service
    let mut total_input_files = 0;
    let mut total_input_characters = 0;

    for (service, volume, description) in &service_configs {
        for i in 0..*volume {
            let log_content = test_helpers::factories::JsonLogFactory::create_realistic(
                "production",
                service,
                "info",
                &format!("{} - Entry #{}", description, i),
            );
            let compressed_content =
                test_helpers::compression::compress_with_gzip(log_content.as_bytes());

            client
                .put_object()
                .bucket(input_bucket)
                .key(&format!(
                    "raw-logs/dt=20231225/hour=14/{}-service-{:03}.log.gz",
                    service, i
                ))
                .body(ByteStream::from(compressed_content))
                .send()
                .await?;

            total_input_files += 1;
            total_input_characters += log_content.len();
        }
    }

    // Create consolidated files that group all logs per service
    let mut total_consolidated_characters = 0;
    for (service, volume, description) in &service_configs {
        let mut service_logs = Vec::new();
        for i in 0..*volume {
            service_logs.push(test_helpers::factories::JsonLogFactory::create_realistic(
                "production",
                service,
                "info",
                &format!("{} - Entry #{}", description, i),
            ));
        }

        let combined_content = service_logs.join("\n");
        let compressed_content =
            test_helpers::compression::compress_with_zstd(combined_content.as_bytes());

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

        total_consolidated_characters += combined_content.len();
    }

    println!("Service volume test: Created {} input files ({} chars) and {} consolidated files ({} chars)",
             total_input_files, total_input_characters, service_configs.len(), total_consolidated_characters);

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
        "Service volume result: ok={}, message={}",
        result.ok, result.message
    );

    // Verify volume distribution is reflected in file counts
    let s3_client_for_verification = WrappedS3Client::new("us-east-1", 15).await?;
    let input_files = s3_client_for_verification
        .get_matching_filenames_from_s3(&archived_buckets[0], &date_str, &hour_str, true)
        .await?;
    assert_eq!(input_files.files.len(), total_input_files);

    // Check that high-volume services have more files
    let app_files: Vec<_> = input_files
        .files
        .iter()
        .filter(|f| f.key.contains("app-service"))
        .collect();
    let db_files: Vec<_> = input_files
        .files
        .iter()
        .filter(|f| f.key.contains("db-service"))
        .collect();
    let cache_files: Vec<_> = input_files
        .files
        .iter()
        .filter(|f| f.key.contains("cache-service"))
        .collect();

    assert!(
        app_files.len() > db_files.len(),
        "App service should have more files than DB service"
    );
    assert!(
        db_files.len() > cache_files.len(),
        "DB service should have more files than cache service"
    );

    // Verify consolidated files exist for all services
    let consolidated_files = s3_client_for_verification
        .get_matching_filenames_from_s3(consolidated_bucket, &date_str, &hour_str, true)
        .await?;
    assert_eq!(consolidated_files.files.len(), service_configs.len());

    for (service, _volume, _description) in &service_configs {
        assert!(consolidated_files
            .files
            .iter()
            .any(|f| f.key.contains(&format!("{}-consolidated", service))));
    }

    println!(
        "Service volume test: Successfully verified realistic volume patterns across {} services",
        service_configs.len()
    );

    Ok(())
}
