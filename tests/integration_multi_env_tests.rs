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
async fn test_multi_environment_consolidation_patterns() -> Result<(), Box<dyn std::error::Error>> {
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

    // Create realistic multi-environment scenario
    let environments = ["production", "staging", "development"];
    let services = ["app", "web", "api", "db"];

    // Create input files for each env-service combination across multiple buckets
    let mut input_file_count = 0;
    for (env_idx, env) in environments.iter().enumerate() {
        for (svc_idx, service) in services.iter().enumerate() {
            let log_content = test_helpers::factories::JsonLogFactory::create_realistic(
                env,
                service,
                "info",
                &format!("Multi-env log from {} {}", env, service),
            );
            let compressed_content =
                test_helpers::compression::compress_with_gzip(log_content.as_bytes());

            // Distribute across input buckets
            let bucket_key = if (env_idx + svc_idx) % 2 == 0 {
                "input-0"
            } else {
                "input-1"
            };
            let bucket_name = bucket_mappings.get(bucket_key).unwrap();

            client
                .put_object()
                .bucket(bucket_name)
                .key(&format!(
                    "raw-logs/dt=20231225/hour=14/{}-{}-{:03}.log.gz",
                    env,
                    service,
                    env_idx * services.len() + svc_idx
                ))
                .body(ByteStream::from(compressed_content))
                .send()
                .await?;

            input_file_count += 1;
        }
    }

    // Create consolidated files grouped by env-service (realistic consolidation pattern)
    let consolidated_bucket = bucket_mappings.get("consolidated").unwrap();
    let mut consolidated_file_count = 0;

    for env in &environments {
        for service in &services {
            // Simulate consolidation: multiple input logs become one consolidated file per env-service
            let consolidated_logs = vec![
                test_helpers::factories::JsonLogFactory::create_realistic(
                    env,
                    service,
                    "info",
                    &format!("Consolidated {} {} log 1", env, service),
                ),
                test_helpers::factories::JsonLogFactory::create_realistic(
                    env,
                    service,
                    "info",
                    &format!("Consolidated {} {} log 2", env, service),
                ),
            ];

            let combined_content = consolidated_logs.join("\n");
            let compressed_content =
                test_helpers::compression::compress_with_zstd(combined_content.as_bytes());

            client
                .put_object()
                .bucket(consolidated_bucket)
                .key(&format!(
                    "raw-logs/dt=20231225/hour=14/{}-{}-consolidated.json.zst",
                    env, service
                ))
                .body(ByteStream::from(compressed_content))
                .send()
                .await?;

            consolidated_file_count += 1;
        }
    }

    println!("Multi-env test: Created {} input files and {} consolidated files for {} environments and {} services",
             input_file_count, consolidated_file_count, environments.len(), services.len());

    // Test the Check command workflow
    let s3_client_for_checker = WrappedS3Client::new("us-east-1", 15).await?;
    let checker = Checker::new(s3_client_for_checker, 8, None, 2048);
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
        "Multi-env consolidation result: ok={}, message={}",
        result.ok, result.message
    );

    // Verify expected file counts and patterns
    let s3_client_for_verification = WrappedS3Client::new("us-east-1", 15).await?;
    let mut total_input_files = 0;
    for bucket_config in &archived_buckets {
        let files = s3_client_for_verification
            .get_matching_filenames_from_s3(bucket_config, &date_str, &hour_str, true)
            .await?;
        total_input_files += files.files.len();

        // Verify input file naming patterns include environment and service
        for file in &files.files {
            assert!(
                file.key.contains("production-")
                    || file.key.contains("staging-")
                    || file.key.contains("development-")
            );
            assert!(
                file.key.contains("-app-")
                    || file.key.contains("-web-")
                    || file.key.contains("-api-")
                    || file.key.contains("-db-")
            );
        }
    }
    assert_eq!(total_input_files, input_file_count);

    // Verify consolidated files follow env-service grouping pattern
    let consolidated_files = s3_client_for_verification
        .get_matching_filenames_from_s3(consolidated_bucket, &date_str, &hour_str, true)
        .await?;
    assert_eq!(consolidated_files.files.len(), consolidated_file_count);

    for file in &consolidated_files.files {
        assert!(file.key.contains("-consolidated.json.zst"));
        // Should have one consolidated file per env-service combination
        assert!(file.key.matches('-').count() >= 2); // env-service-consolidated pattern
    }

    println!("Multi-env test: Successfully verified {} environments × {} services = {} consolidation groups", 
             environments.len(), services.len(), consolidated_file_count);

    Ok(())
}
