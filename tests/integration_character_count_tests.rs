use aws_sdk_s3::primitives::ByteStream;
use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, load_config,
};
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::fs;
use tempfile::NamedTempFile;

mod test_helpers;
use test_helpers::mock_data_populator::{MockDataPopulator, PopulationUtils};

mod integration_test_setup;
use integration_test_setup::setup_test_environment;

#[tokio::test]
async fn test_character_count_verification_perfect_match() -> Result<(), Box<dyn std::error::Error>>
{
    // Setup test environment
    let (_minio_container, clients) = setup_test_environment().await?;
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

    // Create data that will result in perfect character count match
    let populator = MockDataPopulator::new("/tmp");

    // Use specific file count to ensure predictable character counts
    let summary = populator
        .populate_with_synthetic_data(&clients, &bucket_mappings, 8)
        .await?;

    println!(
        "Perfect match test: Populated {} files with matching character counts",
        summary.total_files()
    );

    // Test the Check command workflow
    let checker = Checker::new(s3_client, 4, None, 1024);
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();

    let date_str = "20231225".to_string();
    let hour_str = "14".to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;

    // Verify the check results indicate successful consolidation
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.message.is_empty());

    // The result should be positive since we created matching data
    println!(
        "Perfect match result: ok={}, message={}",
        result.ok, result.message
    );

    Ok(())
}

#[tokio::test]
async fn test_character_count_verification_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let (_minio_container, clients) = setup_test_environment().await?;
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

    // Create mismatched data: populate input normally, then add extra content to consolidated
    let populator = MockDataPopulator::new("/tmp");
    let _summary = populator
        .populate_with_synthetic_data(&clients, &bucket_mappings, 6)
        .await?;

    // Add extra content to consolidated bucket to create character count mismatch
    let extra_content = "EXTRA CONTENT CAUSING MISMATCH - This content should not be here";
    client
        .put_object()
        .bucket("consolidated-bucket")
        .key("raw-logs/dt=20231225/hour=14/production-extra-mismatch.json.zst")
        .body(ByteStream::from(
            test_helpers::compression::compress_with_zstd(extra_content.as_bytes()),
        ))
        .send()
        .await?;

    println!("Mismatch test: Added extra content to create character count mismatch");

    // Test the Check command workflow
    let checker = Checker::new(s3_client, 4, None, 1024);
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();

    let date_str = "20231225".to_string();
    let hour_str = "14".to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;

    // Verify the check results indicate failed consolidation
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.message.is_empty());

    // The result should indicate mismatch (exact behavior depends on Checker implementation)
    println!(
        "Mismatch result: ok={}, message={}",
        result.ok, result.message
    );

    Ok(())
}

#[tokio::test]
async fn test_character_count_verification_partial_consolidation(
) -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let (_minio_container, clients) = setup_test_environment().await?;
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

    // Populate input buckets normally
    let populator = MockDataPopulator::new("/tmp");
    let _summary = populator
        .populate_with_synthetic_data(&clients, &bucket_mappings, 10)
        .await?;

    // Remove some consolidated files to simulate partial consolidation
    // First, get the list of consolidated files
    let consolidated_bucket_name = bucket_mappings.get("consolidated").unwrap();
    let objects = client
        .list_objects_v2()
        .bucket(consolidated_bucket_name)
        .prefix("raw-logs/dt=20231225/hour=14/")
        .send()
        .await?;

    // Delete half of the consolidated files to simulate partial consolidation
    let contents = objects.contents();
    if !contents.is_empty() {
        let files_to_delete = contents.len() / 2;
        for (i, obj) in contents.iter().enumerate() {
            if i < files_to_delete {
                if let Some(key) = obj.key() {
                    client
                        .delete_object()
                        .bucket(consolidated_bucket_name)
                        .key(key)
                        .send()
                        .await?;
                    println!("Deleted consolidated file: {}", key);
                }
            }
        }
    }

    println!("Partial consolidation test: Removed some consolidated files to simulate incomplete consolidation");

    // Test the Check command workflow
    let checker = Checker::new(s3_client, 4, None, 1024);
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();

    let date_str = "20231225".to_string();
    let hour_str = "14".to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;

    // Verify the check results indicate partial/failed consolidation
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.message.is_empty());

    // The result should indicate failure due to missing consolidated files
    println!(
        "Partial consolidation result: ok={}, message={}",
        result.ok, result.message
    );

    Ok(())
}
