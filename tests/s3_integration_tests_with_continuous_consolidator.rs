mod test_helpers;

use std::path;

use anyhow::Result;

use aws_sdk_s3::Client;
use test_helpers::{
    check_consolidation_with_config_and_date, check_output_bucket_not_empty, AwsS3Client,
    ConsolidatorRunner, ContainerConfig, DynamicMockGenerator, S3FileDisplayer, TestEnvironment,
};
use serde::{Deserialize};

#[derive(Debug, Deserialize)]
struct BucketConfig {
    bucket: String,
    path: Vec<serde_yaml::Value>,
    #[serde(default)]
    force_env: Option<String>,
    #[serde(default)]
    proceed_without_matching_objects: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(rename = "bucketsToConsolidate")]
    buckets_to_consolidate: Vec<BucketConfig>,
    #[serde(rename = "bucketsConsolidated")]
    buckets_consolidated: Vec<BucketConfig>,
    #[serde(rename = "bucketsCheckerResults")]
    buckets_checker_results: Vec<BucketConfig>,
}

fn load_config_buckets(config_path: &str) -> Result<Config> {
    let config_content = std::fs::read_to_string(config_path)?;
    let config: Config = serde_yaml::from_str(&config_content)?;
    Ok(config)
}

#[tokio::test]
async fn test_check_consolidation_with_continuous_consolidator() -> Result<()> {
    let test_dataset = "simple-001".to_string();
    let test_env = TestEnvironment::create(test_dataset.clone()).await?;

    let mock_generator = DynamicMockGenerator::new_for_continuous();
    let s3_client = Client::new(&test_env.client);
    mock_generator.populate_s3_for_continuous_mode(&s3_client).await?;
    let aws_s3_client = AwsS3Client::new(s3_client.clone());
    let file_displayer = S3FileDisplayer::new(aws_s3_client);

    let config_buckets = load_config_buckets("tests/mock_data/config.yaml")?;
    let input_bucket_names: Vec<&str> = config_buckets.buckets_to_consolidate
        .iter()
        .map(|b| b.bucket.as_str())
        .collect();
    file_displayer
        .display_multiple_buckets(&input_bucket_names, "input")
        .await?;

    let absolute_path = path::absolute("tests/mock_data/config.yaml")?;
    let absolute_path = absolute_path.display().to_string();
    let minio_network = test_env.container.get_bridge_ip_address().await?;

    let container_config = ContainerConfig {
        image: "log-consolidator".to_string(),
        tag: "dev".to_string(),
        config_path: absolute_path,
        target_time: String::new(),
        minio_endpoint: format!("http://{}:9000", minio_network),
        sleep_duration_secs: 10,
    };

    let consolidator_runner = ConsolidatorRunner::new(&container_config).with_continuous();
    consolidator_runner.run().await?;

    let result_buckets = &config_buckets.buckets_consolidated[0].bucket;
    let aws_s3_client_output = AwsS3Client::new(s3_client.clone());

    check_output_bucket_not_empty(&aws_s3_client_output, result_buckets).await?;

    let output_file_displayer = S3FileDisplayer::new(aws_s3_client_output);
    output_file_displayer
        .display_bucket_files(result_buckets, "consolidated")
        .await?;

    let (date, hour) = mock_generator.get_date_hour_strings();
    let config = test_env.config.clone();
    let consolidation_result =
        check_consolidation_with_config_and_date(test_dataset, &config, &test_env, &date, &hour).await?;

    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_continuous_consolidator_line_return() -> Result<()> {
    let test_dataset = "line-return-001".to_string();
    let test_env = TestEnvironment::create(test_dataset.clone()).await?;

    let mock_generator = DynamicMockGenerator::new_for_continuous();
    let s3_client = Client::new(&test_env.client);
    mock_generator.populate_s3_for_continuous_mode_line_returns(&s3_client).await?;
    let aws_s3_client = AwsS3Client::new(s3_client.clone());
    let file_displayer = S3FileDisplayer::new(aws_s3_client);

    let config_buckets = load_config_buckets("tests/mock_data/config.yaml")?;
    let input_bucket_names: Vec<&str> = config_buckets.buckets_to_consolidate
        .iter()
        .map(|b| b.bucket.as_str())
        .collect();
    file_displayer
        .display_multiple_buckets(&input_bucket_names, "input")
        .await?;

    let absolute_path = path::absolute("tests/mock_data/config.yaml")?;
    let absolute_path = absolute_path.display().to_string();
    let minio_network = test_env.container.get_bridge_ip_address().await?;

    let container_config = ContainerConfig {
        image: "log-consolidator".to_string(),
        tag: "dev".to_string(),
        config_path: absolute_path,
        target_time: String::new(),
        minio_endpoint: format!("http://{}:9000", minio_network),
        sleep_duration_secs: 10,
    };

    let consolidator_runner = ConsolidatorRunner::new(&container_config).with_continuous();
    consolidator_runner.run().await?;

    let result_buckets = &config_buckets.buckets_consolidated[0].bucket;
    let aws_s3_client_output = AwsS3Client::new(s3_client.clone());

    check_output_bucket_not_empty(&aws_s3_client_output, result_buckets).await?;

    let output_file_displayer = S3FileDisplayer::new(aws_s3_client_output);
    output_file_displayer
        .display_bucket_files(result_buckets, "consolidated")
        .await?;
    let (date, hour) = mock_generator.get_date_hour_strings();
    let config = test_env.config.clone();
    let consolidation_result =
        check_consolidation_with_config_and_date(test_dataset, &config, &test_env, &date, &hour).await?;

    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}
