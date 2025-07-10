mod test_helpers;

use std::path;

use anyhow::Result;

use aws_sdk_s3::Client;
use test_helpers::{
    check_consolidation_with_config, check_output_bucket_not_empty, AwsS3Client,
    ConsolidatorRunner, ContainerConfig, S3FileDisplayer, TestEnvironment,
};

/// Convert MockDataGenerator date/hour to consolidator target time format
/// Converts "20231225" + "14" to "2023-12-25T14:00:00+00:00"
fn generate_target_time(date: &str, hour: &str) -> String {
    let year = &date[0..4];
    let month = &date[4..6];
    let day = &date[6..8];
    format!("{}-{}-{}T{}:00:00+00:00", year, month, day, hour)
}

#[tokio::test]
async fn test_check_consolidation_with_consolidator() -> Result<()> {
    let test_dataset = "simple-001".to_string();
    let test_env = TestEnvironment::create(test_dataset.clone()).await?;

    let target_time = generate_target_time("20231225", "14");

    test_env.populate_inputs_buckets().await;

    let s3_client = Client::new(&test_env.client);
    let aws_s3_client = AwsS3Client::new(s3_client.clone());
    let file_displayer = S3FileDisplayer::new(aws_s3_client);

    // Display input bucket contents
    let input_buckets = ["input-a", "input-b", "input-c"];
    file_displayer
        .display_multiple_buckets(&input_buckets, "input")
        .await?;

    let absolute_path = path::absolute("tests/mock_data/config.yaml")?;
    let absolute_path = absolute_path.display().to_string();
    let minio_network = test_env.container.get_bridge_ip_address().await?;

    let container_config = ContainerConfig {
        image: "log-consolidator".to_string(),
        tag: "dev".to_string(),
        config_path: absolute_path,
        target_time,
        minio_endpoint: format!("http://{}:9000", minio_network),
        sleep_duration_secs: 5,
    };

    let consolidator_runner = ConsolidatorRunner::new(&container_config);
    consolidator_runner.run().await?;

    let result_buckets = "output-a";
    let aws_s3_client_output = AwsS3Client::new(s3_client.clone());

    check_output_bucket_not_empty(&aws_s3_client_output, result_buckets).await?;

    let output_file_displayer = S3FileDisplayer::new(aws_s3_client_output);
    output_file_displayer
        .display_bucket_files(result_buckets, "consolidated")
        .await?;

    let config = test_env.config.clone();
    let consolidation_result =
        check_consolidation_with_config(test_dataset, &config, &test_env).await?;

    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_consolidator_line_return() -> Result<()> {
    let test_dataset = "line-return-001".to_string();
    let test_env = TestEnvironment::create(test_dataset.clone()).await?;

    let target_time = generate_target_time("20231225", "14");

    test_env.populate_inputs_buckets().await;

    let s3_client = Client::new(&test_env.client);
    let aws_s3_client = AwsS3Client::new(s3_client.clone());
    let file_displayer = S3FileDisplayer::new(aws_s3_client);

    // Display input bucket contents
    let input_buckets = ["input-a", "input-b", "input-c"];
    file_displayer
        .display_multiple_buckets(&input_buckets, "input")
        .await?;

    let absolute_path = path::absolute("tests/mock_data/config.yaml")?;
    let absolute_path = absolute_path.display().to_string();
    let minio_network = test_env.container.get_bridge_ip_address().await?;

    let container_config = ContainerConfig {
        image: "log-consolidator".to_string(),
        tag: "dev".to_string(),
        config_path: absolute_path,
        target_time,
        minio_endpoint: format!("http://{}:9000", minio_network),
        sleep_duration_secs: 5,
    };

    let consolidator_runner = ConsolidatorRunner::new(&container_config);
    consolidator_runner.run().await?;

    let result_buckets = "output-a";
    let aws_s3_client_output = AwsS3Client::new(s3_client.clone());

    check_output_bucket_not_empty(&aws_s3_client_output, result_buckets).await?;

    let output_file_displayer = S3FileDisplayer::new(aws_s3_client_output);
    output_file_displayer
        .display_bucket_files(result_buckets, "consolidated")
        .await?;

    let config = test_env.config.clone();
    let consolidation_result =
        check_consolidation_with_config(test_dataset, &config, &test_env).await?;

    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}
