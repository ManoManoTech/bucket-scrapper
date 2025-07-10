mod test_helpers;

use anyhow::Result;
use aws_sdk_s3::Client;

use test_helpers::test_consolidation::{check_consolidation, check_consolidation_with_config};
use test_helpers::{TestEnvironment, TestConstants};
use log_consolidator_checker_rust::config::loader::load_config;

#[tokio::test]
async fn test_archiving_logs() -> Result<()> {
    let test_dataset = "simple-001".to_string();
    let test = TestEnvironment::create(test_dataset).await?;
    test.populate_all_buckets();
    let s3_client = Client::new(&test.client);

    // Test that buckets have been created and contain mounted data
    for bucket in &test.inputs_buckets {
        let objects = s3_client.list_objects_v2().bucket(bucket).send().await?;

        let contents = objects.contents();
        println!("Bucket {} contains {} objects", bucket, contents.len());
        for obj in contents {
            if let Some(key) = obj.key() {
                println!("  - {}", key);
            }
        }
    }

    for bucket in &test.outputs_buckets {
        let objects = s3_client.list_objects_v2().bucket(bucket).send().await?;

        let contents = objects.contents();
        println!("Bucket {} contains {} objects", bucket, contents.len());
        for obj in contents {
            if let Some(key) = obj.key() {
                println!("  - {}", key);
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_check_consolidation() -> Result<()> {
    let test_dataset = "simple-001".to_string();
    let test_env = TestEnvironment::create(test_dataset.clone()).await?;
    test_env.populate_all_buckets().await;
    
    let config = load_config(TestConstants::MOCK_CONFIG_PATH)?;
    let consolidation_result = check_consolidation_with_config(test_dataset, &config, &test_env).await?;

    // Assert consolidation completed successfully
    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    assert_eq!(
        consolidation_result.date, "20231225",
        "Result date should match expected date"
    );
    assert_eq!(
        consolidation_result.hour, "14",
        "Result hour should match expected hour"
    );
    assert!(
        !consolidation_result.uploaded_result_key.is_empty(),
        "Result should have been uploaded to S3"
    );

    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_line_return() -> Result<()> {
    let test_dataset_1 = "line-return-001".to_string();
    let test_env_1 = TestEnvironment::create(test_dataset_1.clone()).await?;
    test_env_1.populate_all_buckets().await;
    let config_1 = load_config(TestConstants::MOCK_CONFIG_PATH)?;
    let line_return_result = check_consolidation_with_config(test_dataset_1, &config_1, &test_env_1).await?;
    
    let test_dataset_2 = "without-line-return-001".to_string();
    let test_env_2 = TestEnvironment::create(test_dataset_2.clone()).await?;
    test_env_2.populate_all_buckets().await;
    let config_2 = load_config(TestConstants::MOCK_CONFIG_PATH)?;
    let without_line_return_result = check_consolidation_with_config(test_dataset_2, &config_2, &test_env_2).await?;
    assert!(
        without_line_return_result.ok,
        "Consolidation check should succeed. Message: {}",
        without_line_return_result.message
    );
    assert!(
        line_return_result.ok,
        "Consolidation check should succeed. Message: {}",
        line_return_result.message
    );
    assert!(
        !without_line_return_result.uploaded_result_key.is_empty(),
        "Result should have been uploaded to S3"
    );
    assert!(
        !line_return_result.uploaded_result_key.is_empty(),
        "Result should have been uploaded to S3"
    );

    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_broken_inputs() -> Result<()> {
    let consolidation_result = check_consolidation("broken-input-001".to_string()).await?;

    // Assert consolidation completed successfully
    assert!(
        consolidation_result.ok == false,
        "Consolidation check should failed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_broken_outputs() -> Result<()> {
    let consolidation_result = check_consolidation("broken-output-001".to_string()).await?;

    // Assert consolidation completed successfully
    assert!(
        consolidation_result.ok == false,
        "Consolidation check should failed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}
