mod test_helpers;

use anyhow::Result;
use aws_sdk_s3::Client;

use test_helpers::test_consolidation::check_consolidation;
use test_helpers::TestEnvironment;

#[tokio::test]
async fn test_archiving_logs() -> Result<()> {
    let test_dataset = "simple-001".to_string();
    let test = TestEnvironment::create(test_dataset).await?;
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
    let consolidation_result = check_consolidation("simple-001".to_string()).await?;

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
    let line_return_result = check_consolidation("line-return-001".to_string()).await?;
    let without_line_return_result =
        check_consolidation("without-line-return-001".to_string()).await?;
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
