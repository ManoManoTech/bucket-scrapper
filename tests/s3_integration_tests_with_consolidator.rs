mod test_helpers;

use std::path;

use anyhow::Result;

use aws_sdk_s3::Client;
use test_helpers::{check_consolidation, mock_data_generator::MockDataGenerator, TestEnvironment};
use testcontainers::{
    core::{logs::LogFrame, Mount},
    runners::AsyncRunner,
    GenericImage, ImageExt,
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
    // NOTE: debug
    let objects_in = s3_client.list_objects().bucket("inputs-a").send().await?;
    println!("Objects in input buckets: {:?}", objects_in);

    let absolute_path = path::absolute("tests/mock_data/config.yaml")?;
    let absolute_path = absolute_path.display().to_string();

    let config_mount = Mount::bind_mount(&absolute_path, "/app/config.yaml");
    test_env.populate_inputs_buckets().await;

    let minio_network = test_env.container.get_bridge_ip_address().await?;

    let container = GenericImage::new("log-consolidator", "dev")
        .with_mount(config_mount)
        .with_env_var(
            "LOG_CONSOLIDATOR_S3_ENDPOINT",
            &format!("http://{}:9000", minio_network),
        )
        .with_env_var("LOG_CONSOLIDATOR_TARGET_SPEC_PATH", "/app/config.yaml")
        .with_env_var("LOG_CONSOLIDATOR_S3_ACCESS_KEY_ID", "minioadmin")
        .with_env_var("LOG_CONSOLIDATOR_S3_SECRET_ACCESS_KEY", "minioadmin")
        .with_env_var("LOG_CONSOLIDATOR_S3_REGION", "us-east-1")
        .with_env_var("LOG_CONSOLIDATOR_S3_FORCE_PATH_STYLE", "true")
        .with_env_var("LOG_CONSOLIDATOR_S3_NO_RETRY_INCREMENT", "10")
        .with_env_var("LOG_CONSOLIDATOR_S3_RETRY_COST", "1")
        .with_env_var("LOG_CONSOLIDATOR_S3_RETRY_TIMEOUT_COST", "2")
        .with_env_var("LOG_CONSOLIDATOR_TARGET_TIME", &target_time)
        .with_log_consumer(|frame: &LogFrame| {
            let mut msg = std::str::from_utf8(frame.bytes()).expect("Failed to parse log message");
            if msg.ends_with('\n') {
                msg = &msg[..msg.len() - 1];
            }
            println!("Consolidator container log: {msg}");

            if msg.contains("\"level\":\"error\"") || msg.contains("\"level\":\"fatal\"") {
                panic!("Consolidator failed with error: {}", msg);
            }
        })
        .start()
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // BUG: check why this is not the good bucket int
    // let result_buckets = test_env.result_buckets.clone().pop().unwrap();
    let result_buckets = "output-a";
    let objects = s3_client.list_objects().bucket("output-a").send().await?;
    println!(
        "======Objects in output buckets: {:?}",
        objects_in.contents()
    );

    if objects.contents().is_empty() {
        panic!("No objects found in the results bucket: {}", result_buckets);
    }

    let consolidation_result = check_consolidation("simple-001".to_string()).await?;
    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    // assert_eq!(
    //     consolidation_result.date, "20231225",
    //     "Result date should match expected date"
    // );
    // assert_eq!(
    //     consolidation_result.hour, "14",
    //     "Result hour should match expected hour"
    // );
    // assert!(
    //     !consolidation_result.uploaded_result_key.is_empty(),
    //     "Result should have been uploaded to S3"
    // );

    Ok(())
}
