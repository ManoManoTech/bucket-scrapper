mod test_helpers;

use std::path;

use anyhow::Result;

use aws_sdk_s3::Client;
use log_consolidator_checker_rust::config::loader::load_config;
use test_helpers::{
    check_consolidation, check_consolidation_with_config, mock_data_generator::MockDataGenerator,
    TestConstants, TestEnvironment,
};
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

    // List and display input bucket contents
    let input_buckets = ["input-a", "input-b", "input-c"];
    for bucket in input_buckets {
        let objects_in = s3_client.list_objects().bucket(bucket).send().await?;
        println!(
            "Objects in input bucket {}: {:?}",
            bucket,
            objects_in.contents()
        );

        for object in objects_in.contents() {
            if let Some(key) = object.key() {
                println!("Processing input file: {}", key);

                let get_object_response = s3_client
                    .get_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await?;

                let body = get_object_response.body.collect().await?;
                let data = body.into_bytes();

                if key.ends_with(".gz") {
                    use flate2::read::GzDecoder;
                    let mut decoder = GzDecoder::new(data.as_ref());
                    let mut decompressed = Vec::new();
                    std::io::copy(&mut decoder, &mut decompressed)?;

                    let content = String::from_utf8_lossy(&decompressed);
                    println!("Input content {} ({} bytes):", key, decompressed.len());
                    println!("{}", content);
                } else {
                    let content = String::from_utf8_lossy(&data);
                    println!("Input content {} ({} bytes):", key, data.len());
                    println!("{}", content);
                }

                println!("--- End of input file {} ---\n", key);
            }
        }
    }

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
    // TODO: replace it by a callback in log consumer
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // BUG: check why this is not the good bucket int
    // let result_buckets = test_env.result_buckets.clone().pop().unwrap();
    let result_buckets = "output-a";
    let objects = s3_client.list_objects().bucket("output-a").send().await?;
    println!("======Objects in output buckets: {:?}", objects.contents());
    if objects.contents().is_empty() {
        panic!("No objects found in the results bucket: {}", result_buckets);
    }

    let config = load_config(TestConstants::MOCK_CONFIG_PATH).unwrap();
    for object in objects.contents() {
        if let Some(key) = object.key() {
            println!("Downloading consolidated file: {}", key);

            let get_object_response = s3_client
                .get_object()
                .bucket(result_buckets)
                .key(key)
                .send()
                .await?;

            let body = get_object_response.body.collect().await?;
            let data = body.into_bytes();

            if key.ends_with(".zst") {
                let mut decompressor = zstd::Decoder::new(data.as_ref())
                    .map_err(|e| anyhow::anyhow!("Erreur zstd: {}", e))?;

                let mut decompressed = Vec::new();
                std::io::copy(&mut decompressor, &mut decompressed)?;

                let content = String::from_utf8_lossy(&decompressed);
                println!(
                    "Decompressed content {} ({} bytes):",
                    key,
                    decompressed.len()
                );
                println!("{}", content);
            } else if key.ends_with(".gz") {
                use flate2::read::GzDecoder;
                let mut decoder = GzDecoder::new(data.as_ref());
                let mut decompressed = Vec::new();
                std::io::copy(&mut decoder, &mut decompressed)?;

                let content = String::from_utf8_lossy(&decompressed);
                println!(
                    "Decompressed content {} ({} bytes):",
                    key,
                    decompressed.len()
                );
                println!("{}", content);
            } else {
                let content = String::from_utf8_lossy(&data);
                println!("Decompressed content {} ({} bytes):", key, data.len());
                println!("{}", content);
            }

            println!("--- End of file {} ---\n", key);
        }
    }

    // TODO: run checker
    let consolidation_result =
        check_consolidation_with_config(test_dataset, &config, &test_env).await?;

    assert!(
        consolidation_result.ok,
        "Consolidation check should succeed. Message: {}",
        consolidation_result.message
    );
    Ok(())
}
