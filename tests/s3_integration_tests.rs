mod test_helpers;

use anyhow::Result;
use aws_sdk_s3::Client;

use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
};
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use test_helpers::{TestConstants, TestEnvironment};

#[tokio::test]
async fn test_archiving_logs() -> Result<()> {
    let test = TestEnvironment::create().await?;
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
    let test_env = TestEnvironment::create().await?;

    // Load config
    let config = load_config(TestConstants::MOCK_CONFIG_PATH)?;

    // Create wrapped S3 client using the test environment's client
    let s3_client = Client::new(&test_env.client);
    let wrapped_s3_client = WrappedS3Client::new(TestConstants::DEFAULT_REGION, 15, Some(s3_client)).await?;

    // Create checker
    let checker = Checker::new(wrapped_s3_client, 4, Some(2), 128); // Small settings for test

    // Get bucket configs
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets
        .first()
        .ok_or_else(|| anyhow::anyhow!("No consolidated bucket found"))?;

    // Run the check for the date/hour in our test data (20231225/14)
    let date = "20231225".to_string();
    let hour = "14".to_string();

    println!("Running consolidation check for {}/{}", date, hour);

    // Debug: List what's in each bucket before running the check
    println!("Listing bucket contents before check:");
    for bucket_config in &archived_buckets {
        println!("Archived bucket: {}", bucket_config.bucket);
        println!("  Path config: {:?}", bucket_config.path);

        // Generate expected key prefix
        let formatter =
            log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
                bucket_config,
            );
        let expected_key_prefix = formatter(&date, &hour)?;
        println!("  Expected key prefix: {}", expected_key_prefix);

        let file_list = checker
            .list_bucket_files(bucket_config, &date, &hour)
            .await?;
        println!(
            "  Found {} files in bucket {}",
            file_list.files.len(),
            bucket_config.bucket
        );
        for file in &file_list.files {
            println!("    - {} (size: {} bytes)", file.key, file.size);
        }
    }

    println!("Consolidated bucket: {}", consolidated_bucket.bucket);
    println!("  Path config: {:?}", consolidated_bucket.path);
    let formatter = log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
        consolidated_bucket,
    );
    let expected_key_prefix = formatter(&date, &hour)?;
    println!("  Expected key prefix: {}", expected_key_prefix);

    let consolidated_file_list = checker
        .list_bucket_files(consolidated_bucket, &date, &hour)
        .await?;
    println!(
        "  Found {} files in consolidated bucket {}",
        consolidated_file_list.files.len(),
        consolidated_bucket.bucket
    );
    for file in &consolidated_file_list.files {
        println!("    - {} (size: {} bytes)", file.key, file.size);
    }

    // Assertions: Verify that files were found in all buckets
    let mut bucket_file_results = Vec::new();
    for (i, bucket_config) in archived_buckets.iter().enumerate() {
        let files = checker
            .list_bucket_files(bucket_config, &date, &hour)
            .await?;
        bucket_file_results.push((i, bucket_config, files));
    }
    let consolidated_files = checker
        .list_bucket_files(consolidated_bucket, &date, &hour)
        .await?;

    // Assert that input buckets contain files and have correct prefixes
    for (i, bucket_config, files) in &bucket_file_results {
        assert!(
            files.files.len() > 0,
            "Input bucket {} ({}) should contain files",
            i,
            bucket_config.bucket
        );

        // Generate expected prefix dynamically from config
        let formatter =
            log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
                bucket_config,
            );
        let expected_prefix = formatter(&date, &hour)?;

        for file in &files.files {
            assert!(
                file.key.starts_with(&expected_prefix),
                "File in bucket {} should have prefix '{}', but found: {}",
                bucket_config.bucket,
                expected_prefix,
                file.key
            );
        }
    }

    // Assert consolidated bucket files have correct prefix
    let consolidated_formatter =
        log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
            consolidated_bucket,
        );
    let expected_consolidated_prefix = consolidated_formatter(&date, &hour)?;
    assert!(
        consolidated_files.files.len() > 0,
        "Consolidated bucket should contain files"
    );
    for file in &consolidated_files.files {
        assert!(
            file.key.starts_with(&expected_consolidated_prefix),
            "Consolidated file should have prefix '{}', but found: {}",
            expected_consolidated_prefix,
            file.key
        );
    }

    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date, &hour)
        .await?;

    println!("Check result: ok={}, message={}", result.ok, result.message);

    // Assert that the comparison completed (result should be either true or false, not error)
    assert!(result.message.len() > 0, "Result should have a message");
    assert!(result.date == date, "Result date should match input date");
    assert!(result.hour == hour, "Result hour should match input hour");

    // Upload result to bucketsCheckerResults (output-b)
    let results_bucket = get_results_bucket(&config);
    if let Some(results_bucket_config) = results_bucket {
        let s3_client = Client::new(&test_env.client);

        // Create result JSON
        let result_json = serde_json::json!({
            "date": result.date,
            "hour": result.hour,
            "ok": result.ok,
            "message": result.message,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date
        });

        // Generate S3 key for result
        let result_key = format!(
            "check-results/dt={}/h={}/check-result-{}-{}.json",
            date, hour, date, hour
        );

        // Upload to S3
        s3_client
            .put_object()
            .bucket(&results_bucket_config.bucket)
            .key(&result_key)
            .body(result_json.to_string().into_bytes().into())
            .content_type("application/json")
            .send()
            .await?;

        println!(
            "Uploaded check result to bucket {} with key {}",
            results_bucket_config.bucket, result_key
        );

        // Verify the result was uploaded
        let objects = s3_client
            .list_objects_v2()
            .bucket(&results_bucket_config.bucket)
            .prefix("check-results/")
            .send()
            .await?;

        let contents = objects.contents();
        println!(
            "Results bucket {} now contains {} objects:",
            results_bucket_config.bucket,
            contents.len()
        );
        for obj in contents {
            if let Some(key) = obj.key() {
                println!("  - {}", key);
            }
        }

        // Assert that the result was properly uploaded
        assert!(
            contents.len() >= 1,
            "Results bucket should contain at least 1 object"
        );
        let uploaded_result = contents
            .iter()
            .find(|obj| obj.key().map_or(false, |key| key.contains(&result_key)));
        assert!(
            uploaded_result.is_some(),
            "Uploaded result should be found in results bucket with key: {}",
            result_key
        );

        // Verify the uploaded result content
        let get_result = s3_client
            .get_object()
            .bucket(&results_bucket_config.bucket)
            .key(&result_key)
            .send()
            .await?;

        let body = get_result.body.collect().await?;
        let uploaded_json: serde_json::Value = serde_json::from_slice(&body.into_bytes())?;

        // Display the complete result file content
        println!("\nResult file content:");
        println!("File: s3://{}/{}", results_bucket_config.bucket, result_key);
        println!("{}", serde_json::to_string_pretty(&uploaded_json)?);

        // Assert uploaded JSON structure
        assert!(
            uploaded_json["ok"].is_boolean(),
            "Uploaded result should have 'ok' boolean field"
        );
        assert!(
            uploaded_json["date"].is_string(),
            "Uploaded result should have 'date' string field"
        );
        assert!(
            uploaded_json["hour"].is_string(),
            "Uploaded result should have 'hour' string field"
        );
        assert!(
            uploaded_json["message"].is_string(),
            "Uploaded result should have 'message' string field"
        );
        assert_eq!(
            uploaded_json["date"].as_str().unwrap(),
            date,
            "Uploaded date should match test date"
        );
        assert_eq!(
            uploaded_json["hour"].as_str().unwrap(),
            hour,
            "Uploaded hour should match test hour"
        );

        println!("✓ All assertions passed - test completed successfully!");
    } else {
        panic!("Results bucket configuration not found!");
    }

    Ok(())
}
