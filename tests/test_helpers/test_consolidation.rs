use anyhow::Result;
use aws_sdk_s3::Client;
use serde_json;

use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
};
use log_consolidator_checker_rust::config::types::BucketConfig;
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;

use super::{TestConstants, TestEnvironment};

#[derive(Debug)]
pub struct ConsolidationResult {
    pub ok: bool,
    pub message: String,
    pub date: String,
    pub hour: String,
    pub uploaded_result_key: String,
}

pub async fn check_consolidation(test_dataset: String) -> Result<ConsolidationResult> {
    check_consolidation_with_config(test_dataset, TestConstants::MOCK_CONFIG_PATH).await
}

pub async fn check_consolidation_with_config(
    test_dataset: String,
    config_path: &str,
) -> Result<ConsolidationResult> {
    let test_env = TestEnvironment::create(test_dataset.clone()).await?;

    // Load config
    let config = load_config(config_path)?;
    // TODO: externalize this
    // test_env.populate_all_buckets();

    // Create wrapped S3 client using the test environment's client
    let s3_client = Client::new(&test_env.client);
    let wrapped_s3_client =
        WrappedS3Client::new(TestConstants::DEFAULT_REGION, 15, Some(s3_client.clone())).await?;

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

    println!(
        "[{}] Running consolidation check for {}/{}",
        test_dataset, date, hour
    );

    // Debug: List what's in each bucket before running the check
    println!("[{}] Listing bucket contents before check:", test_dataset);
    for bucket_config in &archived_buckets {
        println!(
            "[{}] Archived bucket: {}",
            test_dataset, bucket_config.bucket
        );
        println!("[{}]   Path config: {:?}", test_dataset, bucket_config.path);

        // Generate expected key prefix
        let formatter =
            log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
                bucket_config,
            );
        let expected_key_prefix = formatter(&date, &hour)?;
        println!(
            "[{}]   Expected key prefix: {}",
            test_dataset, expected_key_prefix
        );

        let file_list = checker
            .list_bucket_files(bucket_config, &date, &hour)
            .await?;
        println!(
            "[{}]   Found {} files in bucket {}",
            test_dataset,
            file_list.files.len(),
            bucket_config.bucket
        );
        for file in &file_list.files {
            println!(
                "[{}]     - {} (size: {} bytes)",
                test_dataset, file.key, file.size
            );
        }
    }

    println!(
        "[{}] Consolidated bucket: {}",
        test_dataset, consolidated_bucket.bucket
    );
    println!(
        "[{}]   Path config: {:?}",
        test_dataset, consolidated_bucket.path
    );
    let formatter = log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
        consolidated_bucket,
    );
    let expected_key_prefix = formatter(&date, &hour)?;
    println!("  Expected key prefix: {}", expected_key_prefix);

    let consolidated_file_list = checker
        .list_bucket_files(consolidated_bucket, &date, &hour)
        .await?;
    println!(
        "[{}]   Found {} files in consolidated bucket {}",
        test_dataset,
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

    // Check that at least some input buckets contain files and have correct prefixes
    let total_input_files: usize = bucket_file_results
        .iter()
        .map(|(_, _, files)| files.files.len())
        .sum();

    for (i, bucket_config, files) in &bucket_file_results {
        if files.files.len() == 0 {
            println!(
                "[{}] WARNING: Input bucket {} ({}) contains no files",
                test_dataset, i, bucket_config.bucket
            );
        }
    }

    // Only fail if ALL input buckets are empty
    if total_input_files == 0 {
        println!("[{}] ERROR: ALL input buckets are empty", test_dataset);
        return Ok(ConsolidationResult {
            ok: false,
            message: "All input buckets are empty".to_string(),
            date: date.clone(),
            hour: hour.clone(),
            uploaded_result_key: "".to_string(),
        });
    }

    // Validate file prefixes for non-empty buckets
    for (i, bucket_config, files) in &bucket_file_results {
        if files.files.len() > 0 {
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
    }

    // Check consolidated bucket files have correct prefix
    let consolidated_formatter =
        log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter(
            consolidated_bucket,
        );
    let expected_consolidated_prefix = consolidated_formatter(&date, &hour)?;
    if consolidated_files.files.len() == 0 {
        println!(
            "[{}] WARNING: Consolidated bucket contains no files",
            test_dataset
        );
        return Ok(ConsolidationResult {
            ok: false,
            message: "Consolidated bucket contains no files".to_string(),
            date: date.clone(),
            hour: hour.clone(),
            uploaded_result_key: "".to_string(),
        });
    }
    for file in &consolidated_files.files {
        assert!(
            file.key.starts_with(&expected_consolidated_prefix),
            "Consolidated file should have prefix '{}', but found: {}",
            expected_consolidated_prefix,
            file.key
        );
    }

    // Filter out empty buckets before passing to checker
    let non_empty_buckets: Vec<&BucketConfig> = bucket_file_results
        .iter()
        .filter(|(_, _, files)| files.files.len() > 0)
        .map(|(_, bucket_config, _)| **bucket_config)
        .collect();

    println!(
        "[{}] Passing {} non-empty buckets to checker (filtered from {} total)",
        test_dataset,
        non_empty_buckets.len(),
        archived_buckets.len()
    );

    let result = checker
        .get_comparison_results(&non_empty_buckets, consolidated_bucket, &date, &hour)
        .await?;

    println!(
        "[{}] Check result: ok={}, message={}",
        test_dataset, result.ok, result.message
    );

    // Assert that the comparison completed (result should be either true or false, not error)
    assert!(result.message.len() > 0, "Result should have a message");
    assert!(result.date == date, "Result date should match input date");
    assert!(result.hour == hour, "Result hour should match input hour");

    // Upload result to bucketsCheckerResults (output-b)
    let results_bucket = get_results_bucket(&config);
    assert!(
        results_bucket.is_some(),
        "Result hour should match input hour"
    );

    if let Some(results_bucket_config) = results_bucket {
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
            "[{}] Uploaded check result to bucket {} with key {}",
            test_dataset, results_bucket_config.bucket, result_key
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
            "[{}] Results bucket {} now contains {} objects:",
            test_dataset,
            results_bucket_config.bucket,
            contents.len()
        );
        for obj in contents {
            if let Some(key) = obj.key() {
                println!("[{}]   - {}", test_dataset, key);
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
        println!("[{}] Result file content:", test_dataset);
        println!(
            "[{}] File: s3://{}/{}",
            test_dataset, results_bucket_config.bucket, result_key
        );
        println!(
            "[{}] {}",
            test_dataset,
            serde_json::to_string_pretty(&uploaded_json)?
        );

        // Assert uploaded JSON structure
        assert!(
            uploaded_json["ok"].as_bool().is_some(),
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

        println!(
            "[{}] ✓ All assertions passed - test completed successfully!", t
           est_dataset);

        println!("[{}] Metrics:", test_dataset);
        println!("[{}]   - Checked result: {}\n", test_dataset, uploaded_json);

        Ok(ConsolidationResult {
            ok: result.ok,
            message: result.message.clone(),
            date: result.date.clone(),
            hour: result.hour.clone(),
            uploaded_result_key: result_key,
        })
    } else {
        Err(anyhow::anyhow!("Results bucket configuration not found!"))
    }
}
