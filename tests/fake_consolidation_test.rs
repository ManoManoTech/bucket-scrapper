mod test_helpers;

use anyhow::Result;
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use log_consolidator_checker_rust::utils::structured_log::BucketRole;
use test_helpers::fake_consolidation_setup::FakeConsolidationEnvironment;

#[tokio::test]
#[ignore = "ezci-impure"]
async fn test_fake_consolidation_with_real_files() -> Result<()> {
    println!("\n=== Testing with Rust Checker (Direct Function Call) ===\n");

    // Setup environment with shared code
    let env = FakeConsolidationEnvironment::setup().await?;

    // Create the checker
    let wrapped_s3_client =
        WrappedS3Client::new("us-east-1", 15, Some(env.s3_client.clone())).await?;

    // Get client for listing operations before passing wrapped_s3_client to Checker
    let client = wrapped_s3_client.get_client().await?;
    let checker = Checker::new(wrapped_s3_client, 4, Some(2), 128);

    // List files in all buckets
    println!("\nListing files in input-bucket-1:");
    let files1 = checker
        .list_bucket_files_with_client(
            &client,
            &env.config.bucketsToConsolidate[0],
            &env.date,
            &env.hour,
            BucketRole::Archived,
        )
        .await?;
    for file in &files1.files {
        println!("  - {} ({} bytes)", file.key, file.size);
    }

    println!("\nListing files in input-bucket-2:");
    let files2 = checker
        .list_bucket_files_with_client(
            &client,
            &env.config.bucketsToConsolidate[1],
            &env.date,
            &env.hour,
            BucketRole::Archived,
        )
        .await?;
    for file in &files2.files {
        println!("  - {} ({} bytes)", file.key, file.size);
    }

    println!("\nListing files in consolidated-bucket:");
    let consolidated_files = checker
        .list_bucket_files_with_client(
            &client,
            &env.config.bucketsConsolidated[0],
            &env.date,
            &env.hour,
            BucketRole::Consolidated,
        )
        .await?;
    for file in &consolidated_files.files {
        println!("  - {} ({} bytes)", file.key, file.size);
    }

    // Run the consolidation check
    println!("\nRunning consolidation check...");

    // Build reference vector for archived buckets
    let archived_bucket_refs: Vec<_> = env.config.bucketsToConsolidate.iter().collect();

    let check_result = checker
        .get_comparison_results(
            &archived_bucket_refs,
            &env.config.bucketsConsolidated[0],
            &env.date,
            &env.hour,
        )
        .await?;

    // Print results
    println!("\nCheck result:");
    println!("  Date: {}", check_result.date);
    println!("  Hour: {}", check_result.hour);
    println!("  Check passed: {}", check_result.ok);
    println!("  Message: {}", check_result.message);

    // Assertions
    assert!(
        check_result.ok,
        "Consolidation check should pass with real files. Message: {}",
        check_result.message
    );

    Ok(())
}
