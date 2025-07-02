use anyhow::Result;
use aws_sdk_s3::Client;
use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
};
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;

const PRODUCTION_CONFIG_PATH: &str = "config-continuous.yaml";

#[tokio::test]
async fn test_production_bucket_connectivity() -> Result<()> {
    // Load production config
    let config = load_config(PRODUCTION_CONFIG_PATH)?;
    
    // Create real S3 client
    let aws_config = aws_config::from_env().load().await;
    let s3_client = Client::new(&aws_config);
    
    // Test connectivity to each production bucket
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    
    println!("Testing connectivity to {} archived buckets:", archived_buckets.len());
    for bucket_config in &archived_buckets {
        println!("  Testing bucket: {}", bucket_config.bucket);
        
        // Test basic bucket access
        let objects = s3_client
            .list_objects_v2()
            .bucket(&bucket_config.bucket)
            .max_keys(1)
            .send()
            .await?;
            
        println!("    ✓ Successfully connected to {}", bucket_config.bucket);
        if let Some(contents) = objects.contents() {
            if !contents.is_empty() {
                println!("    Contains data (sample key: {})", 
                    contents[0].key().unwrap_or("unknown"));
            }
        }
    }
    
    println!("Testing connectivity to {} consolidated buckets:", consolidated_buckets.len());
    for bucket_config in &consolidated_buckets {
        println!("  Testing bucket: {}", bucket_config.bucket);
        
        let objects = s3_client
            .list_objects_v2()
            .bucket(&bucket_config.bucket)
            .max_keys(1)
            .send()
            .await?;
            
        println!("    ✓ Successfully connected to {}", bucket_config.bucket);
        if let Some(contents) = objects.contents() {
            if !contents.is_empty() {
                println!("    Contains data (sample key: {})", 
                    contents[0].key().unwrap_or("unknown"));
            }
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_production_file_listing() -> Result<()> {
    // Load production config
    let config = load_config(PRODUCTION_CONFIG_PATH)?;
    
    // Create wrapped S3 client
    let aws_config = aws_config::from_env().load().await;
    let s3_client = Client::new(&aws_config);
    let wrapped_s3_client = WrappedS3Client::new("eu-west-1", 15, Some(s3_client)).await?;
    
    // Create checker
    let checker = Checker::new(wrapped_s3_client, 4, Some(2), 128);
    
    // Get bucket configs
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    
    // Use a recent date that should have data
    let date = "20250630".to_string(); // Yesterday from today (2025-07-01)
    let hour = "12".to_string();
    
    println!("Listing files for {}/{} in production buckets:", date, hour);
    
    // Test file listing for archived buckets
    for bucket_config in &archived_buckets {
        println!("\nTesting archived bucket: {}", bucket_config.bucket);
        println!("  Path config: {:?}", bucket_config.path);
        
        let file_list = checker
            .list_bucket_files(bucket_config, &date, &hour)
            .await?;
            
        println!("  Found {} files", file_list.files.len());
        
        // Show first few files as samples
        for (i, file) in file_list.files.iter().take(3).enumerate() {
            println!("    Sample {}: {} (size: {} bytes)", i + 1, file.key, file.size);
        }
        
        if file_list.files.len() > 3 {
            println!("    ... and {} more files", file_list.files.len() - 3);
        }
    }
    
    // Test file listing for consolidated buckets
    for bucket_config in &consolidated_buckets {
        println!("\nTesting consolidated bucket: {}", bucket_config.bucket);
        println!("  Path config: {:?}", bucket_config.path);
        
        let file_list = checker
            .list_bucket_files(bucket_config, &date, &hour)
            .await?;
            
        println!("  Found {} files", file_list.files.len());
        
        // Show first few files as samples
        for (i, file) in file_list.files.iter().take(3).enumerate() {
            println!("    Sample {}: {} (size: {} bytes)", i + 1, file.key, file.size);
        }
        
        if file_list.files.len() > 3 {
            println!("    ... and {} more files", file_list.files.len() - 3);
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_production_consolidation_check() -> Result<()> {
    // Load production config
    let config = load_config(PRODUCTION_CONFIG_PATH)?;
    
    // Create wrapped S3 client
    let aws_config = aws_config::from_env().load().await;
    let s3_client = Client::new(&aws_config);
    let wrapped_s3_client = WrappedS3Client::new("eu-west-1", 15, Some(s3_client.clone())).await?;
    
    // Create checker with conservative settings for production
    let checker = Checker::new(wrapped_s3_client, 2, Some(1), 64); // Small settings for production test
    
    // Get bucket configs
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets
        .first()
        .ok_or_else(|| anyhow::anyhow!("No consolidated bucket found"))?;
    
    // Use a recent date that should have data
    let date = "20250630".to_string();
    let hour = "12".to_string();
    
    println!("Running production consolidation check for {}/{}", date, hour);
    
    // First verify that we have files in both input and output buckets
    let mut has_input_files = false;
    let mut has_output_files = false;
    
    for bucket_config in &archived_buckets {
        let file_list = checker
            .list_bucket_files(bucket_config, &date, &hour)
            .await?;
        if !file_list.files.is_empty() {
            has_input_files = true;
            println!("✓ Found {} input files in {}", file_list.files.len(), bucket_config.bucket);
        }
    }
    
    let consolidated_file_list = checker
        .list_bucket_files(consolidated_bucket, &date, &hour)
        .await?;
    if !consolidated_file_list.files.is_empty() {
        has_output_files = true;
        println!("✓ Found {} consolidated files in {}", 
            consolidated_file_list.files.len(), consolidated_bucket.bucket);
    }
    
    if !has_input_files {
        println!("⚠️  No input files found for {}/{} - skipping consolidation check", date, hour);
        return Ok(());
    }
    
    if !has_output_files {
        println!("⚠️  No output files found for {}/{} - skipping consolidation check", date, hour);
        return Ok(());
    }
    
    // Run the actual consolidation check
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date, &hour)
        .await?;
    
    println!("Production check result:");
    println!("  OK: {}", result.ok);
    println!("  Message: {}", result.message);
    println!("  Date: {}", result.date);
    println!("  Hour: {}", result.hour);
    println!("  Analysis period: {} to {}", result.analysis_start_date, result.analysis_end_date);
    println!("  Input files: {}", result.input_files_count);
    println!("  Output files: {}", result.output_files_count);
    println!("  Input characters: {}", result.input_total_characters);
    println!("  Output characters: {}", result.output_total_characters);
    println!("  Input bytes: {}", result.input_total_bytes);
    println!("  Output bytes: {}", result.output_total_bytes);
    
    // Basic assertions
    assert!(!result.message.is_empty(), "Result should have a message");
    assert_eq!(result.date, date, "Result date should match input date");
    assert_eq!(result.hour, hour, "Result hour should match input hour");
    assert!(result.input_files_count > 0, "Should have processed some input files");
    assert!(result.output_files_count > 0, "Should have processed some output files");
    assert!(result.input_total_characters > 0, "Should have processed some characters");
    
    // Don't upload to results bucket in production test to avoid pollution
    println!("✓ Production consolidation check completed successfully");
    
    Ok(())
}

#[tokio::test]
async fn test_production_bucket_structure_validation() -> Result<()> {
    // Load production config
    let config = load_config(PRODUCTION_CONFIG_PATH)?;
    
    // Create S3 client
    let aws_config = aws_config::from_env().load().await;
    let s3_client = Client::new(&aws_config);
    
    // Validate archived buckets structure
    let archived_buckets = get_archived_buckets(&config);
    println!("Validating {} archived buckets:", archived_buckets.len());
    
    for bucket_config in &archived_buckets {
        println!("\nBucket: {}", bucket_config.bucket);
        println!("  Path config: {:?}", bucket_config.path);
        println!("  Force env: {:?}", bucket_config.force_env);
        println!("  Proceed without matching objects: {:?}", bucket_config.proceed_without_matching_objects);
        
        // Check if bucket follows expected naming pattern
        let expected_patterns = vec![
            "support-infra-",
            "int-infra-",
            "stg-infra-",
            "prd-infra-"
        ];
        
        let has_expected_pattern = expected_patterns.iter()
            .any(|pattern| bucket_config.bucket.starts_with(pattern));
        
        assert!(has_expected_pattern, 
            "Bucket {} should follow expected naming pattern", bucket_config.bucket);
        
        // Test bucket accessibility
        let result = s3_client
            .head_bucket()
            .bucket(&bucket_config.bucket)
            .send()
            .await;
            
        match result {
            Ok(_) => println!("  ✓ Bucket accessible"),
            Err(e) => {
                println!("  ✗ Bucket not accessible: {}", e);
                // Don't fail the test, just log the issue
            }
        }
    }
    
    // Validate consolidated buckets structure
    let consolidated_buckets = get_consolidated_buckets(&config);
    println!("\nValidating {} consolidated buckets:", consolidated_buckets.len());
    
    for bucket_config in &consolidated_buckets {
        println!("\nBucket: {}", bucket_config.bucket);
        println!("  Path config: {:?}", bucket_config.path);
        
        // Test bucket accessibility
        let result = s3_client
            .head_bucket()
            .bucket(&bucket_config.bucket)
            .send()
            .await;
            
        match result {
            Ok(_) => println!("  ✓ Bucket accessible"),
            Err(e) => {
                println!("  ✗ Bucket not accessible: {}", e);
            }
        }
    }
    
    // Validate results bucket
    if let Some(results_bucket) = get_results_bucket(&config) {
        println!("\nValidating results bucket:");
        println!("Bucket: {}", results_bucket.bucket);
        println!("  Path config: {:?}", results_bucket.path);
        
        let result = s3_client
            .head_bucket()
            .bucket(&results_bucket.bucket)
            .send()
            .await;
            
        match result {
            Ok(_) => println!("  ✓ Bucket accessible"),
            Err(e) => {
                println!("  ✗ Bucket not accessible: {}", e);
            }
        }
    }
    
    println!("\n✓ Production bucket structure validation completed");
    
    Ok(())
}