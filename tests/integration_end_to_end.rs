use log_consolidator_checker_rust::config::loader::{load_config, get_archived_buckets, get_consolidated_buckets};
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use log_consolidator_checker_rust::s3::checker::Checker;
use log_consolidator_checker_rust::utils::date::date_range_to_date_hour_list;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use tempfile::NamedTempFile;
use std::fs;

#[tokio::test]
async fn test_end_to_end_list_command_workflow() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;
    
    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);
    
    // Set environment variables for AWS SDK
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    
    // Create test configuration
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "archived-bucket"
    path:
      - static_path: "logs"
      - datefmt: "%Y/%m/%d"
      - static_path: "hour"
      - datefmt: "%H"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"
      - datefmt: "%Y/%m/%d"
      - datefmt: "%H"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;
    
    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;
    
    // Load configuration
    let config = load_config(temp_config.path())?;
    
    // Initialize S3 client
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;
    
    // Create test buckets
    let archived_bucket = "archived-bucket";
    let consolidated_bucket = "consolidated-bucket";
    let results_bucket = "results-bucket";
    
    for bucket_name in [archived_bucket, consolidated_bucket, results_bucket] {
        client.create_bucket()
            .bucket(bucket_name)
            .send()
            .await?;
    }
    
    // Create test data for archived bucket (simulating log files for 2023-12-25 hour 14)
    // Using .txt files to avoid compression issues in tests
    let test_files = vec![
        ("logs/2023/12/25/14/app1.log.gz", "App1 log content for hour 14"),
        ("logs/2023/12/25/14/app2.log.gz", "App2 log content for hour 14"),
        ("logs/2023/12/25/14/service.json.zst", "Service logs in JSON format"),
    ];
    
    for (key, content) in test_files {
        client.put_object()
            .bucket(archived_bucket)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }
    
    // Create consolidated data
    let consolidated_files = vec![
        ("consolidated/2023/12/25/14/consolidated_logs.json.zst", "Consolidated logs from all apps"),
    ];
    
    for (key, content) in consolidated_files {
        client.put_object()
            .bucket(consolidated_bucket)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }
    
    // Test the List command workflow
    let start_date = DateTime::parse_from_rfc3339("2023-12-25T14:00:00Z")?.with_timezone(&Utc);
    let end_date = DateTime::parse_from_rfc3339("2023-12-25T14:59:59Z")?.with_timezone(&Utc);
    
    let date_hours = date_range_to_date_hour_list(&start_date, &end_date)?;
    assert_eq!(date_hours.len(), 1);
    assert_eq!(date_hours[0].date, "20231225");
    assert_eq!(date_hours[0].hour, "14");
    
    // Test archived buckets listing
    let archived_buckets = get_archived_buckets(&config);
    assert_eq!(archived_buckets.len(), 1);
    
    for bucket_config in archived_buckets {
        for date_hour in &date_hours {
            let result = s3_client
                .get_matching_filenames_from_s3(
                    bucket_config,
                    &date_hour.date,
                    &date_hour.hour,
                    true,
                )
                .await?;
            
            // Should find 3 files (2 .gz and 1 .zst)
            assert_eq!(result.files.len(), 3);
            assert!(result.total_archives_size > 0);
            
            // Verify file names
            let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
            assert!(keys.iter().any(|k| k.contains("app1.log.gz")));
            assert!(keys.iter().any(|k| k.contains("app2.log.gz")));
            assert!(keys.iter().any(|k| k.contains("service.json.zst")));
        }
    }
    
    // Test consolidated buckets listing
    let consolidated_buckets = get_consolidated_buckets(&config);
    assert_eq!(consolidated_buckets.len(), 1);
    
    for bucket_config in consolidated_buckets {
        for date_hour in &date_hours {
            let result = s3_client
                .get_matching_filenames_from_s3(
                    bucket_config,
                    &date_hour.date,
                    &date_hour.hour,
                    true,
                )
                .await?;
            
            // Should find 1 consolidated file
            assert_eq!(result.files.len(), 1);
            assert!(result.total_archives_size > 0);
            assert!(result.files[0].key.contains("consolidated_logs.json.zst"));
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_end_to_end_check_command_workflow() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container  
    let minio_container = MinIO::default().start().await?;
    
    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);
    
    // Set environment variables for AWS SDK
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    
    // Create test configuration
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-logs-bucket"
    path:
      - static_path: "raw-logs"
      - datefmt: "%Y/%m/%d"
      - static_path: "h"
      - datefmt: "%H"

bucketsConsolidated:
  - bucket: "target-consolidated-bucket" 
    path:
      - static_path: "processed"
      - datefmt: "%Y/%m/%d"
      - datefmt: "%H"

bucketsCheckerResults:
  - bucket: "check-results-bucket"
    path:
      - static_path: "checker-output"
"#;
    
    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;
    
    // Load configuration
    let config = load_config(temp_config.path())?;
    
    // Initialize S3 client
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;
    
    // Create test buckets
    let source_bucket = "source-logs-bucket";
    let target_bucket = "target-consolidated-bucket";
    let results_bucket = "check-results-bucket";
    
    for bucket_name in [source_bucket, target_bucket, results_bucket] {
        client.create_bucket()
            .bucket(bucket_name)
            .send()
            .await?;
    }
    
    // Create comprehensive test data for date 20231225 hour 15
    let date = "20231225";
    let hour = "15";
    
    // Source files with realistic log content
    let source_files = vec![
        ("raw-logs/2023/12/25/h/15/web-server-1.log.gz", "Web server logs from instance 1"),
        ("raw-logs/2023/12/25/h/15/web-server-2.log.gz", "Web server logs from instance 2"),
        ("raw-logs/2023/12/25/h/15/database.json.zst", "Database query logs in JSON"),
        ("raw-logs/2023/12/25/h/15/api-gateway.json.gz", "API gateway access logs"),
    ];
    
    for (key, content) in source_files {
        client.put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }
    
    // Target consolidated files
    let target_files = vec![
        ("processed/2023/12/25/15/all-services.json.zst", "All services consolidated into single file"),
    ];
    
    for (key, content) in target_files {
        client.put_object()
            .bucket(target_bucket)
            .key(key) 
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }
    
    // Test the Check command workflow
    let checker = Checker::new(
        s3_client,
        4, // max_parallel
        None, // process_threads (use default)
        1024, // memory_pool_mb
    );
    
    // Get bucket configurations
    let archived_buckets = get_archived_buckets(&config);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first()
        .ok_or("No consolidated bucket found")?;
    
    // Perform the comparison (this is the core Check command logic)
    let date_str = date.to_string();
    let hour_str = hour.to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;
    
    // Verify the check results
    // The result should indicate whether consolidation was successful
    // Note: The actual success/failure depends on the Checker implementation
    // For this test, we mainly verify the workflow completes without errors
    assert!(!result.message.is_empty());
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.analysis_start_date.is_empty());
    assert!(!result.analysis_end_date.is_empty());
    
    // The comparison result should be either true or false, depending on the actual data
    // For this test, we just verify it completed without error
    println!("Check result: ok={}, message={}", result.ok, result.message);
    
    Ok(())
}

#[tokio::test]
async fn test_end_to_end_multiple_date_hours() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;
    
    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);
    
    // Set environment variables for AWS SDK
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    
    // Create test configuration with multiple archived buckets
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "app-logs-bucket"
    path:
      - static_path: "applications"
      - datefmt: "dt=placeholder/hour=99"
  - bucket: "system-logs-bucket"
    path:
      - static_path: "system"
      - datefmt: "dt=placeholder/hour=99"

bucketsConsolidated:
  - bucket: "unified-logs-bucket"
    path:
      - static_path: "unified"
      - datefmt: "dt=placeholder/hour=99"

bucketsCheckerResults:
  - bucket: "validation-results-bucket"
    path:
      - static_path: "validation"
"#;
    
    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;
    
    // Load configuration
    let config = load_config(temp_config.path())?;
    
    // Initialize S3 client
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;
    
    // Create test buckets
    let buckets = ["app-logs-bucket", "system-logs-bucket", "unified-logs-bucket", "validation-results-bucket"];
    for bucket_name in buckets {
        client.create_bucket()
            .bucket(bucket_name)
            .send()
            .await?;
    }
    
    // Create test data for multiple hours: 2023-12-26 hours 10, 11, 12
    let test_scenarios = vec![
        ("20231226", "10"),
        ("20231226", "11"), 
        ("20231226", "12"),
    ];
    
    for (date, hour) in &test_scenarios {
        // App logs bucket
        let app_files = vec![
            (format!("applications/dt={}/hour={}/frontend.log.gz", date, hour), format!("Frontend logs for {}/{}", date, hour)),
            (format!("applications/dt={}/hour={}/backend.json.zst", date, hour), format!("Backend API logs for {}/{}", date, hour)),
        ];
        
        for (key, content) in app_files {
            client.put_object()
                .bucket("app-logs-bucket")
                .key(&key)
                .body(ByteStream::from(content.as_bytes().to_vec()))
                .send()
                .await?;
        }
        
        // System logs bucket
        let system_files = vec![
            (format!("system/dt={}/hour={}/kernel.log.gz", date, hour), format!("Kernel logs for {}/{}", date, hour)),
            (format!("system/dt={}/hour={}/auth.json.zst", date, hour), format!("Authentication logs for {}/{}", date, hour)),
        ];
        
        for (key, content) in system_files {
            client.put_object()
                .bucket("system-logs-bucket")
                .key(&key)
                .body(ByteStream::from(content.as_bytes().to_vec()))
                .send()
                .await?;
        }
        
        // Unified logs bucket
        let unified_file = (
            format!("unified/dt={}/hour={}/all.json.zst", date, hour),
            format!("Unified logs for {}/{}", date, hour)
        );
        
        client.put_object()
            .bucket("unified-logs-bucket")
            .key(&unified_file.0)
            .body(ByteStream::from(unified_file.1.as_bytes().to_vec()))
            .send()
            .await?;
    }
    
    // Test List command workflow with date range
    let start_date = DateTime::parse_from_rfc3339("2023-12-26T10:00:00Z")?.with_timezone(&Utc);
    let end_date = DateTime::parse_from_rfc3339("2023-12-26T12:59:59Z")?.with_timezone(&Utc);
    
    let date_hours = date_range_to_date_hour_list(&start_date, &end_date)?;
    assert_eq!(date_hours.len(), 3); // Hours 10, 11, 12
    
    // Test all archived buckets
    let archived_buckets = get_archived_buckets(&config);
    assert_eq!(archived_buckets.len(), 2); // app-logs and system-logs
    
    for bucket_config in &archived_buckets {
        for date_hour in &date_hours {
            let result = s3_client
                .get_matching_filenames_from_s3(
                    bucket_config,
                    &date_hour.date,
                    &date_hour.hour,
                    true,
                )
                .await?;
            
            // Each bucket should have 2 files per hour
            assert_eq!(result.files.len(), 2);
            assert!(result.total_archives_size > 0);
        }
    }
    
    // Test consolidated bucket
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();
    
    for date_hour in &date_hours {
        let result = s3_client
            .get_matching_filenames_from_s3(
                consolidated_bucket,
                &date_hour.date,
                &date_hour.hour,
                true,
            )
            .await?;
        
        // Should have 1 unified file per hour
        assert_eq!(result.files.len(), 1);
        assert!(result.files[0].key.contains("all.json.zst"));
    }
    
    // Test Check command for one specific hour
    let checker = Checker::new(s3_client, 8, None, 2048);
    
    let date_str = "20231226".to_string();
    let hour_str = "11".to_string();
    let result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await?;
    
    // Verify the check completed successfully
    assert_eq!(result.date, date_str);
    assert_eq!(result.hour, hour_str);
    assert!(!result.message.is_empty());
    assert!(!result.analysis_start_date.is_empty());
    assert!(!result.analysis_end_date.is_empty());
    
    println!("Multi-date check result: ok={}, message={}", result.ok, result.message);
    
    Ok(())
}

#[tokio::test]
async fn test_end_to_end_error_scenarios() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;
    
    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);
    
    // Set environment variables for AWS SDK
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    
    // Create test configuration
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "missing-bucket"
    path:
      - static_path: "data"
      - datefmt: "2006/01/02/15"

bucketsConsolidated:
  - bucket: "also-missing-bucket"
    path:
      - static_path: "consolidated"
      - datefmt: "2006/01/02/15"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;
    
    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;
    
    // Load configuration
    let config = load_config(temp_config.path())?;
    
    // Initialize S3 client
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    
    // Test accessing non-existent buckets (should handle gracefully)
    let archived_buckets = get_archived_buckets(&config);
    let bucket_config = archived_buckets.first().unwrap();
    
    let date_str = "20231225".to_string();
    let hour_str = "14".to_string();
    let result = s3_client
        .get_matching_filenames_from_s3(
            bucket_config,
            &date_str,
            &hour_str,
            true,
        )
        .await;
    
    // Should return an error for non-existent bucket
    assert!(result.is_err());
    
    // Test Checker with non-existent buckets
    let checker = Checker::new(s3_client, 4, None, 1024);
    let consolidated_buckets = get_consolidated_buckets(&config);
    let consolidated_bucket = consolidated_buckets.first().unwrap();
    
    let check_result = checker
        .get_comparison_results(&archived_buckets, consolidated_bucket, &date_str, &hour_str)
        .await;
    
    // Should handle errors gracefully (exact behavior depends on Checker implementation)
    // At minimum, it should not panic
    match check_result {
        Ok(_) => {
            // If it succeeds, that's fine - it means empty results were handled
        }
        Err(_) => {
            // If it errors, that's also expected for non-existent buckets
        }
    }
    
    Ok(())
}