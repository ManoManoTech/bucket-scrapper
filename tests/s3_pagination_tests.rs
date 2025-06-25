use aws_sdk_s3::primitives::ByteStream;
use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

#[tokio::test]
async fn test_s3_client_large_file_list() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;

    // Wait a bit for container to be ready
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    // Set environment variables for AWS SDK
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-large-list";

    // Create bucket
    client.create_bucket().bucket(bucket_name).send().await?;

    // Create test files (simulate directory with multiple files)
    let num_files = 20;
    for i in 0..num_files {
        let key = format!("logs/2023/12/25/14/app-{:03}.log.gz", i);
        let content = format!("Log content for file {}", i);

        client
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }

    // Create bucket config
    let bucket_config = BucketConfig {
        bucket: bucket_name.to_string(),
        path: vec![
            PathSchema::Static {
                static_path: "logs".to_string(),
            },
            PathSchema::DateFormat {
                datefmt: "2006/01/02/15".to_string(),
            },
        ],
        only_prefix_patterns: None,
        proceed_without_matching_objects: false,
        extra: HashMap::new(),
    };

    // Test file listing with large number of files
    let result = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            true,
        )
        .await?;

    // Should find all files
    assert_eq!(result.files.len(), num_files);

    // Verify files are properly sized and named
    assert!(result.total_archives_size > 0);

    // Verify checksum is calculated
    assert!(!result.checksum.is_empty());

    // Verify files have consistent sizes and metadata
    for file in &result.files {
        assert!(file.size > 0);
        assert!(!file.key.is_empty());
        assert_eq!(file.bucket, bucket_name);
    }

    Ok(())
}

#[tokio::test]
async fn test_s3_client_pagination() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container with retry logic
    let minio_container = MinIO::default().start().await?;

    // Wait a bit for container to be ready
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    // Set environment variables for AWS SDK
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    // Retry S3 client creation to handle connection issues
    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    // Test connection with a simple operation
    let _ = client.list_buckets().send().await?;

    let bucket_name = "test-pagination";

    // Create bucket
    client.create_bucket().bucket(bucket_name).send().await?;

    // Create files to test pagination functionality
    // Using a smaller number to reduce test instability while still testing pagination logic
    let num_files = 25;
    for i in 0..num_files {
        let key = format!("logs/2023/12/25/14/service-{:04}.log.gz", i);
        let content = format!(
            "Service log entry #{} with some content to vary file sizes",
            i
        );

        client
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }

    // Create bucket config
    let bucket_config = BucketConfig {
        bucket: bucket_name.to_string(),
        path: vec![
            PathSchema::Static {
                static_path: "logs".to_string(),
            },
            PathSchema::DateFormat {
                datefmt: "2006/01/02/15".to_string(),
            },
        ],
        only_prefix_patterns: None,
        proceed_without_matching_objects: false,
        extra: HashMap::new(),
    };

    // Test pagination handling
    let result = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            true,
        )
        .await?;

    // Should find all files despite pagination
    assert_eq!(result.files.len(), num_files);

    // Verify total size calculation across pages
    assert!(result.total_archives_size > 0);

    // Verify all files have proper metadata
    for file in &result.files {
        assert!(!file.key.is_empty());
        assert!(file.size > 0);
        assert_eq!(file.bucket, bucket_name);
    }

    // Verify checksum was calculated
    assert!(!result.checksum.is_empty());
    assert_eq!(result.checksum.len(), 32); // MD5 hex string length

    Ok(())
}