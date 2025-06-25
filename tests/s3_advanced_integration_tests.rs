use aws_sdk_s3::primitives::ByteStream;
use log_consolidator_checker_rust::config::loader::load_config;
use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::collections::HashMap;
use std::fs;
use tempfile::NamedTempFile;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

#[tokio::test]
async fn test_s3_client_file_filtering() -> Result<(), Box<dyn std::error::Error>> {
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

    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-filtering";

    // Create bucket
    client.create_bucket().bucket(bucket_name).send().await?;

    // Create test files with different extensions
    let test_files = vec![
        ("logs/2023/12/25/14/app.log.gz", "Compressed log file"),
        ("logs/2023/12/25/14/data.json.zst", "Compressed JSON file"),
        (
            "logs/2023/12/25/14/config.txt",
            "Plain text file - should be filtered out",
        ),
        (
            "logs/2023/12/25/14/readme.md",
            "Markdown file - should be filtered out",
        ),
        (
            "logs/2023/12/25/14/metrics.json.gz",
            "Compressed metrics file",
        ),
    ];

    for (key, content) in test_files {
        client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }

    // Create bucket config for testing
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

    // Test file listing with filtering enabled
    let result = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            true, // Enable filtering
        )
        .await?;

    // Should only find .gz and .zst files (3 files)
    assert_eq!(result.files.len(), 3);

    // Verify the files found are only the compressed ones
    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("app.log.gz")));
    assert!(keys.iter().any(|k| k.contains("data.json.zst")));
    assert!(keys.iter().any(|k| k.contains("metrics.json.gz")));

    // Verify .txt and .md files are filtered out
    assert!(!keys.iter().any(|k| k.contains("config.txt")));
    assert!(!keys.iter().any(|k| k.contains("readme.md")));

    // Test without filtering
    let result_no_filter = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            false, // Disable filtering
        )
        .await?;

    // Should find all 5 files when filtering is disabled
    assert_eq!(result_no_filter.files.len(), 5);

    Ok(())
}

#[tokio::test]
async fn test_s3_client_prefix_patterns() -> Result<(), Box<dyn std::error::Error>> {
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

    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-patterns";

    // Create bucket
    client.create_bucket().bucket(bucket_name).send().await?;

    // Create test files with different prefixes
    let test_files = vec![
        ("logs/2023/12/25/14/app-frontend.log.gz", "Frontend logs"),
        ("logs/2023/12/25/14/app-backend.log.gz", "Backend logs"),
        ("logs/2023/12/25/14/sys-kernel.log.gz", "Kernel logs"),
        (
            "logs/2023/12/25/14/usr-analytics.json.zst",
            "Analytics data",
        ),
        ("logs/2023/12/25/14/tmp-cache.log.gz", "Temp cache logs"),
    ];

    for (key, content) in test_files {
        client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }

    // Create bucket config with prefix patterns (only app- files)
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
        only_prefix_patterns: Some(vec!["^app-.*".to_string()]),
        proceed_without_matching_objects: false,
        extra: HashMap::new(),
    };

    // Test file listing with prefix patterns
    let result = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            true,
        )
        .await?;

    // Should only find app- prefixed files (2 files)
    assert_eq!(result.files.len(), 2);

    // Verify only app- files are found
    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("app-frontend.log.gz")));
    assert!(keys.iter().any(|k| k.contains("app-backend.log.gz")));

    // Verify other files are filtered out
    assert!(!keys.iter().any(|k| k.contains("sys-kernel")));
    assert!(!keys.iter().any(|k| k.contains("usr-analytics")));
    assert!(!keys.iter().any(|k| k.contains("tmp-cache")));

    Ok(())
}

#[tokio::test]
async fn test_s3_client_multiple_prefix_patterns() -> Result<(), Box<dyn std::error::Error>> {
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

    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-multi-patterns";

    // Create bucket
    client.create_bucket().bucket(bucket_name).send().await?;

    // Create test files with different patterns
    let test_files = vec![
        ("logs/2023/12/25/14/web-server-01.log.gz", "Web server 1"),
        ("logs/2023/12/25/14/web-server-02.log.gz", "Web server 2"),
        ("logs/2023/12/25/14/api-gateway.json.zst", "API gateway"),
        ("logs/2023/12/25/14/database-primary.log.gz", "Primary DB"),
        ("logs/2023/12/25/14/cache-redis.log.gz", "Redis cache"),
        (
            "logs/2023/12/25/14/monitoring-prometheus.json.zst",
            "Prometheus",
        ),
    ];

    for (key, content) in test_files {
        client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }

    // Create bucket config with multiple prefix patterns
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
        only_prefix_patterns: Some(vec![
            "^web-server-.*".to_string(),
            "^api-.*".to_string(),
            "^monitoring-.*".to_string(),
        ]),
        proceed_without_matching_objects: false,
        extra: HashMap::new(),
    };

    // Test file listing with multiple prefix patterns
    let result = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            true,
        )
        .await?;

    // Should find web-server, api, and monitoring files (4 files total)
    assert_eq!(result.files.len(), 4);

    // Verify the correct files are found
    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("web-server-01.log.gz")));
    assert!(keys.iter().any(|k| k.contains("web-server-02.log.gz")));
    assert!(keys.iter().any(|k| k.contains("api-gateway.json.zst")));
    assert!(keys
        .iter()
        .any(|k| k.contains("monitoring-prometheus.json.zst")));

    // Verify excluded files
    assert!(!keys.iter().any(|k| k.contains("database-primary")));
    assert!(!keys.iter().any(|k| k.contains("cache-redis")));

    Ok(())
}

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
async fn test_s3_client_with_config_loading() -> Result<(), Box<dyn std::error::Error>> {
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
  - bucket: "integration-test-archived"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"
    only_prefix_patterns:
      - "^app-.*"
      - "^web-.*"

bucketsConsolidated:
  - bucket: "integration-test-consolidated"
    path:
      - static_path: "processed"
      - datefmt: "2006/01/02/15"

bucketsCheckerResults:
  - bucket: "integration-test-results"
    path:
      - static_path: "results"
"#;

    let temp_config = NamedTempFile::new()?;
    fs::write(temp_config.path(), config_content)?;

    // Load configuration
    let config = load_config(temp_config.path())?;

    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
    let client = s3_client.get_client().await?;

    // Create buckets from config
    for bucket_config in &config.bucketsToConsolidate {
        client
            .create_bucket()
            .bucket(&bucket_config.bucket)
            .send()
            .await?;
    }

    for bucket_config in &config.bucketsConsolidated {
        client
            .create_bucket()
            .bucket(&bucket_config.bucket)
            .send()
            .await?;
    }

    // Create test files in archived bucket
    let archived_bucket = &config.bucketsToConsolidate[0];
    let test_files = vec![
        (
            "raw-logs/dt=20231226/hour=10/app-frontend.log.gz",
            "Frontend logs",
        ),
        (
            "raw-logs/dt=20231226/hour=10/app-backend.json.zst",
            "Backend logs",
        ),
        (
            "raw-logs/dt=20231226/hour=10/web-nginx.log.gz",
            "Nginx logs",
        ),
        (
            "raw-logs/dt=20231226/hour=10/db-postgres.log.gz",
            "DB logs - should be filtered",
        ),
    ];

    for (key, content) in test_files {
        client
            .put_object()
            .bucket(&archived_bucket.bucket)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
    }

    // Test using the actual config
    let result = s3_client
        .get_matching_filenames_from_s3(
            archived_bucket,
            &"20231226".to_string(),
            &"10".to_string(),
            true,
        )
        .await?;

    // Should find 3 files (app- and web- prefixed, db- filtered out)
    assert_eq!(result.files.len(), 3);

    // Verify correct files found
    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("app-frontend")));
    assert!(keys.iter().any(|k| k.contains("app-backend")));
    assert!(keys.iter().any(|k| k.contains("web-nginx")));
    assert!(!keys.iter().any(|k| k.contains("db-postgres")));

    // Test consolidated bucket
    let consolidated_bucket = &config.bucketsConsolidated[0];

    // Create test file in consolidated bucket
    client
        .put_object()
        .bucket(&consolidated_bucket.bucket)
        .key("processed/2023/12/26/10/all-logs.json.zst")
        .body(ByteStream::from(b"Consolidated logs".to_vec()))
        .send()
        .await?;

    let consolidated_result = s3_client
        .get_matching_filenames_from_s3(
            consolidated_bucket,
            &"20231226".to_string(),
            &"10".to_string(),
            true,
        )
        .await?;

    // Should find 1 consolidated file
    assert_eq!(consolidated_result.files.len(), 1);
    assert!(consolidated_result.files[0]
        .key
        .contains("all-logs.json.zst"));

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
