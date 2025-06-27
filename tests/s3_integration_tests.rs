// Consolidated S3 Integration Tests
// This file combines all S3-related testing scenarios into one comprehensive test suite

use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;
use log_consolidator_checker_rust::config::loader::load_config;
use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::collections::HashMap;
use std::fs;
use tempfile::NamedTempFile;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::minio::MinIO;

// Import test helper modules
mod test_helpers;
use test_helpers::*;

// Test environment for comprehensive testing
pub struct TestEnvironment {
    pub multi_env: MultiMinioEnvironment,
    pub populator: MockDataPopulator,
    pub bucket_mappings: HashMap<String, String>,
}

impl TestEnvironment {
    pub async fn new() -> Result<Self> {
        let container_groups = vec![
            (
                "input",
                vec![BucketNames::INPUT_BUCKET_1, BucketNames::INPUT_BUCKET_2],
            ),
            (
                "archive",
                vec![BucketNames::ARCHIVE_BUCKET_1, BucketNames::ARCHIVE_BUCKET_2],
            ),
            ("consolidated", vec![BucketNames::CONSOLIDATED_BUCKET_1]),
            ("results", vec![BucketNames::RESULTS_BUCKET_1]),
        ];

        let multi_env = MultiMinioEnvironment::new(container_groups).await?;
        multi_env.wait_for_all_buckets().await?;

        let populator = match MockDataPopulator::from_env() {
            Ok(p) => p,
            Err(_) => MockDataPopulator::new(""),
        };

        let bucket_mappings = Self::create_bucket_mappings();

        Ok(TestEnvironment {
            multi_env,
            populator,
            bucket_mappings,
        })
    }

    fn create_bucket_mappings() -> HashMap<String, String> {
        let mut mappings = HashMap::new();
        mappings.insert("input".to_string(), BucketNames::INPUT_BUCKET_1.to_string());
        mappings.insert(
            "archive".to_string(),
            BucketNames::ARCHIVE_BUCKET_1.to_string(),
        );
        mappings.insert(
            "consolidated".to_string(),
            BucketNames::CONSOLIDATED_BUCKET_1.to_string(),
        );
        mappings.insert(
            "results".to_string(),
            BucketNames::RESULTS_BUCKET_1.to_string(),
        );
        mappings
    }

    pub async fn populate_input_buckets(&self) -> Result<PopulationSummary> {
        let mut clients = HashMap::new();
        for (name, config) in self.multi_env.get_all_containers() {
            clients.insert(name.clone(), config.client.clone());
        }

        match std::env::var("MOCK_DATA_FOLDER") {
            Ok(_) => {
                match self
                    .populator
                    .populate_input_buckets(&clients, &self.bucket_mappings)
                    .await
                {
                    Ok(summary) => Ok(summary),
                    Err(_) => {
                        self.populator
                            .populate_with_synthetic_data(&clients, &self.bucket_mappings, 5)
                            .await
                    }
                }
            }
            Err(_) => {
                self.populator
                    .populate_with_synthetic_data(&clients, &self.bucket_mappings, 5)
                    .await
            }
        }
    }

    pub fn get_client(&self, container_name: &str) -> Option<&aws_sdk_s3::Client> {
        self.multi_env.get_client(container_name)
    }

    pub fn get_all_bucket_names(&self) -> Vec<String> {
        BucketNames::all_standard_buckets()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub fn get_summary(&self) -> HashMap<String, Vec<String>> {
        self.multi_env.get_environment_summary()
    }
}

// Simple MinIO test environment for basic tests
async fn setup_simple_minio() -> Result<(ContainerAsync<MinIO>, WrappedS3Client)> {
    let minio_container = MinIO::default().start().await?;

    // Wait for container to be ready
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let s3_client = WrappedS3Client::new("us-east-1", 15).await?;

    Ok((minio_container, s3_client))
}

// =============================================================================
// COMPREHENSIVE ENVIRONMENT TESTS
// =============================================================================

#[tokio::test]
async fn test_comprehensive_environment_setup() -> Result<()> {
    println!("Testing comprehensive 6-bucket test environment...");

    let test_env = TestEnvironment::new().await?;

    // Validate bucket existence
    let bucket_names = test_env.get_all_bucket_names();
    assert_eq!(bucket_names.len(), 6, "Should have 6 buckets total");

    // Test bucket access for each container type
    for (container_name, bucket_list) in test_env.get_summary() {
        if let Some(client) = test_env.get_client(&container_name) {
            for bucket_name in &bucket_list {
                let result = client.head_bucket().bucket(bucket_name).send().await;
                assert!(
                    result.is_ok(),
                    "Bucket {} should be accessible",
                    bucket_name
                );
            }
        }
    }

    // Populate input buckets with data
    let population_summary = test_env.populate_input_buckets().await?;
    assert!(
        population_summary.total_files() > 0,
        "Should have populated some files"
    );

    // Test file listing in input buckets
    if let Some(input_client) = test_env.get_client("input") {
        let files =
            S3Operations::list_files(input_client, BucketNames::INPUT_BUCKET_1, None).await?;

        for file_key in &files {
            assert!(
                file_key.contains("raw-logs/"),
                "File keys should have raw-logs prefix"
            );
            assert!(
                file_key.contains(".gz") || file_key.contains(".zst"),
                "Files should be compressed"
            );
        }
    }

    Ok(())
}

// =============================================================================
// FILE FILTERING TESTS
// =============================================================================

#[tokio::test]
async fn test_file_filtering() -> Result<()> {
    let (_container, s3_client) = setup_simple_minio().await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-filtering";
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
            true,
        )
        .await?;

    // Should only find .gz and .zst files (3 files)
    assert_eq!(result.files.len(), 3);

    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("app.log.gz")));
    assert!(keys.iter().any(|k| k.contains("data.json.zst")));
    assert!(keys.iter().any(|k| k.contains("metrics.json.gz")));
    assert!(!keys.iter().any(|k| k.contains("config.txt")));
    assert!(!keys.iter().any(|k| k.contains("readme.md")));

    // Test without filtering
    let result_no_filter = s3_client
        .get_matching_filenames_from_s3(
            &bucket_config,
            &"20231225".to_string(),
            &"14".to_string(),
            false,
        )
        .await?;

    // Should find all 5 files when filtering is disabled
    assert_eq!(result_no_filter.files.len(), 5);

    Ok(())
}

// =============================================================================
// PREFIX PATTERN TESTS
// =============================================================================

#[tokio::test]
async fn test_single_prefix_pattern() -> Result<()> {
    let (_container, s3_client) = setup_simple_minio().await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-single-pattern";
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

    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("app-frontend.log.gz")));
    assert!(keys.iter().any(|k| k.contains("app-backend.log.gz")));
    assert!(!keys.iter().any(|k| k.contains("sys-kernel")));
    assert!(!keys.iter().any(|k| k.contains("usr-analytics")));
    assert!(!keys.iter().any(|k| k.contains("tmp-cache")));

    Ok(())
}

#[tokio::test]
async fn test_multiple_prefix_patterns() -> Result<()> {
    let (_container, s3_client) = setup_simple_minio().await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-multi-patterns";
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

    let keys: Vec<&String> = result.files.iter().map(|f| &f.key).collect();
    assert!(keys.iter().any(|k| k.contains("web-server-01.log.gz")));
    assert!(keys.iter().any(|k| k.contains("web-server-02.log.gz")));
    assert!(keys.iter().any(|k| k.contains("api-gateway.json.zst")));
    assert!(keys
        .iter()
        .any(|k| k.contains("monitoring-prometheus.json.zst")));
    assert!(!keys.iter().any(|k| k.contains("database-primary")));
    assert!(!keys.iter().any(|k| k.contains("cache-redis")));

    Ok(())
}

// =============================================================================
// LARGE FILE TESTS
// =============================================================================

#[tokio::test]
async fn test_large_file_list() -> Result<()> {
    let (_container, s3_client) = setup_simple_minio().await?;
    let client = s3_client.get_client().await?;

    let bucket_name = "test-large-list";
    client.create_bucket().bucket(bucket_name).send().await?;

    // Create test files (simulate directory with multiple files)
    let num_files = 30;
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
    assert!(result.total_archives_size > 0);
    assert!(!result.checksum.is_empty());

    // Verify files have consistent sizes and metadata
    for file in &result.files {
        assert!(file.size > 0);
        assert!(!file.key.is_empty());
        assert_eq!(file.bucket, bucket_name);
    }

    Ok(())
}

// =============================================================================
// CONFIG INTEGRATION TESTS
// =============================================================================

#[tokio::test]
async fn test_config_integration() -> Result<()> {
    let (_container, s3_client) = setup_simple_minio().await?;
    let client = s3_client.get_client().await?;

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
