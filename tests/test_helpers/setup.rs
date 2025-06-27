use anyhow::Result;
/// Legacy test environment setup for compatibility
/// This module provides the original TestEnvironment for backward compatibility
/// New tests should use the MultiMinioEnvironment instead
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

/// Legacy test environment (kept for compatibility with existing tests)
pub struct LegacyTestEnvironment {
    pub s3_client: WrappedS3Client,
    pub client: aws_sdk_s3::Client,
    pub endpoint: String,
    pub container: testcontainers::ContainerAsync<MinIO>,
}

impl LegacyTestEnvironment {
    pub async fn new() -> Result<Self> {
        let container = MinIO::default().start().await?;

        // Wait for container to be ready
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let host_port = container.get_host_port_ipv4(9000).await?;
        let endpoint = format!("http://127.0.0.1:{}", host_port);

        // Create AWS config without using environment variables
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(&endpoint)
            .credentials_provider(credentials)
            .region(aws_config::Region::new("us-east-1"))
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .load()
            .await;

        // Create S3 client with custom config
        let client = aws_sdk_s3::Client::new(&aws_config);

        // Create wrapped S3 client using temporary env vars (isolated approach)
        let s3_client = Self::create_wrapped_client(&endpoint).await?;

        // Test connection
        let _ = client.list_buckets().send().await?;

        Ok(LegacyTestEnvironment {
            s3_client,
            client,
            endpoint,
            container,
        })
    }

    async fn create_wrapped_client(endpoint: &str) -> Result<WrappedS3Client> {
        // Temporarily set environment variables for the wrapped client
        std::env::set_var("AWS_ENDPOINT_URL", endpoint);
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

        let s3_client = WrappedS3Client::new("us-east-1", 15).await?;
        Ok(s3_client)
    }
}

/// Standard bucket configurations for testing
pub struct StandardBucketConfigs;

impl StandardBucketConfigs {
    /// Create a standard logs bucket configuration
    pub fn logs_bucket(bucket_name: &str) -> BucketConfig {
        BucketConfig {
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
        }
    }

    /// Create bucket configuration with multiple prefix patterns
    pub fn multi_pattern_bucket(bucket_name: &str, patterns: Vec<String>) -> BucketConfig {
        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "2006/01/02/15".to_string(),
                },
            ],
            only_prefix_patterns: Some(patterns),
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }
}

/// Common test constants (legacy)
pub struct LegacyTestConstants;

impl LegacyTestConstants {
    pub const DEFAULT_DATE: &'static str = "20231225";
    pub const DEFAULT_HOUR: &'static str = "14";
    pub const DEFAULT_REGION: &'static str = "us-east-1";
    pub const DEFAULT_RETRY_LIMIT: usize = 15;
}

impl Default for LegacyTestConstants {
    fn default() -> Self {
        Self
    }
}
