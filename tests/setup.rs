use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

/// Test environment setup containing MinIO container and S3 client
pub struct TestEnvironment {
    pub s3_client: WrappedS3Client,
    pub client: aws_sdk_s3::Client,
    pub endpoint: String,
    pub input_container: testcontainers::ContainerAsync<MinIO>,
    pub archive_container: testcontainers::ContainerAsync<MinIO>,
}

impl TestEnvironment {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let input_container = MinIO::default()
            .with_name("304971447450.dkr.ecr.eu-west-3.amazonaws.com/public/minio/minio")
            .with_tag("RELEASE.2025-02-18T16-25-55Z");
        let archive_container = MinIO::default()
            .with_name("304971447450.dkr.ecr.eu-west-3.amazonaws.com/public/minio/minio")
            .with_tag("RELEASE.2025-02-18T16-25-55Z");


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

        Ok(TestEnvironment {
            s3_client,
            client,
            endpoint,
            _input_container: input_container,
            _archive_container: archive_container,
        })
    }

/// Common test constants
pub struct TestConstants;

impl TestConstants {
    pub const DEFAULT_DATE: &'static str = "20231225";
    pub const DEFAULT_HOUR: &'static str = "14";
    pub const DEFAULT_REGION: &'static str = "us-east-1";
    pub const DEFAULT_RETRY_LIMIT: usize = 15;
}

impl Default for TestConstants {
    fn default() -> Self {
        Self
    }
}
