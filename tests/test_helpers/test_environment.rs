use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_types::SdkConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use super::mock_data_generator::MockDataGenerator;
use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
};

pub struct TestConstants;

impl TestConstants {
    pub const MINIO_PORT: u16 = 9000;
    pub const DEFAULT_REGION: &'static str = "us-east-1";
    pub const MOCK_CONFIG_PATH: &'static str = "tests/mock_data/config.yaml";
}

impl Default for TestConstants {
    fn default() -> Self {
        Self
    }
}

pub struct TestEnvironment {
    pub inputs_buckets: Vec<String>,
    pub outputs_buckets: Vec<String>,
    pub result_buckets: Vec<String>,
    pub client: SdkConfig,
    pub container: testcontainers::ContainerAsync<MinIO>,
    pub test_dataset: String,
}

impl TestEnvironment {
    pub async fn create(test_dataset: String) -> Result<Self> {
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");

        let container = MinIO::default().start().await?;
        let port = container
            .get_host_port_ipv4(TestConstants::MINIO_PORT)
            .await?;
        let endpoint = format!("http://127.0.0.1:{}", port);
        let client = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .region(aws_config::Region::new(TestConstants::DEFAULT_REGION))
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .load()
            .await;

        // Load configuration from mock config file
        let config = load_config(TestConstants::MOCK_CONFIG_PATH)?;

        // Extract bucket names from config using native functions
        let archived_buckets = get_archived_buckets(&config);
        let consolidated_buckets = get_consolidated_buckets(&config);
        let results_bucket = get_results_bucket(&config);

        // Generate bucket lists from config
        let inputs_buckets: Vec<String> = archived_buckets
            .iter()
            .map(|bucket| bucket.bucket.clone())
            .collect();

        let outputs_buckets: Vec<String> = consolidated_buckets
            .iter()
            .map(|bucket| bucket.bucket.clone())
            .collect();

        let result_buckets: Vec<String> = results_bucket
            .map(|bucket| vec![bucket.bucket.clone()])
            .unwrap_or_default();

        let s3_client = Client::new(&client);

        println!("Creating S3 buckets for test environment...");

        // Create all buckets dynamically from config
        for bucket_config in &archived_buckets {
            s3_client
                .create_bucket()
                .bucket(&bucket_config.bucket)
                .send()
                .await?;
            println!(
                "Created input bucket: {} (env: {})",
                bucket_config.bucket,
                bucket_config
                    .extra
                    .get("force_env")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
            );
        }

        for bucket_config in &consolidated_buckets {
            s3_client
                .create_bucket()
                .bucket(&bucket_config.bucket)
                .send()
                .await?;
            println!("Created consolidated bucket: {}", bucket_config.bucket);
        }

        if let Some(results_bucket_config) = &results_bucket {
            s3_client
                .create_bucket()
                .bucket(&results_bucket_config.bucket)
                .send()
                .await?;
            println!("Created results bucket: {}", results_bucket_config.bucket);
        }

        println!("Populating buckets with mock data...");

        // Upload mock data to the buckets using Rust generator
        let mock_generator = MockDataGenerator::new(test_dataset.clone());
        mock_generator.populate_all_buckets(&s3_client).await?;

        println!("Test environment ready!");

        Ok(Self {
            inputs_buckets,
            outputs_buckets,
            result_buckets,
            client,
            container,
            test_dataset,
        })
    }
}
