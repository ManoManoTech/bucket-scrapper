use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_types::SdkConfig;
use std::path::Path;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::minio::MinIO;
use tokio::fs;

use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
};

pub struct TestConstants;

impl TestConstants {
    pub const MINIO_PORT: u16 = 9000;
    pub const DEFAULT_REGION: &'static str = "us-east-1";
    pub const INPUT_BUCKET_NAME: &'static str = "input";
    pub const ARCHIVE_BUCKET_NAME: &'static str = "archive";
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
}

impl TestEnvironment {
    pub async fn create() -> Result<Self> {
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
        let config_path = "tests/mocks/config.yaml";
        let config = load_config(config_path)?;

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

        // Create all buckets dynamically from config
        for bucket_config in &archived_buckets {
            s3_client.create_bucket().bucket(&bucket_config.bucket).send().await?;
        }

        for bucket_config in &consolidated_buckets {
            s3_client.create_bucket().bucket(&bucket_config.bucket).send().await?;
        }

        if let Some(results_bucket_config) = &results_bucket {
            s3_client.create_bucket().bucket(&results_bucket_config.bucket).send().await?;
        }

        // Upload mock data to the buckets
        Self::populate_mock_data(&s3_client).await?;

        Ok(Self {
            inputs_buckets,
            outputs_buckets,
            result_buckets,
            client,
            container,
        })
    }

    async fn populate_mock_data(s3_client: &Client) -> Result<()> {
        let base_path = "tests/mocks/generated";

        // Load config to get bucket names dynamically
        let config = load_config("tests/mocks/config.yaml")?;
        let archived_buckets = get_archived_buckets(&config);
        let consolidated_buckets = get_consolidated_buckets(&config);
        let results_bucket = get_results_bucket(&config);

        // Upload archived buckets (inputs)
        for (i, bucket_config) in archived_buckets.iter().enumerate() {
            let mock_dir = match i {
                0 => "bucket-A",
                1 => "bucket-B", 
                2 => "bucket-C",
                _ => continue,
            };
            let mock_path = format!("{}/{}", base_path, mock_dir);
            if Path::new(&mock_path).exists() {
                Self::upload_directory_to_bucket(s3_client, &mock_path, &bucket_config.bucket).await?;
            }
        }

        // Upload consolidated bucket
        if let Some(consolidated_bucket) = consolidated_buckets.first() {
            let mock_path = format!("{}/bucket-consolidated", base_path);
            if Path::new(&mock_path).exists() {
                Self::upload_directory_to_bucket(s3_client, &mock_path, &consolidated_bucket.bucket).await?;
            }
        }

        // Upload results bucket
        if let Some(results_bucket_config) = results_bucket {
            let mock_path = format!("{}/bucket-checker-results", base_path);
            if Path::new(&mock_path).exists() {
                Self::upload_directory_to_bucket(s3_client, &mock_path, &results_bucket_config.bucket).await?;
            }
        }

        Ok(())
    }

    async fn upload_directory_to_bucket(
        s3_client: &Client,
        dir_path: &str,
        bucket_name: &str,
    ) -> Result<()> {
        use std::collections::VecDeque;

        let mut dirs_to_process = VecDeque::new();
        dirs_to_process.push_back(dir_path.to_string());

        while let Some(current_dir) = dirs_to_process.pop_front() {
            let mut entries = fs::read_dir(&current_dir).await?;

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_dir() {
                    dirs_to_process.push_back(path.to_string_lossy().to_string());
                } else if path.is_file() {
                    // Upload file to S3
                    let file_content = fs::read(&path).await?;

                    // Calculate S3 key relative to base mock directory
                    let relative_path = path
                        .strip_prefix(dir_path)
                        .map_err(|e| anyhow::anyhow!("Failed to get relative path: {}", e))?;
                    let s3_key = relative_path.to_string_lossy().replace("\\", "/");

                    s3_client
                        .put_object()
                        .bucket(bucket_name)
                        .key(&s3_key)
                        .body(file_content.into())
                        .send()
                        .await?;

                    println!(
                        "Uploaded {} to bucket {} with key {}",
                        path.display(),
                        bucket_name,
                        s3_key
                    );
                }
            }
        }

        Ok(())
    }
}