use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_types::SdkConfig;
use testcontainers::{
    core::{logs::LogFrame, Mount},
    runners::AsyncRunner,
    ImageExt,
};
use testcontainers_modules::minio::MinIO;

use super::mock_data_generator::MockDataGenerator;
use log_consolidator_checker_rust::config::{loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
}, types::ConfigSchema};

pub struct TestConstants;

impl Default for TestConstants {
    fn default() -> Self {
        Self
    }
}

impl TestConstants {
    pub const MINIO_PORT: u16 = 9000;
    pub const DEFAULT_REGION: &'static str = "us-east-1";
    pub const MOCK_CONFIG_PATH: &'static str = "tests/mock_data/config.yaml";
}


pub struct TestEnvironment {
    pub inputs_buckets: Vec<String>,
    pub outputs_buckets: Vec<String>,
    pub result_buckets: Vec<String>,
    pub client: SdkConfig,
    pub container: testcontainers::ContainerAsync<MinIO>,
    pub test_dataset: String,
    pub mock: MockDataGenerator,
    pub config: ConfigSchema,
}

impl TestEnvironment {
    pub async fn create(test_dataset: String) -> Result<Self> {
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");

        let container = MinIO::default()
            .with_log_consumer(|frame: &LogFrame| {
                let mut msg =
                    std::str::from_utf8(frame.bytes()).expect("Failed to parse log message");
                if msg.ends_with('\n') {
                    msg = &msg[..msg.len() - 1];
                }
                println!("Minio log: {msg}");
            })
            .start()
            .await?;
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

        let config = load_config(TestConstants::MOCK_CONFIG_PATH)?;

        let archived_buckets = get_archived_buckets(&config);
        let consolidated_buckets = get_consolidated_buckets(&config);
        let results_bucket = get_results_bucket(&config);

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

        let mock = MockDataGenerator::new(test_dataset.clone());

        println!("Test environment ready!");

        Ok(Self {
            inputs_buckets,
            outputs_buckets,
            result_buckets,
            client,
            container,
            test_dataset,
            mock,
            config,
        })
    }

    pub async fn populate_all_buckets(&self) {
        let s3_client = Client::new(&self.client);
        self.mock
            .populate_all_buckets(&s3_client)
            .await
            .expect("Failed to populate buckets");
    }

    pub async fn populate_inputs_buckets(&self) {
        let s3_client = Client::new(&self.client);
        let config = load_config("tests/mock_data/config.yaml").expect("Failed to load config");
        let archived_buckets = get_archived_buckets(&config);

        for bucket_config in archived_buckets {
            self.mock
                .populate_input_bucket(&s3_client, &bucket_config)
                .await
                .expect("Failed to populate input bucket");
        }
    }
}
