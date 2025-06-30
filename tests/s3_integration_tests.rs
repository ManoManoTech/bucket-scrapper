use std::collections::HashMap;

use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::SdkConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

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

pub struct Container {
    pub bucket: String,
    pub client: SdkConfig,
    pub container: testcontainers::ContainerAsync<MinIO>,
}

pub struct TestEnvironment {
    pub inputs: HashMap<String, Container>,
    pub outputs: HashMap<String, Container>,
}

impl TestEnvironment {
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            outputs: HashMap::new(),
        }
    }

    async fn create_container(name: String) -> Result<Container> {
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

        Ok(Container {
            client,
            container,
            bucket: name,
        })
    }

    pub async fn create() -> Self {
        let mut inputs = HashMap::new();
        let mut outputs = HashMap::new();

        for n in 1..3 {
            let name = format!("container-inputs-{}", n);
            let container = Self::create_container(name.clone()).await.unwrap();
            inputs.insert(name, container);
        }

        for n in 1..1 {
            let name = format!("container-outputs-{}", n);
            let container = Self::create_container(name.clone()).await.unwrap();
            outputs.insert(name, container);
        }

        Self { inputs, outputs }
    }
}

#[tokio::test]
async fn test_archiving_logs() -> Result<()> {
    let test = TestEnvironment::create().await;
    // println!("test1");
    // let _ = test.wait_for_buckets().await;
    // println!("test");
    Ok(())
}
