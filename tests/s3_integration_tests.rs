use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::minio::MinIO;
use tokio::time;

pub struct TestEnvironment {
    pub input_client: aws_sdk_s3::Client,
    pub archive_client: aws_sdk_s3::Client,
    pub input_endpoint: String,
    pub archive_endpoint: String,
    pub input_container: testcontainers::ContainerAsync<MinIO>,
    pub archive_container: testcontainers::ContainerAsync<MinIO>,
}

impl TestEnvironment {
    pub async fn new() -> Result<Self> {
        let input_container_handle =
            Self::start_container(TestConstants::MINIO_IMAGE, TestConstants::MINIO_TAG);
        let archive_container_handle =
            Self::start_container(TestConstants::MINIO_IMAGE, TestConstants::MINIO_TAG);

        let (input_container, archive_container) =
            tokio::try_join!(input_container_handle, archive_container_handle)?;

        let input_host_port = input_container
            .get_host_port_ipv4(TestConstants::MINIO_PORT)
            .await?;
        let archive_host_port = archive_container
            .get_host_port_ipv4(TestConstants::MINIO_PORT)
            .await?;

        let input_endpoint = format!("http://127.0.0.1:{}", input_host_port);
        let archive_endpoint = format!("http://127.0.0.1:{}", archive_host_port);

        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");

        let (input_config, archive_config) = tokio::try_join!(
            Self::configure_client(&input_endpoint, credentials.clone()),
            Self::configure_client(&archive_endpoint, credentials),
        )?;

        let input_client = aws_sdk_s3::Client::new(&input_config);
        let archive_client = aws_sdk_s3::Client::new(&archive_config);

        tokio::try_join!(
            input_client
                .create_bucket()
                .bucket(TestConstants::INPUT_BUCKET_NAME)
                .send(),
            archive_client
                .create_bucket()
                .bucket(TestConstants::ARCHIVE_BUCKET_NAME)
                .send(),
        )?;

        Ok(TestEnvironment {
            input_client,
            archive_client,
            input_container,
            archive_container,
            input_endpoint,
            archive_endpoint,
        })
    }

    async fn wait_for_buckets(&self) -> Result<()> {
        const MAX_RETRIES: u32 = 10;
        const DELAY_MS: u64 = 500;

        for _ in 0..MAX_RETRIES {
            let input_buckets_result = self.input_client.list_buckets().send().await;
            let archive_buckets_result = self.archive_client.list_buckets().send().await;

            if input_buckets_result.is_ok() && archive_buckets_result.is_ok() {
                return Ok(());
            }
            time::sleep(time::Duration::from_millis(DELAY_MS)).await;
        }

        anyhow::bail!("One or many buckets are not availables.");
    }

    async fn start_container(
        image: &'static str,
        tag: &'static str,
    ) -> Result<testcontainers::ContainerAsync<MinIO>> {
        let container_request: testcontainers::ContainerRequest<MinIO> =
            MinIO::default().with_name(image).with_tag(tag);
        let container = container_request.start().await?;
        Ok(container)
    }

    async fn configure_client(
        endpoint: &str,
        credentials: Credentials,
    ) -> Result<aws_config::SdkConfig> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .region(aws_config::Region::new(TestConstants::DEFAULT_REGION))
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .load()
            .await;
        Ok(config)
    }
}

pub struct TestConstants;

impl TestConstants {
    pub const MINIO_IMAGE: &'static str =
        "304971447450.dkr.ecr.eu-west-3.amazonaws.com/public/minio/minio";
    pub const MINIO_TAG: &'static str = "RELEASE.2025-02-18T16-25-55Z";
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

#[tokio::test]
async fn test_archiving_logs() -> Result<()> {
    let test = TestEnvironment::new().await?;
    println!("test1");
    let _ = test.wait_for_buckets().await;
    println!("test");
    Ok(())
}
