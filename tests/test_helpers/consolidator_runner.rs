use anyhow::Result;
use testcontainers::{
    core::{logs::LogFrame, Mount},
    runners::AsyncRunner,
    ContainerRequest, GenericImage, ImageExt,
};

pub struct ContainerConfig {
    pub image: String,
    pub tag: String,
    pub config_path: String,
    pub target_time: String,
    pub minio_endpoint: String,
    pub sleep_duration_secs: u64,
}

pub struct ConsolidatorRunner<'a> {
    config: &'a ContainerConfig,
    container: Option<ContainerRequest<GenericImage>>,
}

impl<'a> ConsolidatorRunner<'a> {
    pub fn new(config: &'a ContainerConfig) -> Self {
        Self {
            config,
            container: None,
        }
    }

    pub fn with_default(mut self) -> Self {
        let config_mount = Mount::bind_mount(&self.config.config_path, "/app/config.yaml");
        let container = GenericImage::new(&self.config.image, &self.config.tag)
            .with_mount(config_mount)
            .with_env_var("LOG_CONSOLIDATOR_S3_ENDPOINT", &self.config.minio_endpoint)
            .with_env_var("LOG_CONSOLIDATOR_TARGET_SPEC_PATH", "/app/config.yaml")
            .with_env_var("LOG_CONSOLIDATOR_S3_ACCESS_KEY_ID", "minioadmin")
            .with_env_var("LOG_CONSOLIDATOR_S3_SECRET_ACCESS_KEY", "minioadmin")
            .with_env_var("LOG_CONSOLIDATOR_S3_REGION", "us-east-1")
            .with_env_var("LOG_CONSOLIDATOR_S3_FORCE_PATH_STYLE", "true")
            .with_env_var("LOG_CONSOLIDATOR_S3_NO_RETRY_INCREMENT", "10")
            .with_env_var("LOG_CONSOLIDATOR_S3_RETRY_COST", "1")
            .with_env_var("LOG_CONSOLIDATOR_S3_RETRY_TIMEOUT_COST", "2")
            .with_env_var("LOG_CONSOLIDATOR_TARGET_TIME", &self.config.target_time)
            .with_log_consumer(|frame: &LogFrame| {
                let mut msg =
                    std::str::from_utf8(frame.bytes()).expect("Failed to parse log message");
                if msg.ends_with('\n') {
                    msg = &msg[..msg.len() - 1];
                }
                println!("Consolidator container log: {msg}");

                if msg.contains("\"level\":\"error\"") || msg.contains("\"level\":\"fatal\"") {
                    panic!("Consolidator failed with error: {}", msg);
                }
            });
        self.container = Some(container);
        self
    }

    pub fn with_continuous(mut self) -> Self {
        let config_mount = Mount::bind_mount(&self.config.config_path, "/app/config.yaml");
        let container = GenericImage::new(&self.config.image, &self.config.tag)
            .with_mount(config_mount)
            .with_env_var("LOG_CONSOLIDATOR_S3_ENDPOINT", &self.config.minio_endpoint)
            .with_env_var("LOG_CONSOLIDATOR_TARGET_SPEC_PATH", "/app/config.yaml")
            .with_env_var("LOG_CONSOLIDATOR_S3_ACCESS_KEY_ID", "minioadmin")
            .with_env_var("LOG_CONSOLIDATOR_S3_SECRET_ACCESS_KEY", "minioadmin")
            .with_env_var("LOG_CONSOLIDATOR_S3_REGION", "us-east-1")
            .with_env_var("LOG_CONSOLIDATOR_S3_FORCE_PATH_STYLE", "true")
            .with_env_var("LOG_CONSOLIDATOR_S3_NO_RETRY_INCREMENT", "10")
            .with_env_var("LOG_CONSOLIDATOR_S3_RETRY_COST", "1")
            .with_env_var("LOG_CONSOLIDATOR_S3_RETRY_TIMEOUT_COST", "2")
            .with_log_consumer(|frame: &LogFrame| {
                let mut msg =
                    std::str::from_utf8(frame.bytes()).expect("Failed to parse log message");
                if msg.ends_with('\n') {
                    msg = &msg[..msg.len() - 1];
                }
                println!("Consolidator container log: {msg}");

                if msg.contains("\"level\":\"error\"") || msg.contains("\"level\":\"fatal\"") {
                    panic!("Consolidator failed with error: {}", msg);
                }
            });
        self.container = Some(container);
        self
    }

    pub async fn run(mut self) -> Result<()> {
        match self.container.take() {
            Some(container) => {
                let _running_container = container.start().await?;
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    self.config.sleep_duration_secs,
                ))
                .await;
                Ok(())
            }
            None => Err(anyhow::anyhow!(
                "Container configuration is not set. Please call with_default() or with_continuous() before running."
            )),
        }
    }
}
