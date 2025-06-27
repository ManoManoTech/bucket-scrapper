/// Multi-container MinIO environment for comprehensive S3 testing
/// Provides abstractions for managing multiple MinIO containers and S3 clients
use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::minio::MinIO;

/// Container configuration for different bucket types
#[derive(Debug)]
pub struct ContainerConfig {
    pub name: String,
    pub buckets: Vec<String>,
    pub endpoint: String,
    pub client: aws_sdk_s3::Client,
    pub container: testcontainers::ContainerAsync<MinIO>,
}

/// Multi-container environment supporting various bucket configurations
pub struct MultiMinioEnvironment {
    pub containers: HashMap<String, ContainerConfig>,
    pub credentials: Credentials,
}

impl MultiMinioEnvironment {
    /// Create a new multi-container environment with specified container groups
    pub async fn new(container_groups: Vec<(&str, Vec<&str>)>) -> Result<Self> {
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");
        let mut containers = HashMap::new();

        // Start all containers in parallel
        let container_futures: Vec<_> = container_groups
            .iter()
            .map(|(name, _)| Self::start_container(name))
            .collect();

        let started_containers: Vec<_> = {
            let mut results = Vec::new();
            for future in container_futures {
                results.push(future.await?);
            }
            results
        };

        // Create clients and bucket configs for each container
        for ((name, bucket_names), container) in container_groups.iter().zip(started_containers) {
            let host_port = container.get_host_port_ipv4(9000).await?;
            let endpoint = format!("http://127.0.0.1:{}", host_port);

            let client = Self::create_client(&endpoint, credentials.clone()).await?;

            // Create buckets in parallel
            let bucket_futures: Vec<_> = bucket_names
                .iter()
                .map(|bucket_name| client.create_bucket().bucket(*bucket_name).send())
                .collect();

            {
                for future in bucket_futures {
                    future.await?;
                }
            };

            let container_config = ContainerConfig {
                name: name.to_string(),
                buckets: bucket_names.iter().map(|s| s.to_string()).collect(),
                endpoint,
                client,
                container,
            };

            containers.insert(name.to_string(), container_config);
        }

        Ok(MultiMinioEnvironment {
            containers,
            credentials,
        })
    }

    /// Create a standard environment with 6 buckets across 4 containers
    pub async fn create_standard_environment() -> Result<Self> {
        let container_groups = vec![
            ("input", vec!["input-bucket-1", "input-bucket-2"]),
            ("archive", vec!["archive-bucket-1", "archive-bucket-2"]),
            ("consolidated", vec!["consolidated-bucket"]),
            ("results", vec!["results-bucket"]),
        ];

        Self::new(container_groups).await
    }

    /// Get client for a specific container
    pub fn get_client(&self, container_name: &str) -> Option<&aws_sdk_s3::Client> {
        self.containers.get(container_name).map(|c| &c.client)
    }

    /// Get endpoint for a specific container
    pub fn get_endpoint(&self, container_name: &str) -> Option<&str> {
        self.containers
            .get(container_name)
            .map(|c| c.endpoint.as_str())
    }

    /// Get all bucket names for a container
    pub fn get_bucket_names(&self, container_name: &str) -> Vec<String> {
        self.containers
            .get(container_name)
            .map(|c| c.buckets.clone())
            .unwrap_or_default()
    }

    /// Get all containers and their bucket information
    pub fn get_all_containers(&self) -> &HashMap<String, ContainerConfig> {
        &self.containers
    }

    /// Wait for all buckets to be ready across all containers
    pub async fn wait_for_all_buckets(&self) -> Result<()> {
        let mut all_futures = Vec::new();

        for (container_name, config) in &self.containers {
            for bucket_name in &config.buckets {
                let client = &config.client;
                let bucket = bucket_name.clone();

                // Create a future that tests bucket readiness
                let future = async move {
                    for _ in 0..10 {
                        match client.head_bucket().bucket(&bucket).send().await {
                            Ok(_) => return Ok(()),
                            Err(_) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
                            }
                        }
                    }
                    Err(anyhow::anyhow!(
                        "Bucket {} in container {} not ready",
                        bucket,
                        container_name
                    ))
                };

                all_futures.push(future);
            }
        }

        {
            for future in all_futures {
                future.await?;
            }
        };
        Ok(())
    }

    /// Get a summary of the environment
    pub fn get_environment_summary(&self) -> HashMap<String, Vec<String>> {
        self.containers
            .iter()
            .map(|(name, config)| (name.clone(), config.buckets.clone()))
            .collect()
    }

    // Private helper methods
    async fn start_container(_name: &str) -> Result<testcontainers::ContainerAsync<MinIO>> {
        let container_request =
            MinIO::default().with_startup_timeout(std::time::Duration::from_secs(10));

        let container = container_request.start().await?;

        // Wait a bit for the container to be fully ready
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Ok(container)
    }

    async fn create_client(endpoint: &str, credentials: Credentials) -> Result<aws_sdk_s3::Client> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .load()
            .await;

        Ok(aws_sdk_s3::Client::new(&config))
    }
}

/// Builder pattern for creating custom multi-container environments
pub struct MultiMinioEnvironmentBuilder {
    container_groups: Vec<(String, Vec<String>)>,
}

impl MultiMinioEnvironmentBuilder {
    pub fn new() -> Self {
        Self {
            container_groups: Vec::new(),
        }
    }

    /// Add a container group with specified buckets
    pub fn with_container_group(mut self, name: &str, buckets: Vec<&str>) -> Self {
        self.container_groups.push((
            name.to_string(),
            buckets.iter().map(|s| s.to_string()).collect(),
        ));
        self
    }

    /// Add input containers with specified bucket count
    pub fn with_input_containers(mut self, count: usize) -> Self {
        for i in 0..count {
            self.container_groups
                .push((format!("input-{}", i), vec![format!("input-bucket-{}", i)]));
        }
        self
    }

    /// Add archive containers with specified bucket count
    pub fn with_archive_containers(mut self, count: usize) -> Self {
        for i in 0..count {
            self.container_groups.push((
                format!("archive-{}", i),
                vec![format!("archive-bucket-{}", i)],
            ));
        }
        self
    }

    /// Build the environment
    pub async fn build(self) -> Result<MultiMinioEnvironment> {
        let container_groups: Vec<_> = self
            .container_groups
            .iter()
            .map(|(name, buckets)| (name.as_str(), buckets.iter().map(|s| s.as_str()).collect()))
            .collect();

        MultiMinioEnvironment::new(container_groups).await
    }
}

impl Default for MultiMinioEnvironmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}
