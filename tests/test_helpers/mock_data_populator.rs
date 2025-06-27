/// Mock data population utilities for S3 integration tests
/// Handles reading compressed JSON files from filesystem and distributing them to S3 buckets
use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Represents a log entry with service and environment metadata
#[derive(Debug, Clone)]
struct LogEntry {
    service: String,
    environment: String,
    content: String,
}

/// Handles population of S3 buckets with mock data from filesystem
pub struct MockDataPopulator {
    mock_data_folder: PathBuf,
}

impl MockDataPopulator {
    /// Create new populator with specified mock data folder
    pub fn new(mock_data_folder: &str) -> Self {
        Self {
            mock_data_folder: PathBuf::from(mock_data_folder),
        }
    }

    /// Create populator using environment variable $MOCK_DATA_FOLDER
    pub fn from_env() -> Result<Self> {
        let folder = std::env::var("MOCK_DATA_FOLDER")
            .map_err(|_| anyhow::anyhow!("MOCK_DATA_FOLDER environment variable not set"))?;
        Ok(Self::new(&folder))
    }

    /// Populate input buckets with files from mock data folder
    pub async fn populate_input_buckets(
        &self,
        clients: &HashMap<String, aws_sdk_s3::Client>,
        bucket_mappings: &HashMap<String, String>, // container_name -> bucket_name
    ) -> Result<PopulationSummary> {
        let input_files = self.discover_input_files()?;
        let mut summary = PopulationSummary::new();

        // Distribute files across available input buckets
        let input_clients: Vec<_> = clients
            .iter()
            .filter(|(name, _)| name.starts_with("input"))
            .collect();

        if input_clients.is_empty() {
            return Err(anyhow::anyhow!("No input clients available for population"));
        }

        // Distribute files evenly across input buckets
        for (file_index, (file_path, content)) in input_files.into_iter().enumerate() {
            let client_index = file_index % input_clients.len();
            let (container_name, client) = input_clients[client_index];

            let bucket_name = bucket_mappings.get(container_name).ok_or_else(|| {
                anyhow::anyhow!("No bucket mapping for container {}", container_name)
            })?;

            let s3_key = self.generate_s3_key(&file_path)?;

            client
                .put_object()
                .bucket(bucket_name)
                .key(&s3_key)
                .body(ByteStream::from(content))
                .send()
                .await?;

            summary.add_file(container_name, bucket_name, &s3_key);
        }

        Ok(summary)
    }

    /// Populate buckets with synthetic test data that simulates consolidation scenario
    pub async fn populate_with_synthetic_data(
        &self,
        clients: &HashMap<String, aws_sdk_s3::Client>,
        bucket_mappings: &HashMap<String, String>,
        file_count_per_bucket: usize,
    ) -> Result<PopulationSummary> {
        let mut summary = PopulationSummary::new();

        // Step 1: Generate base log data
        let base_logs = self.generate_base_log_data(file_count_per_bucket)?;

        // Step 2: Populate input buckets with .gz files (original logs)
        for (container_name, client) in clients {
            if container_name.contains("input") {
                if let Some(bucket_name) = bucket_mappings.get(container_name) {
                    let input_files = self.create_input_files(&base_logs, container_name)?;

                    for (key, content) in input_files {
                        client
                            .put_object()
                            .bucket(bucket_name)
                            .key(&key)
                            .body(ByteStream::from(content))
                            .send()
                            .await?;

                        summary.add_file(container_name, bucket_name, &key);
                    }
                }
            }
        }

        // Step 3: Populate consolidated bucket with .zstd files (consolidated logs)
        if let Some(consolidated_client) = clients.get("consolidated") {
            if let Some(bucket_name) = bucket_mappings.get("consolidated") {
                let consolidated_files = self.create_consolidated_files(&base_logs)?;

                for (key, content) in consolidated_files {
                    consolidated_client
                        .put_object()
                        .bucket(bucket_name)
                        .key(&key)
                        .body(ByteStream::from(content))
                        .send()
                        .await?;

                    summary.add_file("consolidated", bucket_name, &key);
                }
            }
        }

        Ok(summary)
    }

    /// Discover and read all JSON.gz files from the inputs subfolder
    fn discover_input_files(&self) -> Result<Vec<(PathBuf, Vec<u8>)>> {
        let inputs_folder = self.mock_data_folder.join("inputs");

        if !inputs_folder.exists() {
            return Err(anyhow::anyhow!(
                "Inputs folder not found: {}",
                inputs_folder.display()
            ));
        }

        let mut files = Vec::new();

        for entry in fs::read_dir(&inputs_folder)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && self.is_supported_file(&path) {
                let content = fs::read(&path)?;
                files.push((path, content));
            }
        }

        if files.is_empty() {
            return Err(anyhow::anyhow!(
                "No supported files found in {}",
                inputs_folder.display()
            ));
        }

        Ok(files)
    }

    /// Check if file has supported extension (.json.gz, .json.zst, .gz)
    fn is_supported_file(&self, path: &Path) -> bool {
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            filename.ends_with(".json.gz")
                || filename.ends_with(".json.zst")
                || filename.ends_with(".gz")
        } else {
            false
        }
    }

    /// Generate realistic S3 key from file path
    fn generate_s3_key(&self, file_path: &Path) -> Result<String> {
        let filename = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid filename in path {:?}", file_path))?;

        // Extract service prefix from filename if possible
        let _service_prefix = self.extract_service_prefix(filename);

        // Generate realistic S3 key structure: raw-logs/dt=YYYYMMDD/hour=HH/prefix-service.ext
        let date = "20231225"; // Standard test date
        let hour = "14"; // Standard test hour

        Ok(format!("raw-logs/dt={}/hour={}/{}", date, hour, filename))
    }

    /// Extract service prefix from filename (app-, web-, api-, etc.)
    fn extract_service_prefix(&self, filename: &str) -> Option<&str> {
        let prefixes = ["app-", "web-", "api-", "db-", "cache-", "auth-"];

        for prefix in &prefixes {
            if filename.starts_with(prefix) {
                return Some(&prefix[..prefix.len() - 1]); // Remove trailing dash
            }
        }

        None
    }

    /// Generate base log data that will be used for both input and consolidated files
    fn generate_base_log_data(&self, total_entries: usize) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();
        let services = ["app", "web", "api", "db"];
        let environments = ["production", "staging"];

        for i in 0..total_entries {
            let service = services[i % services.len()];
            let env = environments[i % environments.len()];

            let entry = LogEntry {
                service: service.to_string(),
                environment: env.to_string(),
                content: super::factories::JsonLogFactory::create_realistic(
                    env,
                    service,
                    "info",
                    &format!("Log entry #{} for {} in {}", i, service, env),
                ),
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Create input files (.gz) from base log data - these represent original logs going into consolidator
    fn create_input_files(
        &self,
        base_logs: &[LogEntry],
        container_name: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let mut files = Vec::new();

        // Distribute logs across different input files (simulating original log files)
        let services = ["app", "web", "api", "db"];

        for service in &services {
            let service_logs: Vec<_> = base_logs
                .iter()
                .filter(|log| log.service == *service)
                .collect();

            if !service_logs.is_empty() {
                let key = format!(
                    "raw-logs/dt=20231225/hour=14/{}-{}.log.gz",
                    service,
                    container_name.replace("input-", "")
                );

                // Combine all logs for this service into one file
                let combined_content = service_logs
                    .iter()
                    .map(|log| log.content.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");

                // Compress with gzip (proper compression for input files)
                let compressed_content =
                    super::compression::compress_with_gzip(combined_content.as_bytes());
                files.push((key, compressed_content));
            }
        }

        Ok(files)
    }

    /// Create consolidated files (.zstd) from base log data - these represent logs after consolidation
    fn create_consolidated_files(&self, base_logs: &[LogEntry]) -> Result<Vec<(String, Vec<u8>)>> {
        let mut files = Vec::new();

        // Group by environment and service (this is what the consolidator does)
        let mut grouped = std::collections::HashMap::new();
        for log in base_logs {
            grouped
                .entry((log.environment.clone(), log.service.clone()))
                .or_insert_with(Vec::new)
                .push(log);
        }

        // Create consolidated files grouped by env-service
        for ((env, service), logs) in grouped {
            let key = format!(
                "raw-logs/dt=20231225/hour=14/{}-{}-consolidated.json.zst",
                env, service
            );

            // Combine all logs for this env-service combination
            let combined_content = logs
                .iter()
                .map(|log| log.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");

            // Compress with zstd (what consolidator produces)
            let compressed_content =
                super::compression::compress_with_zstd(combined_content.as_bytes());
            files.push((key, compressed_content));
        }

        Ok(files)
    }

    /// Generate synthetic test files for a specific container type (legacy method)
    fn generate_synthetic_files(
        &self,
        container_name: &str,
        count: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let mut files = Vec::new();

        let service_prefixes = match container_name {
            name if name.contains("input") => vec!["app", "web", "api"],
            name if name.contains("archive") => vec!["app", "web", "api", "db"],
            _ => vec!["service"],
        };

        for i in 0..count {
            let prefix = service_prefixes[i % service_prefixes.len()];
            let key = format!(
                "raw-logs/dt=20231225/hour=14/{}-service-{:03}.json",
                prefix, i
            );

            // Generate realistic JSON content
            let json_content = super::factories::JsonLogFactory::create_realistic(
                "production",
                &format!("{}-service", prefix),
                "info",
                &format!("Synthetic log entry #{} for {}", i, container_name),
            );

            // For testing purposes, use uncompressed content to avoid compression issues
            // In real scenarios, you would properly compress the data
            let content = json_content.as_bytes().to_vec();

            files.push((key, content));
        }

        Ok(files)
    }
}

/// Summary of population operation
#[derive(Debug)]
pub struct PopulationSummary {
    pub files_by_container: HashMap<String, Vec<String>>,
    pub total_files: usize,
}

impl PopulationSummary {
    fn new() -> Self {
        Self {
            files_by_container: HashMap::new(),
            total_files: 0,
        }
    }

    fn add_file(&mut self, container_name: &str, _bucket_name: &str, key: &str) {
        self.files_by_container
            .entry(container_name.to_string())
            .or_insert_with(Vec::new)
            .push(key.to_string());
        self.total_files += 1;
    }

    /// Get total number of files populated
    pub fn total_files(&self) -> usize {
        self.total_files
    }

    /// Get files populated for a specific container
    pub fn files_for_container(&self, container_name: &str) -> Option<&Vec<String>> {
        self.files_by_container.get(container_name)
    }

    /// Get summary by container type
    pub fn container_summary(&self) -> HashMap<String, usize> {
        self.files_by_container
            .iter()
            .map(|(name, files)| (name.clone(), files.len()))
            .collect()
    }
}

/// Utility functions for mock data population
pub struct PopulationUtils;

impl PopulationUtils {
    /// Create standard bucket mappings for 6-bucket environment
    pub fn create_standard_bucket_mappings() -> HashMap<String, String> {
        let mut mappings = HashMap::new();

        // Input containers
        mappings.insert("input-0".to_string(), "input-bucket-1".to_string());
        mappings.insert("input-1".to_string(), "input-bucket-2".to_string());

        // Archive containers (legacy support)
        mappings.insert("archive-0".to_string(), "archive-bucket-1".to_string());
        mappings.insert("archive-1".to_string(), "archive-bucket-2".to_string());

        // Consolidated and results containers
        mappings.insert(
            "consolidated".to_string(),
            "consolidated-bucket".to_string(),
        );
        mappings.insert("results".to_string(), "results-bucket".to_string());

        mappings
    }

    /// Validate that mock data folder structure is correct
    pub fn validate_mock_data_structure(mock_data_folder: &str) -> Result<()> {
        let base_path = Path::new(mock_data_folder);

        if !base_path.exists() {
            return Err(anyhow::anyhow!(
                "Mock data folder does not exist: {}",
                mock_data_folder
            ));
        }

        let inputs_path = base_path.join("inputs");
        if !inputs_path.exists() {
            return Err(anyhow::anyhow!(
                "Inputs subfolder does not exist: {}",
                inputs_path.display()
            ));
        }

        // Check if there are any supported files
        let has_files = fs::read_dir(&inputs_path)?
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                let path = entry.path();
                path.is_file()
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|name| {
                            name.ends_with(".json.gz")
                                || name.ends_with(".json.zst")
                                || name.ends_with(".gz")
                        })
                        .unwrap_or(false)
            });

        if !has_files {
            return Err(anyhow::anyhow!(
                "No supported files (.json.gz, .json.zst, .gz) found in {}",
                inputs_path.display()
            ));
        }

        Ok(())
    }
}
