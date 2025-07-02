use anyhow::Result;
use aws_sdk_s3::Client;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use std::path::Path;
use tokio::fs;
use zstd::stream::write::Encoder as ZstdEncoder;

use log_consolidator_checker_rust::config::loader::{
    get_archived_buckets, get_consolidated_buckets, load_config,
};
use log_consolidator_checker_rust::config::types::BucketConfig;
use log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter;

const TEST_DATE: &str = "20231225";
const TEST_HOUR: &str = "14";

pub struct MockDataGenerator {
    base_date: String,
    base_hour: String,
    test_dataset: String,
}

impl MockDataGenerator {
    pub fn new(test_dataset: String) -> Self {
        Self {
            base_date: TEST_DATE.to_string(),
            base_hour: TEST_HOUR.to_string(),
            test_dataset,
        }
    }

    /// Generate and upload all mock data to S3 buckets based on config
    pub async fn populate_all_buckets(&self, s3_client: &Client) -> Result<()> {
        let config = load_config(super::test_environment::TestConstants::MOCK_CONFIG_PATH)?;

        println!("Reading mock data from filesystem...");

        // Discover available environments from actual files
        let available_envs = self.discover_available_environments().await?;
        println!("Available environments in dataset {}: {:?}", self.test_dataset, available_envs);

        // Generate input buckets data
        let archived_buckets = get_archived_buckets(&config);
        println!("Processing {} input buckets...", archived_buckets.len());

        for (i, bucket_config) in archived_buckets.iter().enumerate() {
            let bucket_env = bucket_config
                .extra
                .get("force_env")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            
            println!(
                "Processing input bucket {}/{}: {} (env: {})",
                i + 1,
                archived_buckets.len(),
                bucket_config.bucket,
                bucket_env
            );
            
            // Only populate if we have data for this environment
            if available_envs.contains(bucket_env) {
                self.populate_input_bucket(s3_client, bucket_config).await?;
            } else {
                println!("  -> Skipping bucket {} - no data available for environment '{}'", bucket_config.bucket, bucket_env);
            }
        }

        // Generate consolidated bucket data
        let consolidated_buckets = get_consolidated_buckets(&config);
        if let Some(consolidated_bucket) = consolidated_buckets.first() {
            println!(
                "Processing consolidated bucket: {}",
                consolidated_bucket.bucket
            );
            self.populate_consolidated_bucket(s3_client, consolidated_bucket)
                .await?;
        }

        // Note: Results bucket is populated by the checker itself, not by mock generator
        println!("Results bucket will be populated by the checker during tests");

        Ok(())
    }

    /// Discover available environments from input files
    async fn discover_available_environments(&self) -> Result<std::collections::HashSet<String>> {
        use std::collections::HashSet;
        
        let input_dir = format!("tests/mock_data/{}/inputs", self.test_dataset);
        let mut envs = HashSet::new();
        
        if let Ok(mut entries) = fs::read_dir(&input_dir).await {
            while let Some(entry) = entries.next_entry().await? {
                if let Some(file_name) = entry.file_name().to_str() {
                    if file_name.ends_with(".json") {
                        let name_without_ext = file_name.strip_suffix(".json").unwrap();
                        if let Some((_, env)) = name_without_ext.split_once('-') {
                            envs.insert(env.to_string());
                        }
                    }
                }
            }
        }
        
        Ok(envs)
    }

    /// Discover input files and extract service/env combinations
    async fn discover_input_files(&self) -> Result<Vec<(String, String)>> {
        let input_dir = format!("tests/mock_data/{}/inputs", self.test_dataset);
        let mut entries = fs::read_dir(&input_dir).await?;
        let mut combinations = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with(".json") {
                    let name_without_ext = file_name.strip_suffix(".json").unwrap();
                    if let Some((service, env)) = name_without_ext.split_once('-') {
                        combinations.push((service.to_string(), env.to_string()));
                    }
                }
            }
        }

        Ok(combinations)
    }

    /// Populate input bucket with compressed .gz files
    async fn populate_input_bucket(
        &self,
        s3_client: &Client,
        bucket_config: &BucketConfig,
    ) -> Result<()> {
        // Extract force_env from extra HashMap
        let bucket_env = bucket_config
            .extra
            .get("force_env")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Generate path prefix using the formatter
        let formatter = generate_path_formatter(bucket_config);
        let path_prefix = formatter(&self.base_date, &self.base_hour)?;

        // Discover all input files for this bucket's environment
        let combinations = self.discover_input_files().await?;
        
        let mut files_processed = 0;
        for (service, env) in combinations {
            // Only process files matching this bucket's environment
            if env == bucket_env {
                let file_name = format!("{}-{}.json", service, env);
                let input_file_path =
                    format!("tests/mock_data/{}/inputs/{}", self.test_dataset, file_name);

                // Read the JSON file from filesystem
                if Path::new(&input_file_path).exists() {
                    println!(
                        "Processing input file: {} from {}",
                        file_name, input_file_path
                    );

                    let json_content = fs::read(&input_file_path).await?;
                    let original_size = json_content.len();

                    // Count lines in the JSON file for logging
                    let line_count = json_content.iter().filter(|&&b| b == b'\n').count();

                    // Compress with gzip
                    let compressed_data = self.compress_with_gzip(&json_content)?;
                    let compressed_size = compressed_data.len();
                    let compression_ratio =
                        (compressed_size as f64 / original_size as f64 * 100.0) as u32;

                    // Generate S3 key
                    let s3_key = format!("{}/{}.gz", path_prefix, file_name);

                    // Upload to S3
                    s3_client
                        .put_object()
                        .bucket(&bucket_config.bucket)
                        .key(&s3_key)
                        .body(compressed_data.into())
                        .content_type("application/gzip")
                        .send()
                        .await?;

                    println!(
                        "  -> {}: {} lines, {} bytes -> {} bytes gzipped ({}%) -> s3://{}/{}",
                        file_name,
                        line_count,
                        original_size,
                        compressed_size,
                        compression_ratio,
                        bucket_config.bucket,
                        s3_key
                    );
                    
                    files_processed += 1;
                }
            }
        }

        if files_processed == 0 {
            println!("  -> No input files found for environment '{}' in bucket {}", bucket_env, bucket_config.bucket);
        }

        Ok(())
    }

    /// Discover consolidated files and extract env/service combinations
    async fn discover_consolidated_files(&self) -> Result<Vec<(String, String)>> {
        let consolidated_dir = format!("tests/mock_data/{}/consolidated", self.test_dataset);
        let mut entries = fs::read_dir(&consolidated_dir).await?;
        let mut combinations = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with("-consolidated.json") {
                    let name_without_suffix = file_name.strip_suffix("-consolidated.json").unwrap();
                    if let Some((env, service)) = name_without_suffix.split_once('-') {
                        combinations.push((env.to_string(), service.to_string()));
                    }
                }
            }
        }

        Ok(combinations)
    }

    /// Populate consolidated bucket with compressed .zst files
    async fn populate_consolidated_bucket(
        &self,
        s3_client: &Client,
        bucket_config: &BucketConfig,
    ) -> Result<()> {
        // Generate path prefix using the formatter
        let formatter = generate_path_formatter(bucket_config);
        let path_prefix = formatter(&self.base_date, &self.base_hour)?;

        // Discover all consolidated files
        let combinations = self.discover_consolidated_files().await?;

        for (env, service) in combinations {
            let consolidated_file_name = format!("{}-{}-consolidated.json", env, service);
            let input_file_path =
                format!("tests/mock_data/{}/consolidated/{}", self.test_dataset, consolidated_file_name);

            // Read the consolidated JSON file from filesystem
            if Path::new(&input_file_path).exists() {
                let json_content = fs::read(&input_file_path).await?;
                let original_size = json_content.len();

                // Count lines in the JSON file for logging
                let line_count = json_content.iter().filter(|&&b| b == b'\n').count();

                // Compress with zstd
                let compressed_data = self.compress_with_zstd(&json_content)?;
                let compressed_size = compressed_data.len();
                let compression_ratio =
                    (compressed_size as f64 / original_size as f64 * 100.0) as u32;

                // Generate S3 key
                let s3_key = format!("{}/{}.zst", path_prefix, consolidated_file_name);

                // Upload to S3
                s3_client
                    .put_object()
                    .bucket(&bucket_config.bucket)
                    .key(&s3_key)
                    .body(compressed_data.into())
                    .content_type("application/zstd")
                    .send()
                    .await?;

                println!(
                    "  -> {}: {} lines, {} bytes -> {} bytes zstd ({}%) -> s3://{}/{}",
                    consolidated_file_name,
                    line_count,
                    original_size,
                    compressed_size,
                    compression_ratio,
                    bucket_config.bucket,
                    s3_key
                );
            }
        }

        Ok(())
    }

    /// Compress data using gzip
    fn compress_with_gzip(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    /// Compress data using zstd
    fn compress_with_zstd(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = ZstdEncoder::new(Vec::new(), 3)?; // compression level 3
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
}
