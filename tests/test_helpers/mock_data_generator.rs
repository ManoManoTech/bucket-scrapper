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

const SERVICES: &[&str] = &["app", "web"];
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

        // Generate input buckets data
        let archived_buckets = get_archived_buckets(&config);
        println!("Processing {} input buckets...", archived_buckets.len());

        for (i, bucket_config) in archived_buckets.iter().enumerate() {
            println!(
                "Processing input bucket {}/{}: {}",
                i + 1,
                archived_buckets.len(),
                bucket_config.bucket
            );
            self.populate_input_bucket(s3_client, bucket_config).await?;
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

    /// Populate input bucket with compressed .gz files
    async fn populate_input_bucket(
        &self,
        s3_client: &Client,
        bucket_config: &BucketConfig,
    ) -> Result<()> {
        // Extract force_env from extra HashMap
        let env = bucket_config
            .extra
            .get("force_env")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Generate path prefix using the formatter
        let formatter = generate_path_formatter(bucket_config);
        let path_prefix = formatter(&self.base_date, &self.base_hour)?;

        for service in SERVICES {
            let file_name = format!("{}-{}.json", service, env);
            let input_file_path =
                format!("tests/mock_data/{}/inputs/{}", self.test_dataset, file_name);
            println!(
                "Processing input file: {} from {}",
                file_name, input_file_path
            );

            // Read the JSON file from filesystem
            if Path::new(&input_file_path).exists() {
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
            }
        }

        Ok(())
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

        // Loop through all environment-service combinations from input data
        let envs = &["support", "int", "stg"];

        for env in envs {
            for service in SERVICES {
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
