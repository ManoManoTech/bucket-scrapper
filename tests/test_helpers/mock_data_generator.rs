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

// Constants for mock data generation
const FILES_PER_INPUT_BUCKET: usize = 2; // app and web services
const SERVICES: &[&str] = &["app", "web"];
const TEST_DATE: &str = "20231225";
const TEST_HOUR: &str = "14";

pub struct MockDataGenerator {
    base_date: String,
    base_hour: String,
}

impl MockDataGenerator {
    pub fn new() -> Self {
        Self {
            base_date: TEST_DATE.to_string(),
            base_hour: TEST_HOUR.to_string(),
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
            println!("Processing input bucket {}/{}: {}",
                i + 1, archived_buckets.len(), bucket_config.bucket);
            self.populate_input_bucket(s3_client, bucket_config).await?;
        }

        // Generate consolidated bucket data
        let consolidated_buckets = get_consolidated_buckets(&config);
        if let Some(consolidated_bucket) = consolidated_buckets.first() {
            println!("Processing consolidated bucket: {}", consolidated_bucket.bucket);
            self.populate_consolidated_bucket(s3_client, consolidated_bucket).await?;
        }

        // Note: Results bucket is populated by the checker itself, not by mock generator
        println!("Results bucket will be populated by the checker during tests");

        Ok(())
    }

    /// Populate input bucket with compressed .gz files
    async fn populate_input_bucket(&self, s3_client: &Client, bucket_config: &BucketConfig) -> Result<()> {
        // Extract force_env from extra HashMap
        let env = bucket_config.extra.get("force_env")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Generate path prefix using the formatter
        let formatter = generate_path_formatter(bucket_config);
        let path_prefix = formatter(&self.base_date, &self.base_hour)?;

        for service in SERVICES {
            let file_name = format!("{}-{}.json", service, env);
            let input_file_path = format!("tests/mock_data/inputs/{}", file_name);

            // Read the JSON file from filesystem
            if Path::new(&input_file_path).exists() {
                let json_content = fs::read(&input_file_path).await?;
                let original_size = json_content.len();

                // Count lines in the JSON file for logging
                let line_count = json_content.iter().filter(|&&b| b == b'\n').count();

                // Compress with gzip
                let compressed_data = self.compress_with_gzip(&json_content)?;
                let compressed_size = compressed_data.len();
                let compression_ratio = (compressed_size as f64 / original_size as f64 * 100.0) as u32;

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

                println!("  -> {}: {} lines, {} bytes -> {} bytes gzipped ({}%) -> s3://{}/{}",
                    file_name, line_count, original_size, compressed_size, compression_ratio,
                    bucket_config.bucket, s3_key);
            }
        }

        Ok(())
    }

    /// Populate consolidated bucket with compressed .zst files
    async fn populate_consolidated_bucket(&self, s3_client: &Client, bucket_config: &BucketConfig) -> Result<()> {
        // Generate path prefix using the formatter
        let formatter = generate_path_formatter(bucket_config);
        let path_prefix = formatter(&self.base_date, &self.base_hour)?;

        // Loop through all environment-service combinations from input data
        let envs = &["support", "int", "stg"];

        for env in envs {
            for service in SERVICES {
                let consolidated_file_name = format!("{}-{}-consolidated.json", env, service);
                let input_file_path = format!("tests/mock_data/consolidated/{}", consolidated_file_name);

                // Read the consolidated JSON file from filesystem
                if Path::new(&input_file_path).exists() {
                    let json_content = fs::read(&input_file_path).await?;
                    let original_size = json_content.len();

                    // Count lines in the JSON file for logging
                    let line_count = json_content.iter().filter(|&&b| b == b'\n').count();

                    // Compress with zstd
                    let compressed_data = self.compress_with_zstd(&json_content)?;
                    let compressed_size = compressed_data.len();
                    let compression_ratio = (compressed_size as f64 / original_size as f64 * 100.0) as u32;

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

                    println!("  -> {}: {} lines, {} bytes -> {} bytes zstd ({}%) -> s3://{}/{}",
                        consolidated_file_name, line_count, original_size, compressed_size, compression_ratio,
                        bucket_config.bucket, s3_key);
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

impl Default for MockDataGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_with_gzip() {
        let generator = MockDataGenerator::new();
        let test_data = b"test data for compression";
        let compressed = generator.compress_with_gzip(test_data).unwrap();

        assert!(compressed.len() > 0);
        assert!(compressed.len() < test_data.len() + 100); // Should be somewhat compressed
    }

    #[test]
    fn test_compress_with_zstd() {
        let generator = MockDataGenerator::new();
        let test_data = b"test data for zstd compression";
        let compressed = generator.compress_with_zstd(test_data).unwrap();

        assert!(compressed.len() > 0);
        assert!(compressed.len() < test_data.len() + 100); // Should be somewhat compressed
    }

    #[test]
    fn test_constants() {
        assert_eq!(FILES_PER_INPUT_BUCKET, 2);
        assert_eq!(SERVICES.len(), 2);
        assert_eq!(SERVICES[0], "app");
        assert_eq!(SERVICES[1], "web");
    }
}
