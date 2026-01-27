// src/search_executor.rs
use crate::config::types::{BucketConfig, S3ObjectInfo};
use crate::s3::client::WrappedS3Client;
use crate::s3::{
    StreamingDownloader, StreamingDownloaderConfig, StreamingSearchConfig, StreamingSearchExecutor,
};
use crate::search::{SearchResultCollector, StreamSearcher, StreamingSearchCollector};
use crate::utils::date::date_range_to_date_hour_list;
use crate::utils::path_formatter::generate_path_formatter;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

pub struct SearchExecutor {
    s3_client: WrappedS3Client,
    searcher: Arc<StreamSearcher>,
    max_parallel: usize,
    buffer_size_kb: usize,
    channel_buffer: usize,
    max_retries: u32,
    retry_delay: u64,
}

impl SearchExecutor {
    pub fn new(
        s3_client: WrappedS3Client,
        searcher: Arc<StreamSearcher>,
        max_parallel: usize,
        buffer_size_kb: usize,
        channel_buffer: usize,
        max_retries: u32,
        retry_delay: u64,
    ) -> Self {
        Self {
            s3_client,
            searcher,
            max_parallel,
            buffer_size_kb,
            channel_buffer,
            max_retries,
            retry_delay,
        }
    }

    /// Execute search with streaming output
    pub async fn search_streaming(
        &mut self,
        simple_buckets: &[String],
        config_buckets: &[&BucketConfig],
        filter: Option<&str>,
        start_date: Option<DateTime<Utc>>,
        end_date: DateTime<Utc>,
        output_dir: String,
    ) -> Result<(usize, usize, u64, usize)> {
        // Create streaming collector
        let collector = Arc::new(StreamingSearchCollector::new(output_dir)?);

        // Configure streaming executor
        let config = StreamingSearchConfig {
            max_concurrent_downloads: self.max_parallel,
            buffer_size_bytes: self.buffer_size_kb * 1024,
            channel_buffer_size: self.channel_buffer,
            max_retries: self.max_retries,
            initial_retry_delay: Duration::from_secs(self.retry_delay),
        };

        let executor = StreamingSearchExecutor::new(self.s3_client.get_client().await?, config);

        // Process all buckets
        for bucket in simple_buckets {
            let objects = self
                .get_bucket_objects(bucket, "", filter, start_date, end_date)
                .await?;
            if !objects.is_empty() {
                executor
                    .search_objects_streaming(&objects, self.searcher.clone(), collector.clone())
                    .await?;
            }
        }

        for bucket_cfg in config_buckets {
            let objects = self
                .get_config_bucket_objects(bucket_cfg, filter, start_date, end_date)
                .await?;
            if !objects.is_empty() {
                executor
                    .search_objects_streaming(&objects, self.searcher.clone(), collector.clone())
                    .await?;
            }
        }

        // Get final stats
        let (files_searched, files_with_matches, total_matches) = collector.get_stats().await;

        // Finish streaming and get number of files written
        let files_written = Arc::try_unwrap(collector)
            .map_err(|_| anyhow::anyhow!("Failed to unwrap collector"))?
            .finish()
            .await?;

        Ok((
            files_searched,
            files_with_matches,
            total_matches,
            files_written,
        ))
    }

    /// Execute search with buffered output (original implementation)
    pub async fn search_buffered(
        &mut self,
        simple_buckets: &[String],
        config_buckets: &[&BucketConfig],
        filter: Option<&str>,
        start_date: Option<DateTime<Utc>>,
        end_date: DateTime<Utc>,
    ) -> Result<crate::search::SearchResult> {
        let collector = Arc::new(tokio::sync::Mutex::new(SearchResultCollector::new()));

        // Configure downloader
        let download_config = StreamingDownloaderConfig {
            max_concurrent_downloads: self.max_parallel,
            buffer_size_bytes: self.buffer_size_kb * 1024,
            channel_buffer_size: self.channel_buffer,
            max_retries: self.max_retries,
            initial_retry_delay: Duration::from_secs(self.retry_delay),
        };

        let downloader =
            StreamingDownloader::new(self.s3_client.get_client().await?, download_config);

        // Process all buckets
        for bucket in simple_buckets {
            let objects = self
                .get_bucket_objects(bucket, "", filter, start_date, end_date)
                .await?;
            if !objects.is_empty() {
                downloader
                    .search_objects(&objects, self.searcher.clone(), collector.clone())
                    .await?;
            }
        }

        for bucket_cfg in config_buckets {
            let objects = self
                .get_config_bucket_objects(bucket_cfg, filter, start_date, end_date)
                .await?;
            if !objects.is_empty() {
                downloader
                    .search_objects(&objects, self.searcher.clone(), collector.clone())
                    .await?;
            }
        }

        // Get and return results
        let final_collector = Arc::try_unwrap(collector)
            .map_err(|_| anyhow::anyhow!("Failed to get collector"))?
            .into_inner();

        Ok(final_collector.into_result())
    }

    /// Get objects from a simple bucket
    async fn get_bucket_objects(
        &mut self,
        bucket: &str,
        prefix: &str,
        filter: Option<&str>,
        start_date: Option<DateTime<Utc>>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<S3ObjectInfo>> {
        info!("Searching bucket: {}", bucket);

        let mut all_objects = if let Some(start) = start_date {
            let date_hours = date_range_to_date_hour_list(&start, &end_date)?;

            let mut objects = Vec::new();
            for dh in date_hours {
                let prefix = if prefix.is_empty() {
                    format!("{}/{}", dh.date, dh.hour)
                } else {
                    format!("{}/{}/{}", prefix, dh.date, dh.hour)
                };

                let objs: Vec<S3ObjectInfo> = self
                    .s3_client
                    .get_matching_filenames_from_s3(bucket, &prefix, filter)
                    .await?;

                if !objs.is_empty() {
                    info!("Found {} objects in prefix {}", objs.len(), prefix);
                    objects.extend(objs);
                }
            }
            objects
        } else {
            self.s3_client
                .get_matching_filenames_from_s3(bucket, prefix, filter)
                .await?
        };

        // Sort by size for better load balancing
        all_objects.sort_by_key(|o| o.size);

        if !all_objects.is_empty() {
            let total_size: usize = all_objects.iter().map(|o| o.size).sum();
            info!(
                "Processing {} objects ({} MB) in {}",
                all_objects.len(),
                total_size / 1_000_000,
                bucket
            );
        }

        Ok(all_objects)
    }

    /// Get objects from a config bucket with path formatting
    async fn get_config_bucket_objects(
        &mut self,
        bucket_cfg: &BucketConfig,
        filter: Option<&str>,
        start_date: Option<DateTime<Utc>>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<S3ObjectInfo>> {
        info!("Searching bucket: {}", bucket_cfg.bucket);

        if let Some(start) = start_date {
            let date_hours = date_range_to_date_hour_list(&start, &end_date)?;
            let formatter = generate_path_formatter(bucket_cfg);

            let mut all_objects = Vec::new();
            for dh in date_hours {
                let prefix = formatter(&dh.date, &dh.hour)?;
                let objs: Vec<S3ObjectInfo> = self
                    .s3_client
                    .get_matching_filenames_from_s3(&bucket_cfg.bucket, &prefix, filter)
                    .await?;

                if !objs.is_empty() {
                    debug!("Found {} objects in prefix {}", objs.len(), prefix);
                    all_objects.extend(objs);
                }
            }

            // Sort by size for better load balancing
            all_objects.sort_by_key(|o| o.size);

            if !all_objects.is_empty() {
                let total_size: usize = all_objects.iter().map(|o| o.size).sum();
                info!(
                    "Processing {} objects ({} MB) across all prefixes",
                    all_objects.len(),
                    total_size / 1_000_000
                );
            }

            Ok(all_objects)
        } else {
            let prefix = if bucket_cfg.path.is_empty() {
                String::new()
            } else {
                let formatter = generate_path_formatter(bucket_cfg);
                formatter(&String::new(), &String::new())?
            };

            let objects = self
                .s3_client
                .get_matching_filenames_from_s3(&bucket_cfg.bucket, &prefix, filter)
                .await?;

            if !objects.is_empty() {
                info!("Found {} objects to search", objects.len());
            }

            Ok(objects)
        }
    }
}
