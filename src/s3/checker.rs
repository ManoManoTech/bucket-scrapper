// src/s3/checker.rs
// Location: src/s3/checker.rs

use crate::config::types::{BucketConfig, DateString, HourString, S3FileList, S3ObjectInfo};
use crate::s3::client::WrappedS3Client;
use crate::s3::downloader::DownloadOrchestrator;
use crate::utils::character_counter::DetailedCharacterCount;
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::signal_handler::{MemoryMonitor, ProgressTracker};
use anyhow::Result;
use chrono::Utc;
use crc32fast::Hasher;
use rand::prelude::SliceRandom;
use rand::rng;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Analysis data for a bucket at a specific date/hour
#[derive(Debug)]
pub struct AnalysisData {
    /// The bucket name
    pub bucket: String,
    /// The key prefix
    pub key_prefix: String,
    /// The files found
    pub files: Vec<String>,
    /// Total archives size
    pub total_archives_size: usize,
    /// Detailed character count information
    pub detailed_character_count: DetailedCharacterCount,
}

/// Result of comparing archived and consolidated data
#[derive(Debug)]
pub struct ComparisonResult {
    /// Whether the comparison was successful
    pub ok: bool,
    /// The date of the comparison
    pub date: DateString,
    /// The hour of the comparison
    pub hour: HourString,
    /// A message describing the result
    pub message: String,
    /// Analysis data for the consolidated bucket
    // pub consolidated_data: AnalysisData,
    /// Analysis data for the archived buckets
    // pub archived_data: Vec<AnalysisData>,
    /// When the analysis started
    pub analysis_start_date: String,
    /// When the analysis ended
    pub analysis_end_date: String,
}

pub struct Checker {
    s3_client: WrappedS3Client,
    max_parallel: usize,
    max_process_threads: usize,
    memory_pool_mb: usize,
    memory_allocator: Arc<MemoryLimitedAllocator>,
    progress_tracker: Arc<ProgressTracker>,
}

impl Checker {
    pub fn new(
        s3_client: WrappedS3Client,
        max_parallel: usize,
        max_process_threads: Option<usize>,
        memory_pool_mb: usize,
    ) -> Self {
        let max_process_threads = max_process_threads.unwrap_or_else(|| num_cpus::get() - 2usize);
        let memory_pool_bytes = memory_pool_mb * 1024 * 1024; // MB to Bytes
        let memory_allocator = Arc::new(MemoryLimitedAllocator::new(memory_pool_bytes));
        let progress_tracker = Arc::new(ProgressTracker::default());

        Self {
            s3_client,
            max_parallel,
            max_process_threads,
            memory_pool_mb,
            memory_allocator,
            progress_tracker,
        }
    }

    /// Get the memory monitor for signal handling
    pub fn get_memory_monitor(&self) -> Option<MemoryMonitor> {
        Some(MemoryMonitor::with_progress(
            Arc::clone(&self.memory_allocator),
            self.progress_tracker.clone(),
        ))
    }

    pub async fn list_bucket_files(
        &self,
        bucket_config: &BucketConfig,
        date: &DateString,
        hour: &HourString,
    ) -> Result<S3FileList> {
        let bucket = &bucket_config.bucket;
        let key_prefix = {
            let formatter = crate::utils::path_formatter::generate_path_formatter(bucket_config);
            formatter(date, hour)?
        };

        info!("Enumerating files for {} {}", bucket, key_prefix);
        let file_list = self
            .s3_client
            .get_matching_filenames_from_s3(bucket_config, date, hour, true)
            .await?;

        info!(
            "Bucket {}: {} files, totaling {} bytes, file list checksum {}",
            bucket,
            file_list.files.len(),
            file_list.total_archives_size,
            file_list.checksum
        );
        Ok(file_list)
    }

    /// Analyze a bucket for a specific date and hour
    pub async fn download_and_analyze_files(
        &self,
        objects: &[S3ObjectInfo],
    ) -> Result<HashMap<String, DetailedCharacterCount>> {
        let downloader = DownloadOrchestrator::new(
            self.s3_client.get_client().await?,
            self.max_parallel,
            self.max_process_threads,
            Arc::clone(&self.memory_allocator),
            Arc::clone(&self.progress_tracker),
        );

        // Process all files in parallel in a single batch for maximum efficiency
        info!(
            "Processing {} files with {} parallel downloads, {} dedicated processing threads, {}MB memory pool",
            objects.len(),
            self.max_parallel,
            self.max_process_threads,
            self.memory_pool_mb
        );

        Ok(downloader.download_decompress_count(objects).await?)

        // // Get real filenames
        // let real_filenames = objects
        //     .iter()
        //     .map(|f| f.key.clone())
        //     .collect::<Vec<_>>();
        //
        // info!("Analysis is complete.");
        //
        // Ok(AnalysisData {
        //     files: real_filenames,
        //     bucket: file_list.bucket.clone(),
        //     key_prefix: file_list.key_prefix.clone(),
        //     detailed_character_count: total_character_count,
        //     total_archives_size: file_list.total_archives_size,
        // })
    }

    /// Compare archived and consolidated data for a specific date and hour
    pub async fn get_comparison_results(
        &self,
        archived_bucket_configs: &[&BucketConfig],
        consolidated_bucket_config: &BucketConfig,
        date: &DateString,
        hour: &HourString,
    ) -> Result<ComparisonResult> {
        let start_time = Utc::now();

        info!("Getting comparison results for {}/{}", date, hour);

        // Start listing files
        info!("Listing files accross buckets");
        let mut file_lists_futures = Vec::new();
        file_lists_futures.push(self.list_bucket_files(consolidated_bucket_config, date, hour));
        for &archived_bucket_config in archived_bucket_configs {
            let future = self.list_bucket_files(archived_bucket_config, date, hour);
            file_lists_futures.push(future);
        }

        // Sort it all in one big filelist
        let mut file_lists = Vec::new();
        {
            let results = futures::future::join_all(file_lists_futures).await;
            for result in results {
                match result {
                    Ok(mut analysis) => file_lists.append(analysis.files.as_mut()),
                    Err(e) => return Err(anyhow::anyhow!("Failed to list files in bucket: {}", e)),
                }
            }

            // let mut rng = rng();
            // file_lists.shuffle(&mut rng);
            file_lists.sort_by(|a, b| b.size.cmp(&a.size));
        }

        let character_counts_by_bucket = self.download_and_analyze_files(&file_lists[..]).await?;

        // Analyze the archived buckets in parallel
        info!("Analyzing archived buckets");

        // Compute the sum of all archived character counts
        let mut total_archived_counts = DetailedCharacterCount::new();
        for archived_info in archived_bucket_configs {
            let bucket_character_count = character_counts_by_bucket
                .get(&archived_info.bucket)
                .unwrap();
            total_archived_counts.add(&bucket_character_count);
        }

        // Find the consolidated character count
        let consolidated_character_count = character_counts_by_bucket
            .get(&consolidated_bucket_config.bucket)
            .unwrap();

        // Compare character counts
        let (detailed_ok, differences) =
            total_archived_counts.compare(&consolidated_character_count);

        // Generate result
        let end_time = Utc::now();

        let result = if detailed_ok {
            info!("No size difference for {}/{}", date, hour);
            ComparisonResult {
                date: date.clone(),
                hour: hour.clone(),
                ok: true,
                message: "Consolidated size matches original size".to_string(),
                // consolidated_data: consolidated_info,
                // archived_data: archived_infos,
                analysis_start_date: start_time.to_rfc3339(),
                analysis_end_date: end_time.to_rfc3339(),
            }
        } else {
            // Format differences for logging
            let recap_str = {
                let mut diff_vec: Vec<_> = differences.iter().collect();
                diff_vec.sort_by_key(|(key, _)| *key);
                diff_vec
                    .iter()
                    .map(|(key, value)| format!("{}:{}", key, value))
                    .collect::<Vec<_>>()
                    .join(" ")
            };

            let mut hasher = Hasher::new();
            hasher.update(recap_str.as_bytes());
            let difference_hash = format!("{:x}", hasher.finalize());
            let difference_total: i64 = differences.values().sum();

            warn!(
                "Differences {} on {} characters in detailedCharacterCount (dH={})",
                difference_total,
                differences.len(),
                difference_hash
            );

            ComparisonResult {
                date: date.clone(),
                hour: hour.clone(),
                ok: false,
                message: "Difference in detailedCharacterCount".to_string(),
                // consolidated_data: consolidated_info,
                // archived_data: archived_infos,
                analysis_start_date: start_time.to_rfc3339(),
                analysis_end_date: end_time.to_rfc3339(),
            }
        };

        Ok(result)
    }
}
