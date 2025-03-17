// src/s3/checker.rs
// Location: src/s3/checker.rs

use crate::config::types::{BucketConfig, DateString, HourString, S3ObjectInfo};
use crate::s3::client::S3Client;
use crate::s3::downloader::S3Downloader;
use crate::utils::character_counter::DetailedCharacterCount;
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::signal_handler::{MemoryMonitor, ProgressTracker};
use anyhow::Result;
use chrono::Utc;
use crc32fast::Hasher;
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Information about a processed S3 object
#[derive(Debug)]
pub struct ProcessedObjectInfo {
    /// The object key
    pub key: String,
    /// The uncompressed size in bytes
    pub size: usize,
    /// Detailed character count information
    pub detailed_character_count: DetailedCharacterCount,
}

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
    pub consolidated_data: AnalysisData,
    /// Analysis data for the archived buckets
    pub archived_data: Vec<AnalysisData>,
    /// When the analysis started
    pub analysis_start_date: String,
    /// When the analysis ended
    pub analysis_end_date: String,
}

pub struct Checker {
    s3_client: Arc<Mutex<S3Client>>,
    max_parallel: usize,
    process_threads: usize,
    memory_pool_mb: usize,
    memory_allocator: Arc<MemoryLimitedAllocator>,
    progress_tracker: Option<Arc<ProgressTracker>>,
}

impl Checker {
    pub fn new(
        s3_client: S3Client,
        max_parallel: usize,
        process_threads: Option<usize>,
        memory_pool_mb: usize,
    ) -> Self {
        let threads = process_threads.unwrap_or_else(num_cpus::get);

        // Convert MB to bytes for allocator
        let memory_pool_bytes = memory_pool_mb * 1024 * 1024;

        // Create memory allocator
        let memory_allocator = Arc::new(MemoryLimitedAllocator::new(memory_pool_bytes));

        // Create progress tracker
        let progress_tracker = Arc::new(ProgressTracker::default());

        Self {
            s3_client: Arc::new(Mutex::new(s3_client)),
            max_parallel,
            process_threads: threads,
            memory_pool_mb,
            memory_allocator,
            progress_tracker: Some(progress_tracker),
        }
    }

    /// Get the memory monitor for signal handling
    pub fn get_memory_monitor(&self) -> Option<MemoryMonitor> {
        Some(MemoryMonitor::with_progress(
            Arc::clone(&self.memory_allocator),
            self.progress_tracker.clone()?
        ))
    }

    /// Analyze a bucket for a specific date and hour
    pub async fn analyze_bucket(
        &self,
        bucket_config: &BucketConfig,
        date: &DateString,
        hour: &HourString,
    ) -> Result<AnalysisData> {
        let key_prefix = {
            let formatter = crate::utils::path_formatter::generate_path_formatter(bucket_config);
            formatter(date, hour)?
        };

        let bucket = &bucket_config.bucket;

        info!("Enumerating files for {} {}", bucket, key_prefix);

        let file_list = {
            let mut client = self.s3_client.lock().await;
            client
                .get_matching_filenames_from_s3(bucket_config, date, hour, true)
                .await?
        };

        let files_uncompressed_size = file_list.filenames.iter().map(|f| f.size).sum::<usize>();
        debug!(
            "{} files, totaling {} bytes",
            file_list.filenames.len(),
            files_uncompressed_size
        );

        // Calculate a checksum of all filenames to enable identifying changes in file lists
        let files_checksum = {
            let mut filenames = file_list
                .filenames
                .iter()
                .map(|f| f.key.clone())
                .collect::<Vec<_>>();
            filenames.sort();
            let joined = filenames.join("");
            format!("{:x}", md5::compute(joined))
        };

        info!(
            "Starting analysis for all files ({} files, {} bytes of archives) in bucket {} (filelist checksum {})",
            file_list.filenames.len(),
            file_list.total_archives_size,
            bucket,
            files_checksum
        );

        // Get access to raw S3 client for downloader
        let s3_raw_client = {
            let client = self.s3_client.lock().await;
            client.get_raw_client().await?
        };

        // Create downloader with memory allocator
        let mut downloader = S3Downloader::new_with_allocator(
            s3_raw_client,
            self.max_parallel,
            self.process_threads,
            Arc::clone(&self.memory_allocator)
        );

        // Set the progress tracker
        if let Some(tracker) = &self.progress_tracker {
            downloader.set_progress_tracker(Arc::clone(tracker));
        }

        // Process all files in parallel in a single batch for maximum efficiency
        info!(
            "Processing {} files with {} parallel downloads, {} CPU threads, {}MB memory pool",
            file_list.filenames.len(),
            self.max_parallel,
            self.process_threads,
            self.memory_pool_mb
        );

        let total_character_count = downloader
            .process_objects(bucket, &file_list.filenames)
            .await?;

        // Get real filenames
        let real_filenames = file_list
            .filenames
            .iter()
            .map(|f| f.key.clone())
            .collect::<Vec<_>>();

        info!(
            "Finished download and decompress for {}/{}",
            bucket, key_prefix
        );

        Ok(AnalysisData {
            files: real_filenames,
            bucket: bucket.to_string(),
            key_prefix,
            detailed_character_count: total_character_count,
            total_archives_size: file_list.total_archives_size,
        })
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

        // Analyze the consolidated bucket
        info!("Analyzing consolidated bucket");
        let consolidated_info = self
            .analyze_bucket(consolidated_bucket_config, date, hour)
            .await?;

        // Analyze the archived buckets in parallel
        info!("Analyzing archived buckets");
        let mut archived_infos = Vec::new();
        let mut archived_futures = Vec::new();

        for &archived_bucket_config in archived_bucket_configs {
            let future = self.analyze_bucket(archived_bucket_config, date, hour);
            archived_futures.push(future);
        }

        let results = futures::future::join_all(archived_futures).await;
        for result in results {
            match result {
                Ok(analysis) => archived_infos.push(analysis),
                Err(e) => return Err(anyhow::anyhow!("Failed to analyze archived bucket: {}", e)),
            }
        }

        // Compute the sum of all archived character counts
        let mut total_archived_counts = DetailedCharacterCount::new();
        for archived_info in &archived_infos {
            total_archived_counts.add(&archived_info.detailed_character_count);
        }

        // Compare character counts
        let (detailed_ok, differences) =
            total_archived_counts.compare(&consolidated_info.detailed_character_count);

        // Generate result
        let end_time = Utc::now();

        let result = if detailed_ok {
            info!("No size difference for {}/{}", date, hour);
            ComparisonResult {
                date: date.clone(),
                hour: hour.clone(),
                ok: true,
                message: "Consolidated size matches original size".to_string(),
                consolidated_data: consolidated_info,
                archived_data: archived_infos,
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
                consolidated_data: consolidated_info,
                archived_data: archived_infos,
                analysis_start_date: start_time.to_rfc3339(),
                analysis_end_date: end_time.to_rfc3339(),
            }
        };

        Ok(result)
    }
}