// src/s3/checker.rs
// Location: src/s3/checker.rs

use crate::config::types::{BucketConfig, DateString, HourString, S3FileList, S3ObjectInfo};
use crate::s3::client::WrappedS3Client;
use crate::s3::downloader::DownloadOrchestrator;
use crate::utils::character_counter::DetailedCharacterCount;
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::signal_handler::{MemoryMonitor, ProgressTracker};
use crate::utils::structured_log::{
    BucketCharacterStats, BucketInfo, BucketRole, BucketsConfig, CharacterStats, CheckResult,
    FileStats, LogEntry,
};
use anyhow::Result;
use chrono::Utc;
use crc32fast::Hasher;
use env_logger::Logger;
use log::{error, info, warn};
use rand::prelude::SliceRandom;
use rand::rng;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

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
    /// Detailed character counts from archived buckets
    pub archived_counts: DetailedCharacterCount,
    /// Detailed character counts from consolidated bucket
    pub consolidated_counts: DetailedCharacterCount,
    /// Differences between archived and consolidated (byte index -> difference)
    pub differences: HashMap<usize, i64>,
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
    pub fn get_memory_monitor(&self, target_date: &str, target_hour: &str) -> Option<MemoryMonitor> {
        Some(MemoryMonitor::with_progress(
            Arc::clone(&self.memory_allocator),
            self.progress_tracker.clone(),
            target_date,
            target_hour,
        ))
    }

    pub async fn list_bucket_files(
        &self,
        bucket_config: &BucketConfig,
        date: &DateString,
        hour: &HourString,
        role: BucketRole,
    ) -> Result<S3FileList> {
        let client = self.s3_client.get_client().await?;
        self.list_bucket_files_with_client(&client, bucket_config, date, hour)
            .await
    }

    /// List bucket files using a provided client - reduces DNS lookups when
    /// listing multiple buckets
    pub async fn list_bucket_files_with_client(
        &self,
        client: &aws_sdk_s3::Client,
        bucket_config: &BucketConfig,
        date: &DateString,
        hour: &HourString,
    ) -> Result<S3FileList> {
        let bucket = &bucket_config.bucket;
        let key_prefix = {
            let formatter = crate::utils::path_formatter::generate_path_formatter(bucket_config);
            formatter(date, hour)?
        };

        let role_str = match role {
            BucketRole::Archived => "archived",
            BucketRole::Consolidated => "consolidated",
        };

        info!(
            target_date = %date,
            target_hour = %hour,
            bucket = %bucket,
            key_prefix = %key_prefix,
            role = %role_str,
            "Listing files in bucket"
        );

        let file_list = self
            .s3_client
            .get_matching_filenames_from_s3_with_client(client, bucket_config, date, hour, true)
            .await?;

        LogEntry::info(format!(
            "Bucket {} ({}): {} files, {} total, checksum {}",
            bucket,
            role_str,
            file_list.files.len(),
            human_bytes::human_bytes(file_list.total_archives_size as f64),
            &file_list.checksum[..8]
        ))
        .with_target("log_consolidator_checker_rust::s3::checker")
        .with_date_hour(date, hour)
        .with_bucket(
            BucketInfo::new(bucket, role).with_patterns(bucket_config.only_prefix_patterns.clone()),
        )
        .with_file_stats(
            FileStats::new(file_list.files.len(), file_list.total_archives_size)
                .with_checksum(file_list.checksum.clone()),
        )
        .emit();

        Ok(file_list)
    }

    /// Analyze a bucket for a specific date and hour
    pub async fn download_and_analyze_files(
        &self,
        objects: &[S3ObjectInfo],
    ) -> Result<HashMap<String, DetailedCharacterCount>> {
        let client = self.s3_client.get_client().await?;
        self.download_and_analyze_files_with_client(client, objects)
            .await
    }

    /// Analyze files using a provided client - reduces DNS lookups when reusing client
    pub async fn download_and_analyze_files_with_client(
        &self,
        client: aws_sdk_s3::Client,
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

        // Log the buckets configuration at the start
        let archived_bucket_infos: Vec<BucketInfo> = archived_bucket_configs
            .iter()
            .map(|b| BucketInfo::archived(&b.bucket).with_patterns(b.only_prefix_patterns.clone()))
            .collect();

        let consolidated_bucket_info = BucketInfo::consolidated(&consolidated_bucket_config.bucket)
            .with_patterns(consolidated_bucket_config.only_prefix_patterns.clone());

        let archived_names: Vec<&str> = archived_bucket_configs
            .iter()
            .map(|b| b.bucket.as_str())
            .collect();

        LogEntry::info(format!(
            "Starting check for {}/{}: {} archived buckets [{}] vs consolidated [{}]",
            date,
            hour,
            archived_bucket_configs.len(),
            archived_names.join(", "),
            consolidated_bucket_config.bucket
        ))
        .with_target("log_consolidator_checker_rust::s3::checker")
        .with_date_hour(date, hour)
        .with_buckets(BucketsConfig {
            archived: archived_bucket_infos,
            consolidated: consolidated_bucket_info,
            archived_count: archived_bucket_configs.len(),
            total_count: archived_bucket_configs.len() + 1,
        })
        .emit();

        // Get client once upfront for all listing operations to reduce DNS lookups
        let client = self.s3_client.get_client().await?;

        // Start listing files
        let mut file_lists_futures = Vec::new();
        file_lists_futures.push(self.list_bucket_files(
            consolidated_bucket_config,
            date,
            hour,
            BucketRole::Consolidated,
        ));
        for &archived_bucket_config in archived_bucket_configs {
            let future =
                self.list_bucket_files(archived_bucket_config, date, hour, BucketRole::Archived);
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

        // Reuse the same client for downloads to avoid additional DNS lookups
        let character_counts_by_bucket = self
            .download_and_analyze_files_with_client(client, &file_lists[..])
            .await?;

        // Compute the sum of all archived character counts and collect per-bucket stats
        let mut total_archived_counts = DetailedCharacterCount::new();
        let mut per_bucket_stats: Vec<BucketCharacterStats> = Vec::new();

        for archived_info in archived_bucket_configs {
            let bucket_character_count = character_counts_by_bucket
                .get(&archived_info.bucket)
                .unwrap();
            let total_chars = bucket_character_count.total_excluding_newlines();
            per_bucket_stats.push(BucketCharacterStats::archived(
                &archived_info.bucket,
                total_chars,
            ));
            total_archived_counts.add(bucket_character_count);
        }

        // Find the consolidated character count
        let consolidated_character_count =
            match character_counts_by_bucket.get(&consolidated_bucket_config.bucket) {
                Some(ccc) => ccc,
                None => {
                    error!(
                        "missing character count for consolidated bucket: {}",
                        consolidated_bucket_config.bucket
                    );
                    &DetailedCharacterCount::new()
                }
            };

        let consolidated_total = consolidated_character_count.total_excluding_newlines();
        per_bucket_stats.push(BucketCharacterStats::consolidated(
            &consolidated_bucket_config.bucket,
            consolidated_total,
        ));

        let archived_total = total_archived_counts.total_excluding_newlines();

        // Compare character counts
        let (detailed_ok, differences) =
            total_archived_counts.compare(consolidated_character_count);

        // Generate result
        let end_time = Utc::now();
        let duration = end_time - start_time;

        let result = if detailed_ok {
            // Log successful character stats
            LogEntry::info(format!(
                "✅ Check {}/{} PASSED: {} archived chars == {} consolidated chars (took {}s)",
                date,
                hour,
                human_bytes::human_bytes(archived_total as f64),
                human_bytes::human_bytes(consolidated_total as f64),
                duration.num_seconds()
            ))
            .with_target("log_consolidator_checker_rust::s3::checker")
            .with_date_hour(date, hour)
            .with_check_result(CheckResult::Passed)
            .with_character_stats(
                CharacterStats::matched(archived_total, consolidated_total)
                    .with_per_bucket(per_bucket_stats)
                    .with_duration(duration.num_seconds()),
            )
            .emit();

            ComparisonResult {
                date: date.clone(),
                hour: hour.clone(),
                ok: true,
                message: "Consolidated size matches original size".to_string(),
                analysis_start_date: start_time.to_rfc3339(),
                analysis_end_date: end_time.to_rfc3339(),
                archived_counts: total_archived_counts.clone(),
                consolidated_counts: consolidated_character_count.clone(),
                differences,
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

            // Log failed character stats
            LogEntry::error(format!(
                "❌ Check {}/{} FAILED: {} archived vs {} consolidated, {} char types differ by {} total (hash: {})",
                date,
                hour,
                human_bytes::human_bytes(archived_total as f64),
                human_bytes::human_bytes(consolidated_total as f64),
                differences.len(),
                difference_total,
                &difference_hash[..8]
            ))
            .with_target("log_consolidator_checker_rust::s3::checker")
            .with_date_hour(date, hour)
            .with_check_result(CheckResult::Failed)
            .with_character_stats(
                CharacterStats::mismatched(
                    archived_total,
                    consolidated_total,
                    differences.len(),
                    difference_total,
                    difference_hash,
                )
                .with_per_bucket(per_bucket_stats)
                .with_duration(duration.num_seconds()),
            )
            .emit();

            ComparisonResult {
                date: date.clone(),
                hour: hour.clone(),
                ok: false,
                message: format!(
                    "Difference in character count: {} types differ by {} chars",
                    differences.len(),
                    difference_total
                ),
                analysis_start_date: start_time.to_rfc3339(),
                analysis_end_date: end_time.to_rfc3339(),
                archived_counts: total_archived_counts.clone(),
                consolidated_counts: consolidated_character_count.clone(),
                differences,
            }
        };

        Ok(result)
    }
}
