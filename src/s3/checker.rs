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
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

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
    /// Number of archived files checked
    pub archived_files_count: usize,
    /// Number of consolidated files checked
    pub consolidated_files_count: usize,
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
    pub fn get_memory_monitor(
        &self,
        target_date: &str,
        target_hour: &str,
    ) -> Option<MemoryMonitor> {
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
        self.list_bucket_files_with_client(&client, bucket_config, date, hour, role)
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
        role: BucketRole,
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
        self.download_and_analyze_files_with_client(client, objects, None, None)
            .await
    }

    /// Analyze files using a provided client - reduces DNS lookups when reusing client
    pub async fn download_and_analyze_files_with_client(
        &self,
        client: aws_sdk_s3::Client,
        objects: &[S3ObjectInfo],
        date: Option<&str>,
        hour: Option<&str>,
    ) -> Result<HashMap<String, DetailedCharacterCount>> {
        let downloader = match (date, hour) {
            (Some(d), Some(h)) => DownloadOrchestrator::with_context(
                client,
                self.max_parallel,
                self.max_process_threads,
                Arc::clone(&self.memory_allocator),
                Arc::clone(&self.progress_tracker),
                d,
                h,
            ),
            _ => DownloadOrchestrator::new(
                client,
                self.max_parallel,
                self.max_process_threads,
                Arc::clone(&self.memory_allocator),
                Arc::clone(&self.progress_tracker),
            ),
        };

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

        // Check for duplicate bucket names (consolidated bucket in archived list would cause double-counting)
        let consolidated_name = &consolidated_bucket_config.bucket;
        for archived_config in archived_bucket_configs {
            if &archived_config.bucket == consolidated_name {
                error!(
                    consolidated_bucket = %consolidated_name,
                    "BUG DETECTED: Consolidated bucket is also in archived bucket list! This causes double-counting."
                );
                return Err(anyhow::anyhow!(
                    "Configuration error: consolidated bucket '{}' is also listed in bucketsToConsolidate. \
                     This would cause files to be counted twice, leading to incorrect results.",
                    consolidated_name
                ));
            }
        }

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
        // This avoids repeated get_client() calls which acquire RwLock and may trigger DNS
        let client = self.s3_client.get_client().await?;

        // Start listing files - clone client for each concurrent future to avoid borrow issues
        let mut file_lists_futures = Vec::new();
        file_lists_futures.push(self.list_bucket_files_with_client(
            &client,
            consolidated_bucket_config,
            date,
            hour,
            BucketRole::Consolidated,
        ));
        for &archived_bucket_config in archived_bucket_configs {
            let future = self.list_bucket_files_with_client(
                &client,
                archived_bucket_config,
                date,
                hour,
                BucketRole::Archived,
            );
            file_lists_futures.push(future);
        }

        // Sort it all in one big filelist
        let mut file_lists = Vec::new();
        let mut archived_files_count = 0usize;
        let mut consolidated_files_count = 0usize;
        {
            let results = futures::future::join_all(file_lists_futures).await;
            // First result is consolidated, rest are archived
            for (idx, result) in results.into_iter().enumerate() {
                let is_consolidated = idx == 0;
                match result {
                    Ok(mut analysis) => {
                        let files_count = analysis.files.len();

                        if is_consolidated {
                            consolidated_files_count = files_count;
                        } else {
                            archived_files_count += files_count;
                        }

                        info!(
                            bucket = %analysis.bucket,
                            files_count = files_count,
                            total_size = analysis.total_archives_size,
                            bucket_type = if is_consolidated { "consolidated" } else { "archived" },
                            "Adding files from bucket to processing queue"
                        );
                        file_lists.append(analysis.files.as_mut());
                    }
                    Err(e) => {
                        if is_consolidated {
                            // Consolidated bucket failure is critical
                            return Err(anyhow::anyhow!("Failed to list consolidated bucket: {}", e));
                        } else {
                            // Archived bucket failure - skip with warning
                            // Some buckets may not exist or be inaccessible in certain environments
                            let bucket_name = archived_bucket_configs
                                .get(idx - 1)
                                .map(|b| b.bucket.as_str())
                                .unwrap_or("unknown");
                            warn!(
                                bucket = %bucket_name,
                                error_message = %e,
                                error_debug = ?e,
                                "Failed to list archived bucket, skipping"
                            );
                        }
                    }
                }
            }

            // Log total files to be processed
            info!(
                total_files = file_lists.len(),
                archived_files = archived_files_count,
                consolidated_files = consolidated_files_count,
                "Total files to download and analyze"
            );

            // Handle case where there are no archived files (inputs)
            if archived_files_count == 0 {
                let end_time = Utc::now();

                // If there are no input files, there's nothing to consolidate - this is a valid state
                if consolidated_files_count == 0 {
                    info!(
                        target_date = %date,
                        target_hour = %hour,
                        "No files in archived buckets and no consolidated files - nothing to check"
                    );
                    return Ok(ComparisonResult {
                        ok: true,
                        date: date.clone(),
                        hour: hour.clone(),
                        message: format!(
                            "No files to consolidate for {}/{}. Both archived and consolidated buckets are empty.",
                            date, hour
                        ),
                        analysis_start_date: start_time.to_rfc3339(),
                        analysis_end_date: end_time.to_rfc3339(),
                        archived_files_count: 0,
                        consolidated_files_count: 0,
                        archived_counts: DetailedCharacterCount::new(),
                        consolidated_counts: DetailedCharacterCount::new(),
                        differences: HashMap::new(),
                    });
                } else {
                    // Consolidated files exist but no input files - input files were cleaned up after consolidation
                    info!(
                        target_date = %date,
                        target_hour = %hour,
                        consolidated_files = consolidated_files_count,
                        "No archived files but consolidated files exist - input files cleaned up after consolidation"
                    );
                    return Ok(ComparisonResult {
                        ok: true,
                        date: date.clone(),
                        hour: hour.clone(),
                        message: format!(
                            "Cleaned: No input files for {}/{} but {} consolidated files exist. \
                             Input files were cleaned up after successful consolidation.",
                            date, hour, consolidated_files_count
                        ),
                        analysis_start_date: start_time.to_rfc3339(),
                        analysis_end_date: end_time.to_rfc3339(),
                        archived_files_count: 0,
                        consolidated_files_count,
                        archived_counts: DetailedCharacterCount::new(),
                        consolidated_counts: DetailedCharacterCount::new(),
                        differences: HashMap::new(),
                    });
                }
            }

            // Handle case where there are no consolidated files (outputs) - needs consolidation
            if consolidated_files_count == 0 {
                let end_time = Utc::now();
                warn!(
                    target_date = %date,
                    target_hour = %hour,
                    archived_files = archived_files_count,
                    "No consolidated files found - consolidation has not been performed yet"
                );
                return Ok(ComparisonResult {
                    ok: false,
                    date: date.clone(),
                    hour: hour.clone(),
                    message: format!(
                        "Consolidation pending for {}/{}. Found {} archived files but no consolidated output yet.",
                        date, hour, archived_files_count
                    ),
                    analysis_start_date: start_time.to_rfc3339(),
                    analysis_end_date: end_time.to_rfc3339(),
                    archived_files_count,
                    consolidated_files_count: 0,
                    archived_counts: DetailedCharacterCount::new(),
                    consolidated_counts: DetailedCharacterCount::new(),
                    differences: HashMap::new(),
                });
            }

            // let mut rng = rng();
            // file_lists.shuffle(&mut rng);
            file_lists.sort_by(|a, b| b.size.cmp(&a.size));
        }

        // Reuse the same client for downloads to avoid additional DNS lookups
        let character_counts_by_bucket = self
            .download_and_analyze_files_with_client(
                client,
                &file_lists[..],
                Some(date.as_str()),
                Some(hour.as_str()),
            )
            .await?;

        // Compute the sum of all archived character counts and collect per-bucket stats
        let mut total_archived_counts = DetailedCharacterCount::new();
        let mut per_bucket_stats: Vec<BucketCharacterStats> = Vec::new();
        let mut missing_buckets: Vec<String> = Vec::new();

        for archived_info in archived_bucket_configs {
            match character_counts_by_bucket.get(&archived_info.bucket) {
                Some(bucket_character_count) => {
                    let total_chars = bucket_character_count.total_excluding_newlines();
                    per_bucket_stats.push(BucketCharacterStats::archived(
                        &archived_info.bucket,
                        total_chars,
                    ));
                    total_archived_counts.add(bucket_character_count);
                }
                None => {
                    // Bucket had no successfully processed files (all downloads failed)
                    warn!(
                        bucket = %archived_info.bucket,
                        "No character counts for archived bucket - all downloads may have failed"
                    );
                    missing_buckets.push(archived_info.bucket.clone());
                    // Add with zero count so it appears in stats
                    per_bucket_stats.push(BucketCharacterStats::archived(&archived_info.bucket, 0));
                }
            }
        }

        // If any buckets are completely missing, this is a serious error
        if !missing_buckets.is_empty() {
            error!(
                missing_buckets = ?missing_buckets,
                "Some archived buckets have no data - check for download failures"
            );
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
                archived_files_count,
                consolidated_files_count,
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
            let difference_hash = format!("{:08x}", hasher.finalize());
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
                archived_files_count,
                consolidated_files_count,
                archived_counts: total_archived_counts.clone(),
                consolidated_counts: consolidated_character_count.clone(),
                differences,
            }
        };

        Ok(result)
    }

    /// Upload check result to S3 results bucket
    /// Creates two files in `{path_prefix}/`:
    /// 1. `{timestamp_ms}.json` - historical record with timestamp
    /// 2. `check_result.json` - latest result (for easy access)
    pub async fn upload_check_result(
        &self,
        results_bucket: &BucketConfig,
        date: &DateString,
        hour: &HourString,
        result: &ComparisonResult,
    ) -> Result<()> {
        use crate::utils::path_formatter::generate_path_formatter;

        // Generate the path using the bucket's path configuration
        // e.g., "dt=20250102/hour=15"
        let formatter = generate_path_formatter(results_bucket);
        let path_prefix = formatter(date, hour)?;

        // Create a JSON structure for the check result
        let check_result_json = serde_json::json!({
            "date": date,
            "hour": hour,
            "ok": result.ok,
            "message": result.message,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "archived_files_count": result.archived_files_count,
            "consolidated_files_count": result.consolidated_files_count,
            "archived_total_chars": result.archived_counts.total_excluding_newlines(),
            "consolidated_total_chars": result.consolidated_counts.total_excluding_newlines(),
            "differences_count": result.differences.len(),
        });

        let json_bytes = serde_json::to_vec_pretty(&check_result_json)?;

        // Get current timestamp in milliseconds
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        // Upload with timestamp for historical record
        let timestamped_key = format!("{}/{}.json", path_prefix, timestamp_ms);
        self.s3_client
            .upload_object(&results_bucket.bucket, &timestamped_key, json_bytes.clone())
            .await?;

        // Upload as check_result.json for easy access to latest
        let latest_key = format!("{}/check_result.json", path_prefix);
        self.s3_client
            .upload_object(&results_bucket.bucket, &latest_key, json_bytes)
            .await?;

        info!(
            timestamped_key = %timestamped_key,
            latest_key = %latest_key,
            "Uploaded check results to S3"
        );

        Ok(())
    }
}
