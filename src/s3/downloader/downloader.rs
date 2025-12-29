// src/s3/downloader/s3_fetcher.rs
use crate::config::types::S3ObjectInfo;
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::signal_handler::ProgressTracker;
use crate::utils::structured_log::{LogEntry, RetryInfo, S3OperationInfo};
use anyhow::{anyhow, Result};
use aws_sdk_s3::Client;
use backon::ExponentialBuilder;
use backon::Retryable;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, warn};

use super::types::{CompressionType, RawObjectData};

/// Max retry attempts per download
const MAX_RETRY_ATTEMPTS: u32 = 10;

pub struct Downloader {
    client: Client,
    memory_allocator: Arc<MemoryLimitedAllocator>,
    download_semaphore: Arc<Semaphore>,
    target_date: Option<String>,
    target_hour: Option<String>,
}

impl Downloader {
    pub fn new(
        client: Client,
        max_concurrent_downloads: usize,
        memory_allocator: Arc<MemoryLimitedAllocator>,
    ) -> Self {
        let download_semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));

        Self {
            client,
            memory_allocator,
            download_semaphore,
            target_date: None,
            target_hour: None,
        }
    }

    /// Create a downloader with date/hour context for retry logging
    pub fn with_context(
        client: Client,
        max_concurrent_downloads: usize,
        memory_allocator: Arc<MemoryLimitedAllocator>,
        date: &str,
        hour: &str,
    ) -> Self {
        let download_semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));

        Self {
            client,
            memory_allocator,
            download_semaphore,
            target_date: Some(date.to_string()),
            target_hour: Some(hour.to_string()),
        }
    }

    /// Concurrently fetch multiple objects from S3
    pub async fn fetch_objects(
        &self,
        objects: &[S3ObjectInfo],
        tx: mpsc::Sender<RawObjectData>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Result<()> {
        // Log initial memory allocation stats
        info!(
            "Starting downloads. Memory pool size: {} bytes",
            self.memory_allocator.stats().1
        );

        // Handle empty objects list gracefully
        if objects.is_empty() {
            // Deliberately drop tx to signal end of data
            drop(tx);
            return Err(anyhow!("No objects to fetch."));
        }

        // Start downloading files concurrently
        let mut download_handles = Vec::new();
        for obj in objects {
            let permit = self.download_semaphore.clone().acquire_owned().await?;
            let obj_clone = obj.clone();
            let bucket_str = obj.bucket.to_string();
            let client_clone = self.client.clone();
            let tx_clone = tx.clone();
            let allocator = Arc::clone(&self.memory_allocator);
            let progress_tracker = progress_tracker.clone();
            let target_date = self.target_date.clone();
            let target_hour = self.target_hour.clone();

            let handle = tokio::spawn(async move {
                let inner_result = async || {
                    Self::download_object(
                        &client_clone,
                        &bucket_str,
                        &obj_clone,
                        tx_clone.clone(),
                        Arc::clone(&allocator),
                        progress_tracker.clone(),
                    )
                    .await
                    .map_err(|e| {
                        let err_msg = e.to_string();
                        if err_msg.contains("dispatch failure") {
                            anyhow::anyhow!(
                                "S3 download failed for {}/{}: {}. \
                                 This often indicates expired AWS credentials - try 'aws sso login'",
                                bucket_str, obj_clone.key, err_msg
                            )
                        } else {
                            anyhow::anyhow!(
                                "download error {}/{}: '{}'",
                                bucket_str, obj_clone.key, err_msg
                            )
                        }
                    })
                };

                let retry_params = ExponentialBuilder::default()
                    .with_min_delay(Duration::from_secs(2))
                    .with_max_delay(Duration::from_secs(60))
                    .with_factor(2.0)
                    .with_jitter()
                    .with_max_times(MAX_RETRY_ATTEMPTS as usize);

                let mut attempt: u32 = 1;
                let bucket_for_log = bucket_str.clone();
                let key_for_log = obj_clone.key.clone();
                let size_for_log = obj_clone.size;

                // Only log retries after N failures and every N failures to reduce spam
                const LOG_RETRY_AFTER: u32 = 3;
                const LOG_RETRY_EVERY: u32 = 3;

                let result = inner_result
                    .retry(retry_params)
                    .sleep(tokio::time::sleep)
                    .notify(move |err: &anyhow::Error, dur: Duration| {
                        attempt += 1;

                        // Only log after LOG_RETRY_AFTER attempts and every LOG_RETRY_EVERY attempts
                        if attempt < LOG_RETRY_AFTER
                            || (attempt - LOG_RETRY_AFTER) % LOG_RETRY_EVERY != 0
                        {
                            return;
                        }

                        let delay_secs = dur.as_secs_f64();

                        let mut log_entry = LogEntry::warn("S3 download retry scheduled")
                            .with_target("s3::downloader")
                            .with_s3_operation(
                                S3OperationInfo::download(&bucket_for_log, &key_for_log)
                                    .with_size(size_for_log),
                            )
                            .with_retry(
                                RetryInfo::new(attempt, MAX_RETRY_ATTEMPTS, delay_secs)
                                    .with_error("download_error", err.to_string())
                                    .with_retriable(true),
                            );

                        // Add date/hour context if available
                        if let (Some(date), Some(hour)) = (&target_date, &target_hour) {
                            log_entry = log_entry.with_date_hour(date, hour);
                        }

                        log_entry.emit();
                    })
                    .await;

                // Release permit when done, regardless of error status
                drop(permit);

                result
            });

            download_handles.push(handle);
        }

        // Wait for all downloads to complete
        let results = join_all(download_handles).await;
        let mut failed_downloads: Vec<String> = Vec::new();

        for result in results {
            match result {
                Ok(Ok(())) => {} // Task succeeded
                Ok(Err(e)) => {
                    // Download failed after max retries
                    failed_downloads.push(e.to_string());
                }
                Err(e) => {
                    // Task panicked
                    failed_downloads.push(format!("task panic: {}", e));
                }
            }
        }

        // Close the channel to signal end of downloads
        // Explicitly drop tx here to close the channel
        drop(tx);

        // Fail if any downloads failed after max retries
        if !failed_downloads.is_empty() {
            return Err(anyhow!(
                "{} download(s) failed after max retries: {}",
                failed_downloads.len(),
                failed_downloads.first().unwrap_or(&"unknown".to_string())
            ));
        }

        debug!("All downloads completed.");
        Ok(())
    }

    // Downloads a single object and sends the data to the processing channel
    async fn download_object(
        client: &Client,
        bucket: &str,
        obj: &S3ObjectInfo,
        tx: mpsc::Sender<RawObjectData>,
        allocator: Arc<MemoryLimitedAllocator>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Result<()> {
        debug!("Downloading {} (size: {} bytes)", obj.key, obj.size);

        // Determine compression type
        let compression_type = if obj.key.ends_with(".gz") {
            CompressionType::Gzip
        } else if obj.key.ends_with(".zst") {
            CompressionType::Zstd
        } else {
            CompressionType::None
        };

        // Estimate buffer size needed (protect against 0 size)
        let estimated_size = obj.size.max(1024); // Minimum 1KB

        // Allocate a buffer from our memory-limited pool
        let mut buffer = allocator.alloc_vec(estimated_size).await;

        // Get the object from S3
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(&obj.key)
            .send()
            .await
            .map_err(|e| {
                let err_msg = e.to_string();
                if err_msg.contains("dispatch failure") {
                    anyhow::anyhow!(
                        "S3 request failed for {}: {}. \
                         This often indicates expired AWS credentials - try 'aws sso login'",
                        obj.key,
                        err_msg
                    )
                } else {
                    anyhow::anyhow!("Failed to download S3 object {}: {}", obj.key, err_msg)
                }
            })?;

        // Read the stream into our memory-limited buffer
        let byte_stream = resp.body;
        let async_read = byte_stream.into_async_read();
        let mut buf_reader = BufReader::new(async_read);

        // Reserve capacity in our buffer
        buffer.reserve(estimated_size);

        // Read the data
        match buf_reader.read_to_end(buffer.as_vec_mut()).await {
            Ok(bytes_read) => {
                debug!("Downloaded {} bytes for {}", bytes_read, obj.key);

                // Update progress tracker for download completion
                progress_tracker.increment_processed(bytes_read);

                // Send data for processing
                match tx
                    .send(RawObjectData {
                        bucket: bucket.to_string(),
                        key: obj.key.clone(),
                        data: buffer,
                        compression_type,
                    })
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!("Failed to send data for processing (channel closed): {}", e);
                        Err(anyhow::anyhow!(
                            "Channel closed, receiver likely dropped: {}",
                            e
                        ))
                    }
                }
            }
            Err(e) => Err(anyhow::anyhow!(
                "Failed to read data from S3 stream for {}: {}",
                obj.key,
                e
            )),
        }
    }
}
