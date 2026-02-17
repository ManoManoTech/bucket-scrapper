// src/s3/streaming_search_executor.rs
use crate::config::types::S3ObjectInfo;
use crate::search::{StreamSearcher, StreamingSearchCollector};
use anyhow::Result;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use aws_sdk_s3::Client;
use backon::{ExponentialBuilder, Retryable};
use std::io::{self, Read};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, BufReader};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info, warn};

/// A wrapper that counts bytes read from an underlying reader
struct CountingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicUsize>,
}

impl<R: Read> CountingReader<R> {
    fn new(inner: R) -> (Self, Arc<AtomicUsize>) {
        let bytes_read = Arc::new(AtomicUsize::new(0));
        (
            Self {
                inner,
                bytes_read: bytes_read.clone(),
            },
            bytes_read,
        )
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}

/// A minimal collector wrapper for use in blocking context
pub struct SimpleStreamCollector {
    bucket: String,
    key: String,
    matches: Vec<String>,
    local_count: u64,
}

impl SimpleStreamCollector {
    pub fn new(bucket: String, key: String) -> Self {
        Self {
            bucket,
            key,
            matches: Vec::new(),
            local_count: 0,
        }
    }

    pub fn add_match(&mut self, _line_num: u64, line: &str) {
        self.local_count += 1;
        self.matches.push(line.to_string());
    }

    pub fn add_count(&mut self, _bucket: &str, _key: &str, count: u64) {
        self.local_count = count;
    }

    pub fn mark_file_searched(&mut self) {
        // Just a marker, actual tracking happens later
    }

    pub fn match_count(&self) -> usize {
        self.local_count as usize
    }

    pub fn into_matches(self) -> Vec<String> {
        self.matches
    }
}

/// Configuration for the streaming search executor
#[derive(Clone)]
pub struct StreamingSearchConfig {
    pub max_concurrent_downloads: usize,
    pub buffer_size_bytes: usize,
    pub channel_buffer_size: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
}

impl Default for StreamingSearchConfig {
    fn default() -> Self {
        Self {
            max_concurrent_downloads: 32,
            buffer_size_bytes: 64 * 1024, // 64KB chunks
            channel_buffer_size: 100,
            max_retries: 10,
            initial_retry_delay: Duration::from_secs(2),
        }
    }
}

/// Executes searches on S3 objects with streaming output
pub struct StreamingSearchExecutor {
    client: Client,
    config: StreamingSearchConfig,
    download_semaphore: Arc<Semaphore>,
}

impl StreamingSearchExecutor {
    pub fn new(client: Client, config: StreamingSearchConfig) -> Self {
        let download_semaphore = Arc::new(Semaphore::new(config.max_concurrent_downloads));

        Self {
            client,
            config,
            download_semaphore,
        }
    }

    /// Process a batch of S3 objects with streaming output
    pub async fn search_objects_streaming(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        collector: Arc<StreamingSearchCollector>,
    ) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }

        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        let start_time = std::time::Instant::now();

        info!(
            objects = objects.len(),
            mb = total_bytes / 1_000_000,
            "Starting streaming search"
        );

        let (tx, mut rx) = mpsc::channel::<Result<()>>(self.config.channel_buffer_size);

        // Spawn tasks for each object
        let mut handles = Vec::new();
        for obj in objects {
            let obj_clone = obj.clone();
            let client = self.client.clone();
            let searcher = searcher.clone();
            let collector = collector.clone();
            let tx = tx.clone();
            let semaphore = self.download_semaphore.clone();
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await?;

                let result = Self::download_and_search_streaming(
                    client, obj_clone, searcher, collector, config,
                )
                .await;

                tx.send(result).await.ok();
                Ok::<(), anyhow::Error>(())
            });

            handles.push(handle);
        }

        // Drop original sender to allow channel to close
        drop(tx);

        // Collect results
        let mut errors = Vec::new();
        while let Some(result) = rx.recv().await {
            if let Err(e) = result {
                errors.push(e.to_string());
            }
        }

        // Wait for all tasks
        for handle in handles {
            if let Err(e) = handle.await {
                errors.push(format!("Task panic: {}", e));
            }
        }

        let elapsed = start_time.elapsed();
        let (files_searched, files_with_matches, total_matches) = collector.get_stats().await;

        info!(
            elapsed_s = elapsed.as_secs_f32(),
            files = files_searched,
            files_with_matches = files_with_matches,
            matches = total_matches,
            "Streaming search complete"
        );

        if !errors.is_empty() {
            return Err(anyhow::anyhow!(
                "{} downloads failed: {}",
                errors.len(),
                errors.first().unwrap_or(&"unknown".to_string())
            ));
        }

        Ok(())
    }

    /// Download and search a single object with streaming output
    async fn download_and_search_streaming(
        client: Client,
        obj: S3ObjectInfo,
        searcher: Arc<StreamSearcher>,
        collector: Arc<StreamingSearchCollector>,
        config: StreamingSearchConfig,
    ) -> Result<()> {
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();

        let inner = || async {
            Self::download_and_search_streaming_inner(
                &client,
                &obj,
                &searcher,
                &collector,
                config.buffer_size_bytes,
            )
            .await
        };

        let retry_params = ExponentialBuilder::default()
            .with_min_delay(config.initial_retry_delay)
            .with_max_delay(Duration::from_secs(60))
            .with_factor(2.0)
            .with_jitter()
            .with_max_times(config.max_retries as usize);

        inner
            .retry(retry_params)
            .sleep(tokio::time::sleep)
            .notify(move |err: &anyhow::Error, dur: Duration| {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    retry_in_s = dur.as_secs_f64(),
                    error = %err,
                    "Retry scheduled"
                );
            })
            .await
    }

    /// Inner download and search function with streaming
    async fn download_and_search_streaming_inner(
        client: &Client,
        obj: &S3ObjectInfo,
        searcher: &Arc<StreamSearcher>,
        collector: &Arc<StreamingSearchCollector>,
        buffer_size: usize,
    ) -> Result<()> {
        debug!(bucket = %obj.bucket, key = %obj.key, "Starting streaming search");

        // Mark file as being searched
        collector.mark_file_searched().await?;

        // Get object from S3
        let resp = client
            .get_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get S3 object: {}", e))?;

        // Convert to async reader
        let stream = resp.body.into_async_read();
        let buffered = BufReader::with_capacity(buffer_size, stream);

        // Apply decompression based on file extension
        let decompressed: Pin<Box<dyn AsyncRead + Send>> = if obj.key.ends_with(".gz") {
            Box::pin(GzipDecoder::new(buffered))
        } else if obj.key.ends_with(".zst") || obj.key.ends_with(".zstd") {
            Box::pin(ZstdDecoder::new(buffered))
        } else {
            Box::pin(buffered)
        };

        // Clone necessary data for the blocking task
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();
        let searcher_clone = searcher.clone();
        let collector_clone = collector.clone();

        // Use spawn_blocking for the actual search
        let matches = tokio::task::spawn_blocking(move || {
            // Create a sync bridge from the async reader
            let sync_reader = SyncIoBridge::new(decompressed);
            let buffered_sync = std::io::BufReader::with_capacity(buffer_size, sync_reader);

            // Create a simple collector wrapper
            let local_collector = SimpleStreamCollector::new(bucket.clone(), key.clone());

            // Use the compatibility layer for SearchResultCollector
            use crate::search::SearchResultCollector;
            let mut compat_collector = SearchResultCollector::new();

            // Search the stream
            searcher_clone.search_stream(&bucket, &key, buffered_sync, &mut compat_collector)?;

            // Extract matches
            let result = compat_collector.into_result();
            Ok::<Vec<_>, anyhow::Error>(result.matches)
        })
        .await??;

        // Now stream the matches
        for match_item in matches {
            collector
                .add_match(
                    &match_item.bucket,
                    &match_item.key,
                    &match_item.line_content,
                )
                .await?;
        }

        debug!(bucket = %obj.bucket, key = %obj.key, "Completed streaming search");
        Ok(())
    }
}
