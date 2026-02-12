// src/s3/streaming_downloader.rs
use crate::config::types::S3ObjectInfo;
use crate::search::{HttpMatchToSend, HttpStreamingCollector, SearchCollector, SearchResultCollector, StreamSearcher};
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
use tokio::sync::{mpsc, Mutex, Semaphore};
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

/// Progress tracking for search operations
struct SearchProgress {
    total_files: usize,
    files_processed: usize,
    total_bytes: usize,
    bytes_processed: usize,
    matches_found: usize,
    start_time: std::time::Instant,
    last_report_time: std::time::Instant,
}

impl SearchProgress {
    fn new(total_files: usize, total_bytes: usize) -> Self {
        let now = std::time::Instant::now();
        Self {
            total_files,
            files_processed: 0,
            total_bytes,
            bytes_processed: 0,
            matches_found: 0,
            start_time: now,
            last_report_time: now,
        }
    }

    fn update(&mut self, bytes: usize, matches: usize) {
        self.files_processed += 1;
        self.bytes_processed += bytes;
        self.matches_found += matches;
    }

    fn should_report(&self) -> bool {
        // Report every 30 seconds
        self.last_report_time.elapsed() > Duration::from_secs(30)
    }

    fn report(&mut self) {
        let pct = (self.files_processed * 100) / self.total_files.max(1);
        let mb_processed = self.bytes_processed / 1_000_000;
        let mb_total = self.total_bytes / 1_000_000;

        info!(
            "[{}/{} files ({}%), {}MB/{}MB] {} matches found, elapsed: {:.1}s",
            self.files_processed,
            self.total_files,
            pct,
            mb_processed,
            mb_total,
            self.matches_found,
            self.start_time.elapsed().as_secs_f32()
        );

        self.last_report_time = std::time::Instant::now();
    }
}

/// Configuration for the streaming downloader
pub struct StreamingDownloaderConfig {
    pub max_concurrent_downloads: usize,
    pub buffer_size_bytes: usize,
    pub channel_buffer_size: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
}

impl Default for StreamingDownloaderConfig {
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

/// Downloads S3 objects and streams them directly to the search engine
pub struct StreamingDownloader {
    client: Client,
    config: StreamingDownloaderConfig,
    download_semaphore: Arc<Semaphore>,
}

impl StreamingDownloader {
    pub fn new(client: Client, config: StreamingDownloaderConfig) -> Self {
        let download_semaphore = Arc::new(Semaphore::new(config.max_concurrent_downloads));

        Self {
            client,
            config,
            download_semaphore,
        }
    }

    /// Process a batch of S3 objects, streaming each to the search engine
    pub async fn search_objects(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        collector: Arc<tokio::sync::Mutex<SearchResultCollector>>,
    ) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }

        // Calculate total size for progress tracking
        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        let progress = Arc::new(Mutex::new(SearchProgress::new(objects.len(), total_bytes)));

        let (tx, mut rx) =
            mpsc::channel::<(Result<()>, usize, usize)>(self.config.channel_buffer_size);

        // Spawn tasks for each object
        let mut handles = Vec::new();
        for obj in objects {
            let obj_clone = obj.clone();
            let obj_size = obj.size;
            let client = self.client.clone();
            let searcher = searcher.clone();
            let collector = collector.clone();
            let tx = tx.clone();
            let semaphore = self.download_semaphore.clone();
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await?;

                let result =
                    Self::download_and_search(client, obj_clone, searcher, collector, config).await;

                // Send result with size info for progress tracking
                match result {
                    Ok((bytes, matches)) => tx.send((Ok(()), bytes, matches)).await.ok(),
                    Err(e) => tx.send((Err(e), obj_size, 0)).await.ok(),
                };

                Ok::<(), anyhow::Error>(())
            });

            handles.push(handle);
        }

        // Drop original sender to allow channel to close when done
        drop(tx);

        // Collect results and update progress
        let mut errors = Vec::new();
        while let Some((result, bytes, matches)) = rx.recv().await {
            // Update progress
            {
                let mut prog = progress.lock().await;
                prog.update(bytes, matches);
                if prog.should_report() {
                    prog.report();
                }
            }

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

        if !errors.is_empty() {
            return Err(anyhow::anyhow!(
                "{} downloads failed: {}",
                errors.len(),
                errors.first().unwrap_or(&"unknown".to_string())
            ));
        }

        Ok(())
    }

    /// Download a single object and stream it to the search engine with retries
    /// Returns (bytes_processed, matches_found)
    async fn download_and_search(
        client: Client,
        obj: S3ObjectInfo,
        searcher: Arc<StreamSearcher>,
        collector: Arc<tokio::sync::Mutex<SearchResultCollector>>,
        config: StreamingDownloaderConfig,
    ) -> Result<(usize, usize)> {
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();

        let inner = || async {
            Self::download_and_search_inner(
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
                    "Retry scheduled for {}/{} in {:.1}s: {}",
                    bucket,
                    key,
                    dur.as_secs_f64(),
                    err
                );
            })
            .await
    }

    /// Inner download and search function (without retries)
    /// Returns (bytes_processed, matches_found)
    async fn download_and_search_inner(
        client: &Client,
        obj: &S3ObjectInfo,
        searcher: &Arc<StreamSearcher>,
        collector: &Arc<tokio::sync::Mutex<SearchResultCollector>>,
        buffer_size: usize,
    ) -> Result<(usize, usize)> {
        debug!(
            "Starting streaming download and search for {}/{}",
            obj.bucket, obj.key
        );

        // Get object from S3
        let resp = client
            .get_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get S3 object: {}", e))?;

        // Get content length if available
        let content_length = resp.content_length.unwrap_or(0) as usize;
        debug!(
            "Content length for {}/{}: {} bytes",
            obj.bucket, obj.key, content_length
        );

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

        // Create a local collector to avoid mutex contention
        let mut local_collector = SearchResultCollector::new();
        local_collector.mark_file_searched();

        // Use spawn_blocking with SyncIoBridge for true streaming
        // This allows grep_searcher to work with the async stream
        let (bytes_processed, local_collector) = tokio::task::spawn_blocking(move || {
            // Create a sync bridge from the async reader
            let sync_reader = SyncIoBridge::new(decompressed);

            // Use a buffered reader for better performance
            let buffered_sync = std::io::BufReader::with_capacity(buffer_size, sync_reader);

            // Count bytes as we read
            let (counting_reader, bytes_counter) = CountingReader::new(buffered_sync);

            // Search the stream directly without loading into memory
            searcher_clone.search_stream(&bucket, &key, counting_reader, &mut local_collector)?;

            let bytes_processed = bytes_counter.load(Ordering::Relaxed);

            Ok::<(usize, SearchResultCollector), anyhow::Error>((bytes_processed, local_collector))
        })
        .await??;

        let matches_found = local_collector.match_count();

        // Single lock to merge results
        {
            let mut collector_guard = collector.lock().await;
            collector_guard.merge(local_collector);
        }

        debug!("Completed search in {}/{}", obj.bucket, obj.key);
        Ok((bytes_processed, matches_found))
    }

    /// Process a batch of S3 objects, streaming results directly to HTTP API
    /// Returns (files_searched, total_matches)
    pub async fn search_objects_to_http(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        http_sender: mpsc::Sender<HttpMatchToSend>,
    ) -> Result<(usize, usize)> {
        if objects.is_empty() {
            return Ok((0, 0));
        }

        // Calculate total size for progress tracking
        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        let progress = Arc::new(Mutex::new(SearchProgress::new(objects.len(), total_bytes)));

        let (tx, mut rx) =
            mpsc::channel::<(Result<()>, usize, usize)>(self.config.channel_buffer_size);

        // Spawn tasks for each object
        let mut handles = Vec::new();
        for obj in objects {
            let obj_clone = obj.clone();
            let obj_size = obj.size;
            let client = self.client.clone();
            let searcher = searcher.clone();
            let http_sender = http_sender.clone();
            let tx = tx.clone();
            let semaphore = self.download_semaphore.clone();
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await?;

                let result = Self::download_and_search_to_http(
                    client,
                    obj_clone,
                    searcher,
                    http_sender,
                    config,
                )
                .await;

                // Send result with size info for progress tracking
                match result {
                    Ok((bytes, matches)) => tx.send((Ok(()), bytes, matches)).await.ok(),
                    Err(e) => tx.send((Err(e), obj_size, 0)).await.ok(),
                };

                Ok::<(), anyhow::Error>(())
            });

            handles.push(handle);
        }

        // Drop original sender to allow channel to close when done
        drop(tx);

        // Collect results and update progress
        let mut errors = Vec::new();
        let mut total_matches = 0usize;
        let mut files_searched = 0usize;
        while let Some((result, bytes, matches)) = rx.recv().await {
            files_searched += 1;
            total_matches += matches;

            // Update progress
            {
                let mut prog = progress.lock().await;
                prog.update(bytes, matches);
                if prog.should_report() {
                    prog.report();
                }
            }

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

        if !errors.is_empty() {
            return Err(anyhow::anyhow!(
                "{} downloads failed: {}",
                errors.len(),
                errors.first().unwrap_or(&"unknown".to_string())
            ));
        }

        Ok((files_searched, total_matches))
    }

    /// Download a single object and stream results to HTTP with retries
    /// Returns (bytes_processed, matches_found)
    async fn download_and_search_to_http(
        client: Client,
        obj: S3ObjectInfo,
        searcher: Arc<StreamSearcher>,
        http_sender: mpsc::Sender<HttpMatchToSend>,
        config: StreamingDownloaderConfig,
    ) -> Result<(usize, usize)> {
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();

        let inner = || async {
            Self::download_and_search_to_http_inner(
                &client,
                &obj,
                &searcher,
                http_sender.clone(),
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
                    "Retry scheduled for {}/{} in {:.1}s: {}",
                    bucket,
                    key,
                    dur.as_secs_f64(),
                    err
                );
            })
            .await
    }

    /// Inner download and search to HTTP function (without retries)
    /// Returns (bytes_processed, matches_found)
    async fn download_and_search_to_http_inner(
        client: &Client,
        obj: &S3ObjectInfo,
        searcher: &Arc<StreamSearcher>,
        http_sender: mpsc::Sender<HttpMatchToSend>,
        buffer_size: usize,
    ) -> Result<(usize, usize)> {
        debug!(
            "Starting streaming download and HTTP search for {}/{}",
            obj.bucket, obj.key
        );

        // Get object from S3
        let resp = client
            .get_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get S3 object: {}", e))?;

        // Get content length if available
        let content_length = resp.content_length.unwrap_or(0) as usize;
        debug!(
            "Content length for {}/{}: {} bytes",
            obj.bucket, obj.key, content_length
        );

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

        // Create a streaming collector that sends to HTTP
        let mut http_collector = HttpStreamingCollector::new(http_sender);
        http_collector.mark_file_searched();

        // Use spawn_blocking with SyncIoBridge for true streaming
        let (bytes_processed, matches_found) = tokio::task::spawn_blocking(move || {
            // Create a sync bridge from the async reader
            let sync_reader = SyncIoBridge::new(decompressed);

            // Use a buffered reader for better performance
            let buffered_sync = std::io::BufReader::with_capacity(buffer_size, sync_reader);

            // Count bytes as we read
            let (counting_reader, bytes_counter) = CountingReader::new(buffered_sync);

            // Search the stream directly, streaming results to HTTP
            searcher_clone.search_stream(&bucket, &key, counting_reader, &mut http_collector)?;

            let bytes_processed = bytes_counter.load(Ordering::Relaxed);
            let matches_found = http_collector.match_count();

            Ok::<(usize, usize), anyhow::Error>((bytes_processed, matches_found))
        })
        .await??;

        debug!("Completed HTTP streaming search in {}/{}", obj.bucket, obj.key);
        Ok((bytes_processed, matches_found))
    }
}

// Clone implementation for config
impl Clone for StreamingDownloaderConfig {
    fn clone(&self) -> Self {
        Self {
            max_concurrent_downloads: self.max_concurrent_downloads,
            buffer_size_bytes: self.buffer_size_bytes,
            channel_buffer_size: self.channel_buffer_size,
            max_retries: self.max_retries,
            initial_retry_delay: self.initial_retry_delay,
        }
    }
}
