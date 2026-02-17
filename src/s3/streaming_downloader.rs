// src/s3/streaming_downloader.rs
use crate::config::types::S3ObjectInfo;
use crate::search::{DirectFileCollector, HttpStreamingCollector, SearchCollector, StreamSearcher};
use crate::search::SharedFileWriter;
use anyhow::Result;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use aws_sdk_s3::Client;
use backon::{ExponentialBuilder, Retryable};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, BufReader};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info, warn};

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
        self.last_report_time.elapsed() > Duration::from_secs(10)
    }

    fn report(&mut self) {
        let pct = (self.files_processed * 100) / self.total_files.max(1);

        info!(
            files_done = self.files_processed,
            files_total = self.total_files,
            pct = pct,
            mb_done = self.bytes_processed / 1_000_000,
            mb_total = self.total_bytes / 1_000_000,
            matches = self.matches_found,
            elapsed_s = self.start_time.elapsed().as_secs_f32(),
            "Search progress"
        );

        self.last_report_time = std::time::Instant::now();
    }
}

/// Configuration for the streaming downloader
#[derive(Clone)]
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

    /// Process a batch of S3 objects, streaming results directly to HTTP API.
    /// Returns (files_searched, total_matches)
    pub async fn search_objects_to_http(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        http_sender: mpsc::Sender<String>,
    ) -> Result<(usize, usize)> {
        self.search_objects(objects, searcher, move |_obj: &S3ObjectInfo| {
            HttpStreamingCollector::new(http_sender.clone())
        })
        .await
    }

    /// Process a batch of S3 objects, streaming results to file writer.
    /// Uses lazy spawning: acquires semaphore permit BEFORE tokio::spawn, so only
    /// max_concurrent_downloads tasks exist at any time. Memory is O(concurrency), not O(total_objects).
    /// Returns (files_searched, total_matches)
    pub async fn search_objects_to_file(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        writer: SharedFileWriter,
    ) -> Result<(usize, usize)> {
        self.search_objects(objects, searcher, move |obj: &S3ObjectInfo| {
            DirectFileCollector::new(writer.clone(), obj.prefix.clone())
        })
        .await
    }

    /// Generic batch processor: downloads S3 objects and searches them using the
    /// collector produced by `collector_factory` for each object.
    /// Uses lazy spawning (semaphore before spawn) + JoinSet, abort-on-first-error.
    /// Returns (files_searched, total_matches)
    async fn search_objects<C, F>(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        collector_factory: F,
    ) -> Result<(usize, usize)>
    where
        C: SearchCollector + Send + 'static,
        F: Fn(&S3ObjectInfo) -> C + Clone + Send + Sync + 'static,
    {
        if objects.is_empty() {
            return Ok((0, 0));
        }

        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        info!(
            objects = objects.len(),
            mb = total_bytes / 1_000_000,
            concurrency = self.config.max_concurrent_downloads,
            "Starting search"
        );

        let progress = Arc::new(Mutex::new(SearchProgress::new(objects.len(), total_bytes)));

        let mut total_matches = 0usize;
        let mut files_searched = 0usize;
        let mut spawned = 0usize;

        let mut join_set: tokio::task::JoinSet<Result<(usize, usize)>> =
            tokio::task::JoinSet::new();

        // Helper: drain completed tasks, abort on first error
        macro_rules! drain_completed {
            ($join_set:expr, $progress:expr, $files_searched:expr, $total_matches:expr) => {
                while let Some(result) = $join_set.try_join_next() {
                    match result {
                        Ok(Ok((bytes, matches))) => {
                            $files_searched += 1;
                            $total_matches += matches;
                            let mut prog = $progress.lock().await;
                            prog.update(bytes, matches);
                            if prog.should_report() {
                                prog.report();
                            }
                        }
                        Ok(Err(e)) => {
                            $join_set.abort_all();
                            return Err(e);
                        }
                        Err(e) => {
                            $join_set.abort_all();
                            return Err(anyhow::anyhow!("Task panic: {e}"));
                        }
                    }
                }
            };
        }

        for obj in objects {
            // Acquire semaphore BEFORE spawn — this is the key to lazy spawning.
            // We block here until a slot opens, so at most max_concurrent_downloads
            // tasks + their S3 response buffers exist at once.
            let permit = self
                .download_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("Semaphore closed: {e}"))?;

            // Drain any completed tasks to free memory
            drain_completed!(join_set, progress, files_searched, total_matches);

            let obj_clone = obj.clone();
            let client = self.client.clone();
            let searcher = searcher.clone();
            let config = self.config.clone();
            let factory = collector_factory.clone();

            join_set.spawn(async move {
                let _permit = permit; // held until task completes
                Self::download_and_search(client, obj_clone, searcher, factory, config).await
            });

            spawned += 1;
            if spawned == self.config.max_concurrent_downloads {
                info!(
                    concurrency = self.config.max_concurrent_downloads,
                    "All concurrent slots filled, processing"
                );
            }
        }

        info!(
            spawned = spawned,
            remaining = join_set.len(),
            "All tasks spawned, draining"
        );

        // Drain remaining tasks
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((bytes, matches))) => {
                    files_searched += 1;
                    total_matches += matches;
                    let mut prog = progress.lock().await;
                    prog.update(bytes, matches);
                    if prog.should_report() {
                        prog.report();
                    }
                }
                Ok(Err(e)) => {
                    join_set.abort_all();
                    return Err(e);
                }
                Err(e) => {
                    join_set.abort_all();
                    return Err(anyhow::anyhow!("Task panic: {e}"));
                }
            }
        }

        Ok((files_searched, total_matches))
    }

    /// Download a single object and search it with retries.
    /// `make_collector` is called per retry attempt because `spawn_blocking` consumes
    /// the collector. Collectors are cheap (Arc clone + counter reset).
    /// Returns (bytes_processed, matches_found)
    async fn download_and_search<C, F>(
        client: Client,
        obj: S3ObjectInfo,
        searcher: Arc<StreamSearcher>,
        make_collector: F,
        config: StreamingDownloaderConfig,
    ) -> Result<(usize, usize)>
    where
        C: SearchCollector + Send + 'static,
        F: Fn(&S3ObjectInfo) -> C,
    {
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();

        let inner = || async {
            let collector = make_collector(&obj);
            Self::download_and_search_inner(&client, &obj, &searcher, collector, config.buffer_size_bytes)
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

    /// Download and search a single S3 object (without retries).
    /// Returns (compressed_size, matches_found)
    async fn download_and_search_inner<C: SearchCollector + Send + 'static>(
        client: &Client,
        obj: &S3ObjectInfo,
        searcher: &Arc<StreamSearcher>,
        mut collector: C,
        buffer_size: usize,
    ) -> Result<(usize, usize)> {
        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            bytes = obj.size,
            "Downloading"
        );

        // Get object from S3
        let resp = client
            .get_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get S3 object: {e}"))?;

        let content_length = resp.content_length.unwrap_or(0) as usize;
        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            bytes = content_length,
            "Content length"
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

        let bucket = obj.bucket.clone();
        let key = obj.key.clone();
        let compressed_size = content_length;
        let searcher_clone = searcher.clone();

        let matches_found = tokio::task::spawn_blocking(move || {
            let sync_reader = SyncIoBridge::new(decompressed);
            let buffered_sync = std::io::BufReader::with_capacity(buffer_size, sync_reader);

            searcher_clone.search_stream(&bucket, &key, buffered_sync, &mut collector)?;

            Ok::<usize, anyhow::Error>(collector.match_count())
        })
        .await??;

        debug!(bucket = %obj.bucket, key = %obj.key, "Completed streaming search");
        Ok((compressed_size, matches_found))
    }
}
