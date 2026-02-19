// src/s3/streaming_downloader.rs
use crate::config::types::S3ObjectInfo;
use crate::pipeline::{DirectFileExporter, HttpStreamingExporter, PipelineObserver, SearchExporter, StreamSearcher};
use crate::pipeline::SharedFileWriter;
use crate::progress::{ChannelObserver, SearchProgress};
use anyhow::Result;
use aws_sdk_s3::Client;
use backon::{ExponentialBuilder, Retryable};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// A downloaded and decompressed S3 object ready for search.
struct DownloadedObject {
    data: Vec<u8>,
    info: S3ObjectInfo,
}

/// Configuration for the streaming downloader
#[derive(Clone)]
pub struct StreamingDownloaderConfig {
    pub max_concurrent_downloads: usize,
    pub buffer_size_bytes: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
    pub progress_interval: Duration,
    /// Number of search worker tasks (default: cpu_count / 2)
    pub processing_tasks: usize,
    /// Buffer capacity between download+decompress and search
    /// (RAM ≈ this × avg decompressed file size)
    pub download_buffer_size: usize,
}

impl Default for StreamingDownloaderConfig {
    fn default() -> Self {
        let processing_tasks = std::thread::available_parallelism()
            .map(|n| n.get() / 2)
            .unwrap_or(2)
            .max(1);
        Self {
            max_concurrent_downloads: 32,
            buffer_size_bytes: 64 * 1024, // 64KB chunks
            max_retries: 10,
            initial_retry_delay: Duration::from_secs(2),
            progress_interval: Duration::from_secs(3),
            processing_tasks,
            download_buffer_size: 1000,
        }
    }
}

/// Downloads S3 objects, decompresses them, and feeds them to search workers.
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
        http_sender: flume::Sender<String>,
        observer: PipelineObserver,
    ) -> Result<(usize, usize)> {
        self.search_objects(objects, searcher, Some(observer), move |_obj: &S3ObjectInfo| {
            HttpStreamingExporter::new(http_sender.clone())
        })
        .await
    }

    /// Process a batch of S3 objects, streaming results to file writer.
    /// Returns (files_searched, total_matches)
    pub async fn search_objects_to_file(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        writer: SharedFileWriter,
    ) -> Result<(usize, usize)> {
        self.search_objects(objects, searcher, None, move |obj: &S3ObjectInfo| {
            DirectFileExporter::new(writer.clone(), obj.prefix.clone())
        })
        .await
    }

    /// Generic batch processor with decoupled download+decompress and search stages.
    ///
    /// Architecture:
    /// ```text
    /// [sem N] → download+decompress → [decompressed_ch] → search workers → exporter
    /// ```
    ///
    /// Download tasks acquire a semaphore permit, fetch and decompress the object,
    /// release the permit (freeing the S3 connection), then push to the decompressed
    /// channel. Search workers pull from the channel and run regex search via
    /// spawn_blocking.
    ///
    /// Returns (files_searched, total_matches)
    async fn search_objects<E, F>(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<StreamSearcher>,
        pipeline: Option<PipelineObserver>,
        exporter_factory: F,
    ) -> Result<(usize, usize)>
    where
        E: SearchExporter + Send + 'static,
        F: Fn(&S3ObjectInfo) -> E + Clone + Send + Sync + 'static,
    {
        if objects.is_empty() {
            return Ok((0, 0));
        }

        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        info!(
            objects = objects.len(),
            mb = total_bytes / 1_000_000,
            download_concurrency = self.config.max_concurrent_downloads,
            search_workers = self.config.processing_tasks,
            download_buffer = self.config.download_buffer_size,
            "Starting search"
        );

        // Channel between download+decompress and search workers
        let (decompressed_tx, decompressed_rx) =
            flume::bounded::<DownloadedObject>(self.config.download_buffer_size);

        let progress = Arc::new(Mutex::new(SearchProgress::new(
            objects.len(),
            total_bytes,
            self.config.progress_interval,
            pipeline,
            ChannelObserver::from_sender(&decompressed_tx),
        )));

        // --- Spawn download coordinator ---
        let download_handle = {
            let client = self.client.clone();
            let config = self.config.clone();
            let semaphore = self.download_semaphore.clone();
            let objects = objects.to_vec();
            let tx = decompressed_tx;

            tokio::spawn(async move {
                let result = Self::download_coordinator(
                    client, &objects, config, semaphore, tx,
                ).await;
                // tx is dropped here → channel closes → workers drain and exit
                result
            })
        };

        // --- Spawn search workers ---
        let mut worker_handles: Vec<tokio::task::JoinHandle<Result<(usize, usize)>>> =
            Vec::with_capacity(self.config.processing_tasks);

        for worker_id in 0..self.config.processing_tasks {
            let rx = decompressed_rx.clone();
            let searcher = searcher.clone();
            let factory = exporter_factory.clone();
            let progress = progress.clone();

            worker_handles.push(tokio::spawn(async move {
                Self::search_worker(worker_id, rx, searcher, factory, progress).await
            }));
        }

        // Drop our clone of decompressed_rx so channel closes when coordinator drops tx
        drop(decompressed_rx);

        // --- Join download coordinator ---
        match download_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                // Abort workers on download error
                for h in &worker_handles {
                    h.abort();
                }
                return Err(e);
            }
            Err(e) => {
                for h in &worker_handles {
                    h.abort();
                }
                return Err(anyhow::anyhow!("Download coordinator panicked: {e}"));
            }
        }

        // --- Join search workers ---
        let mut total_matches = 0usize;
        let mut files_searched = 0usize;

        for handle in worker_handles {
            match handle.await {
                Ok(Ok((files, matches))) => {
                    files_searched += files;
                    total_matches += matches;
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(anyhow::anyhow!("Search worker panicked: {e}")),
            }
        }

        Ok((files_searched, total_matches))
    }

    /// Coordinates download+decompress tasks using semaphore + JoinSet.
    /// Drops `decompressed_tx` on return to close the channel.
    async fn download_coordinator(
        client: Client,
        objects: &[S3ObjectInfo],
        config: StreamingDownloaderConfig,
        semaphore: Arc<Semaphore>,
        decompressed_tx: flume::Sender<DownloadedObject>,
    ) -> Result<()> {
        let mut spawned = 0usize;
        let mut completed = 0usize;

        let mut join_set: tokio::task::JoinSet<Result<DownloadedObject>> =
            tokio::task::JoinSet::new();

        // Helper: drain completed download tasks, send to channel
        macro_rules! drain_completed {
            () => {
                while let Some(result) = join_set.try_join_next() {
                    match result {
                        Ok(Ok(obj)) => {
                            completed += 1;
                            if decompressed_tx.send_async(obj).await.is_err() {
                                // All search workers gone
                                join_set.abort_all();
                                return Err(anyhow::anyhow!("Search workers gone, channel closed"));
                            }
                        }
                        Ok(Err(e)) => {
                            join_set.abort_all();
                            return Err(e);
                        }
                        Err(e) => {
                            join_set.abort_all();
                            return Err(anyhow::anyhow!("Download task panic: {e}"));
                        }
                    }
                }
            };
        }

        let max_concurrent = config.max_concurrent_downloads;

        for obj in objects {
            // Acquire semaphore BEFORE spawn — lazy spawning
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("Semaphore closed: {e}"))?;

            drain_completed!();

            let obj_clone = obj.clone();
            let client = client.clone();
            let config = config.clone();

            join_set.spawn(async move {
                // Hold permit during download+decompress, release before channel send
                let result = Self::download_and_decompress(client, obj_clone, config).await;
                drop(permit);
                result
            });

            spawned += 1;
            if spawned == max_concurrent {
                info!(
                    concurrency = max_concurrent,
                    "All download slots filled, processing"
                );
            }
        }

        info!(
            spawned = spawned,
            remaining = join_set.len(),
            "All downloads spawned, draining"
        );

        // Drain remaining
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(obj)) => {
                    completed += 1;
                    if decompressed_tx.send_async(obj).await.is_err() {
                        join_set.abort_all();
                        return Err(anyhow::anyhow!("Search workers gone, channel closed"));
                    }
                }
                Ok(Err(e)) => {
                    join_set.abort_all();
                    return Err(e);
                }
                Err(e) => {
                    join_set.abort_all();
                    return Err(anyhow::anyhow!("Download task panic: {e}"));
                }
            }
        }

        debug!(completed = completed, "Download coordinator finished");
        Ok(())
    }

    /// Download and decompress a single S3 object with retries.
    /// Returns the decompressed data.
    async fn download_and_decompress(
        client: Client,
        obj: S3ObjectInfo,
        config: StreamingDownloaderConfig,
    ) -> Result<DownloadedObject> {
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();

        let inner = || async {
            Self::download_and_decompress_inner(&client, &obj).await
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

    /// Download and decompress a single S3 object (no retries).
    async fn download_and_decompress_inner(
        client: &Client,
        obj: &S3ObjectInfo,
    ) -> Result<DownloadedObject> {
        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            bytes = obj.size,
            "Downloading"
        );

        let resp = client
            .get_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get S3 object: {e}"))?;

        let content_length = resp.content_length.unwrap_or(0) as usize;

        // Collect entire compressed body into memory
        let compressed = resp.body.collect().await
            .map_err(|e| anyhow::anyhow!("Failed to read S3 object body: {e}"))?
            .into_bytes();

        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            compressed_bytes = compressed.len(),
            content_length = content_length,
            "Downloaded, decompressing"
        );

        // Decompress synchronously in spawn_blocking
        let key = obj.key.clone();
        let data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let cursor = std::io::Cursor::new(compressed);
            let mut decompressed = Vec::new();

            if key.ends_with(".gz") {
                let mut decoder = flate2::read::GzDecoder::new(cursor);
                std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            } else if key.ends_with(".zst") || key.ends_with(".zstd") {
                let mut decoder = zstd::Decoder::new(cursor)?;
                std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            } else {
                let mut reader = cursor;
                std::io::Read::read_to_end(&mut reader, &mut decompressed)?;
            }

            Ok(decompressed)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Decompress task panic: {e}"))??;

        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            compressed_bytes = content_length,
            decompressed_bytes = data.len(),
            "Decompressed"
        );

        Ok(DownloadedObject {
            data,
            info: obj.clone(),
        })
    }

    /// Search worker: pulls decompressed objects from channel, runs regex search.
    async fn search_worker<E, F>(
        worker_id: usize,
        rx: flume::Receiver<DownloadedObject>,
        searcher: Arc<StreamSearcher>,
        exporter_factory: F,
        progress: Arc<Mutex<SearchProgress>>,
    ) -> Result<(usize, usize)>
    where
        E: SearchExporter + Send + 'static,
        F: Fn(&S3ObjectInfo) -> E,
    {
        let mut files_searched = 0usize;
        let mut total_matches = 0usize;

        while let Ok(obj) = rx.recv_async().await {
            let compressed_size = obj.info.size;
            let bucket = obj.info.bucket.clone();
            let key = obj.info.key.clone();
            let mut exporter = exporter_factory(&obj.info);
            let searcher = searcher.clone();

            let matches_found = tokio::task::spawn_blocking(move || {
                let cursor = std::io::Cursor::new(obj.data);
                let reader = std::io::BufReader::new(cursor);
                searcher.search_stream(&bucket, &key, reader, &mut exporter)?;
                Ok::<usize, anyhow::Error>(exporter.match_count())
            })
            .await
            .map_err(|e| anyhow::anyhow!("Search task panic: {e}"))??;

            files_searched += 1;
            total_matches += matches_found;

            let mut prog = progress.lock().await;
            prog.update(compressed_size, matches_found);
            if prog.should_report() {
                prog.report();
            }
        }

        debug!(worker = worker_id, files = files_searched, matches = total_matches, "Search worker finished");
        Ok((files_searched, total_matches))
    }
}
