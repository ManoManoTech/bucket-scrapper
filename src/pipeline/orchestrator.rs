//! Pipeline orchestrator: download → decompress → filter → output.
//!
//! Only `download_inner` and `download_with_retry` perform actual S3 I/O;
//! the rest is concurrency orchestration (semaphores, channels, task pools)
//! and progress reporting.

use crate::matcher::LineMatcher;
use crate::progress::PipelineProgress;
use crate::s3::S3ObjectInfo;
use super::observer::{ChannelObserver, DownloadObserver, PipelineObserver};
use super::SharedFileWriter;
use anyhow::Result;
use aws_sdk_s3::Client;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// A single decompressed line tagged with its source file.
struct DecompressedLine {
    text: String,
    source: Arc<S3ObjectInfo>,
}

/// Where filter workers send their matches.
enum FilterOutput {
    Http {
        sender: flume::Sender<String>,
        hostname: String,
        service: String,
        team: Option<String>,
    },
    File(SharedFileWriter),
}

/// Configuration for the streaming downloader
#[derive(Clone)]
pub struct StreamingDownloaderConfig {
    pub max_concurrent_downloads: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
    pub progress_interval: Duration,
    /// Number of filter worker tasks (default: cpu_count / 2)
    pub filter_tasks: usize,
    /// Line channel capacity between download+decompress and filter workers
    /// (RAM ≈ this × ~200 bytes avg line)
    pub line_buffer_size: usize,
}

impl Default for StreamingDownloaderConfig {
    fn default() -> Self {
        let filter_tasks = std::thread::available_parallelism()
            .map(|n| n.get() / 2)
            .unwrap_or(2)
            .max(1);
        Self {
            max_concurrent_downloads: 32,
            max_retries: 10,
            initial_retry_delay: Duration::from_secs(2),
            progress_interval: Duration::from_secs(1),
            filter_tasks,
            line_buffer_size: 1_000,
        }
    }
}

/// Downloads S3 objects, stream-decompresses them into lines, and feeds them
/// to filter workers that apply regex matching.
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
    ///
    /// `fatal_error` is the flag from [`HttpResultWriter::fatal_error_flag`].
    /// When it becomes `true` the download coordinator, filter workers and line
    /// emitters bail out early so we stop wasting bandwidth on data that will
    /// never be uploaded.
    ///
    /// Returns (files_searched, total_matches)
    pub async fn search_objects_to_http(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<LineMatcher>,
        http_sender: flume::Sender<String>,
        hostname: String,
        service: String,
        team: Option<String>,
        observer: PipelineObserver,
        fatal_error: Arc<AtomicBool>,
    ) -> Result<(usize, usize)> {
        let output = FilterOutput::Http {
            sender: http_sender,
            hostname,
            service,
            team,
        };
        self.search_objects(objects, searcher, Some(observer), output, Some(fatal_error))
            .await
    }

    /// Process a batch of S3 objects, streaming results to file writer.
    /// Returns (files_searched, total_matches)
    pub async fn search_objects_to_file(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<LineMatcher>,
        writer: SharedFileWriter,
    ) -> Result<(usize, usize)> {
        let output = FilterOutput::File(writer);
        self.search_objects(objects, searcher, None, output, None)
            .await
    }

    /// Generic batch processor with decoupled download+decompress and filter stages.
    ///
    /// Architecture:
    /// ```text
    /// [sem N] → download + stream_decompress → lines → [line_ch] → filter workers → exporter
    /// ```
    ///
    /// Download tasks acquire a semaphore permit, fetch the compressed body,
    /// stream-decompress it line by line pushing into the line channel, then
    /// release the permit. Filter workers pull lines and run regex matching.
    ///
    /// Returns (files_searched, total_matches)
    async fn search_objects(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<LineMatcher>,
        pipeline: Option<PipelineObserver>,
        output: FilterOutput,
        fatal_error: Option<Arc<AtomicBool>>,
    ) -> Result<(usize, usize)> {
        if objects.is_empty() {
            return Ok((0, 0));
        }

        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        info!(
            objects = objects.len(),
            mb = total_bytes / 1_000_000,
            download_concurrency = self.config.max_concurrent_downloads,
            filter_workers = self.config.filter_tasks,
            line_buffer = self.config.line_buffer_size,
            "Starting search"
        );

        // Line channel between download+decompress and filter workers
        let (line_tx, line_rx) =
            flume::bounded::<DecompressedLine>(self.config.line_buffer_size);

        let download_observer = DownloadObserver::new();
        let match_count = Arc::new(AtomicUsize::new(0));

        let progress = Arc::new(Mutex::new(PipelineProgress::new(
            objects.len(),
            total_bytes,
            self.config.progress_interval,
            pipeline,
            ChannelObserver::from_receiver(&line_rx),
            download_observer.clone(),
            match_count.clone(),
        )));

        // Emit initial progress at t=0 so charts always have a starting point
        {
            let mut prog = progress.lock().await;
            prog.report();
        }

        // --- Spawn download coordinator ---
        let download_handle = {
            let client = self.client.clone();
            let config = self.config.clone();
            let semaphore = self.download_semaphore.clone();
            let objects = objects.to_vec();
            let tx = line_tx;
            let progress = progress.clone();
            let fe = fatal_error.clone();

            tokio::spawn(async move {
                let result = Self::download_coordinator(
                    client,
                    &objects,
                    config,
                    semaphore,
                    tx,
                    download_observer,
                    progress,
                    fe,
                )
                .await;
                // tx is dropped here → channel closes → workers drain and exit
                result
            })
        };

        // --- Spawn filter workers ---
        let output = Arc::new(output);
        let mut worker_handles: Vec<tokio::task::JoinHandle<Result<usize>>> =
            Vec::with_capacity(self.config.filter_tasks);

        for worker_id in 0..self.config.filter_tasks {
            let rx = line_rx.clone();
            let searcher = searcher.clone();
            let output = output.clone();
            let match_count = match_count.clone();
            let fe = fatal_error.clone();

            worker_handles.push(tokio::spawn(async move {
                Self::filter_worker(worker_id, rx, searcher, output, match_count, fe).await
            }));
        }

        // Drop our clone of line_rx so channel closes when coordinator drops tx
        drop(line_rx);

        // --- Spawn periodic progress ticker ---
        // Reports progress even when no files complete (e.g. pipeline backed up).
        let progress_ticker = {
            let progress = progress.clone();
            let interval = self.config.progress_interval;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs_f64(interval.as_secs_f64())).await;
                    let mut prog = progress.lock().await;
                    if prog.should_report() {
                        prog.report();
                    }
                }
            })
        };

        // --- Join download coordinator ---
        match download_handle.await {
            Ok(Ok(files_processed)) => {
                debug!(files = files_processed, "Download coordinator finished");
            }
            Ok(Err(e)) => {
                progress_ticker.abort();
                for h in &worker_handles {
                    h.abort();
                }
                return Err(e);
            }
            Err(e) => {
                progress_ticker.abort();
                for h in &worker_handles {
                    h.abort();
                }
                return Err(anyhow::anyhow!("Download coordinator panicked: {e}"));
            }
        }

        // --- Join filter workers ---
        let mut total_matches = 0usize;

        for handle in worker_handles {
            match handle.await {
                Ok(Ok(matches)) => {
                    total_matches += matches;
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(anyhow::anyhow!("Filter worker panicked: {e}")),
            }
        }

        // Stop the progress ticker
        progress_ticker.abort();

        // files_searched = total files processed by coordinator
        let files_searched = progress.lock().await.files_processed;

        Ok((files_searched, total_matches))
    }

    /// Coordinates download+decompress tasks using semaphore + JoinSet.
    /// Each task downloads compressed bytes, then stream-decompresses and emits
    /// lines into the channel.
    /// Drops `line_tx` on return to close the channel.
    /// Returns the number of files successfully processed.
    #[allow(clippy::too_many_arguments)]
    async fn download_coordinator(
        client: Client,
        objects: &[S3ObjectInfo],
        config: StreamingDownloaderConfig,
        semaphore: Arc<Semaphore>,
        line_tx: flume::Sender<DecompressedLine>,
        download_observer: DownloadObserver,
        progress: Arc<Mutex<PipelineProgress>>,
        fatal_error: Option<Arc<AtomicBool>>,
    ) -> Result<usize> {
        let mut spawned = 0usize;
        let mut completed = 0usize;

        let is_fatal = |fe: &Option<Arc<AtomicBool>>| -> bool {
            fe.as_ref()
                .is_some_and(|f| f.load(Ordering::Relaxed))
        };

        let mut join_set: tokio::task::JoinSet<Result<usize>> = tokio::task::JoinSet::new();

        // Helper: drain completed download tasks
        macro_rules! drain_completed {
            () => {
                while let Some(result) = join_set.try_join_next() {
                    match result {
                        Ok(Ok(compressed_size)) => {
                            completed += 1;
                            let mut prog = progress.lock().await;
                            prog.update(compressed_size);
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
                            return Err(anyhow::anyhow!("Download task panic: {e}"));
                        }
                    }
                }
            };
        }

        let max_concurrent = config.max_concurrent_downloads;

        for obj in objects {
            // Bail out early if the HTTP pipeline hit a fatal error
            if is_fatal(&fatal_error) {
                warn!(
                    spawned = spawned,
                    completed = completed,
                    remaining_objects = objects.len() - spawned,
                    "Pipeline fatal error detected, aborting downloads"
                );
                join_set.abort_all();
                return Err(anyhow::anyhow!(
                    "Pipeline aborted: fatal HTTP error (downloaded {completed}/{} objects)",
                    objects.len()
                ));
            }

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
            let dl_obs = download_observer.clone();
            let tx = line_tx.clone();
            let fe = fatal_error.clone();

            join_set.spawn(async move {
                // Download compressed bytes (with retries)
                let (compressed, obj_info) =
                    Self::download_with_retry(client, obj_clone, config, dl_obs).await?;
                let compressed_size = compressed.len();

                // Release permit — S3 connection is free
                drop(permit);

                // Stream-decompress and emit lines (no retry — idempotent lines)
                let source = Arc::new(obj_info);
                let tx_clone = tx;
                tokio::task::spawn_blocking(move || {
                    Self::emit_lines(compressed, &source, &tx_clone, fe)
                })
                .await
                .map_err(|e| anyhow::anyhow!("Decompress+emit task panic: {e}"))??;

                Ok(compressed_size)
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
            // Check fatal error between each join — abort remaining tasks early
            if is_fatal(&fatal_error) {
                warn!(
                    completed = completed,
                    remaining = join_set.len(),
                    "Pipeline fatal error detected during drain, aborting"
                );
                join_set.abort_all();
                return Err(anyhow::anyhow!(
                    "Pipeline aborted: fatal HTTP error (downloaded {completed}/{} objects)",
                    objects.len()
                ));
            }

            match result {
                Ok(Ok(compressed_size)) => {
                    completed += 1;
                    let mut prog = progress.lock().await;
                    prog.update(compressed_size);
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
                    return Err(anyhow::anyhow!("Download task panic: {e}"));
                }
            }
        }

        debug!(completed = completed, "Download coordinator finished");
        Ok(completed)
    }

    /// Stream-decompress compressed bytes and emit lines into the channel.
    /// Runs inside `spawn_blocking` (synchronous IO).
    ///
    /// Checks `fatal_error` every 1024 lines so we stop emitting early when the
    /// HTTP pipeline is dead — avoids blocking on a full line channel.
    fn emit_lines(
        compressed: Bytes,
        source: &Arc<S3ObjectInfo>,
        line_tx: &flume::Sender<DecompressedLine>,
        fatal_error: Option<Arc<AtomicBool>>,
    ) -> Result<()> {
        let cursor = std::io::Cursor::new(compressed);
        let reader: Box<dyn Read> = if source.key.ends_with(".gz") {
            Box::new(flate2::read::GzDecoder::new(cursor))
        } else if source.key.ends_with(".zst") || source.key.ends_with(".zstd") {
            Box::new(zstd::Decoder::new(cursor)?)
        } else {
            Box::new(cursor)
        };

        let mut buf_reader = BufReader::new(reader);
        let mut line = String::new();
        let mut lines_emitted = 0u64;
        loop {
            // Check fatal error every 1024 lines (AtomicBool load is cheap but
            // no need to check every single line)
            if lines_emitted & 0x3FF == 0 {
                if let Some(ref fe) = fatal_error {
                    if fe.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                }
            }

            line.clear();
            if buf_reader.read_line(&mut line)? == 0 {
                break;
            }
            line_tx
                .send(DecompressedLine {
                    text: line.clone(),
                    source: source.clone(),
                })
                .map_err(|_| anyhow::anyhow!("Filter workers gone, channel closed"))?;
            lines_emitted += 1;
        }

        Ok(())
    }

    /// Download compressed bytes from S3 with retries.
    /// Returns (compressed_bytes, object_info).
    async fn download_with_retry(
        client: Client,
        obj: S3ObjectInfo,
        config: StreamingDownloaderConfig,
        download_observer: DownloadObserver,
    ) -> Result<(Bytes, S3ObjectInfo)> {
        let bucket = obj.bucket.clone();
        let key = obj.key.clone();

        let inner = || async {
            Self::download_inner(&client, &obj, &download_observer).await
        };

        let retry_params = ExponentialBuilder::default()
            .with_min_delay(config.initial_retry_delay)
            .with_max_delay(Duration::from_secs(60))
            .with_factor(2.0)
            .with_jitter()
            .with_max_times(config.max_retries as usize);

        let compressed = inner
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
            .await?;

        Ok((compressed, obj))
    }

    /// Download a single S3 object (no retries). Returns compressed bytes.
    async fn download_inner(
        client: &Client,
        obj: &S3ObjectInfo,
        download_observer: &DownloadObserver,
    ) -> Result<Bytes> {
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

        let compressed = resp
            .body
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read S3 object body: {e}"))?
            .into_bytes();

        download_observer.add_bytes(compressed.len());

        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            compressed_bytes = compressed.len(),
            "Downloaded"
        );

        Ok(compressed)
    }

    /// Filter worker: pulls lines from channel, applies regex, emits matches.
    /// Runs entirely in spawn_blocking (CPU-bound regex + blocking channel recv).
    ///
    /// Checks `fatal_error` every 1024 lines so the worker exits promptly when
    /// the HTTP pipeline is dead, even with low match rates where the send-side
    /// error would not be hit often.
    async fn filter_worker(
        worker_id: usize,
        rx: flume::Receiver<DecompressedLine>,
        searcher: Arc<LineMatcher>,
        output: Arc<FilterOutput>,
        match_count: Arc<AtomicUsize>,
        fatal_error: Option<Arc<AtomicBool>>,
    ) -> Result<usize> {
        let result = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut local = 0usize;
            let mut lines_processed = 0u64;
            while let Ok(line) = rx.recv() {
                // Check fatal error every 1024 lines — bail early so the
                // decompressed-line channel drains and download tasks can stop.
                lines_processed += 1;
                if lines_processed & 0x3FF == 0 {
                    if let Some(ref fe) = fatal_error {
                        if fe.load(Ordering::Relaxed) {
                            return Ok(local);
                        }
                    }
                }

                if searcher.matches_line(line.text.as_bytes()) {
                    match output.as_ref() {
                        FilterOutput::Http { sender, hostname, service, team } => {
                            // Format as NDJSON: {"message": "...", "hostname": "...", "service": "...", "context": {"team": "..."}}
                            let escaped_message = line.text.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r");
                            let escaped_hostname = hostname.replace('\\', "\\\\").replace('"', "\\\"");
                            let escaped_service = service.replace('\\', "\\\\").replace('"', "\\\"");
                            let json_line = if let Some(t) = team {
                                let escaped_team = t.replace('\\', "\\\\").replace('"', "\\\"");
                                format!("{{\"message\":\"{}\",\"hostname\":\"{}\",\"service\":\"{}\",\"context\":{{\"team\":\"{}\"}}}}\n",
                                    escaped_message, escaped_hostname, escaped_service, escaped_team)
                            } else {
                                format!("{{\"message\":\"{}\",\"hostname\":\"{}\",\"service\":\"{}\"}}\n",
                                    escaped_message, escaped_hostname, escaped_service)
                            };
                            sender
                                .send(json_line)
                                .map_err(|_| anyhow::anyhow!("HTTP consumer gone, channel closed"))?;
                        }
                        FilterOutput::File(writer) => {
                            writer.write_match(&line.source.prefix, &line.text)?;
                        }
                    }
                    local += 1;
                    match_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(local)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Filter worker panic: {e}"))??;

        debug!(worker = worker_id, matches = result, "Filter worker finished");
        Ok(result)
    }
}
