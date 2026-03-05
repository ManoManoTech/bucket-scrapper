//! Pipeline orchestrator: download → decompress → filter → output.
//!
//! Downloads stream S3 response chunks directly into the decompressor via a
//! bounded channel (`ChunkReader`), avoiding full-object buffering.  Retries
//! resume mid-object using S3 range requests (`bytes=N-`).

use crate::matcher::LineMatcher;
use crate::progress::PipelineProgress;
use crate::s3::{self, S3ObjectInfo};
use super::observer::{ChannelObserver, DownloadObserver, PipelineObserver};
use super::SharedFileWriter;
use anyhow::Result;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::io::{self, BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

/// A single decompressed line tagged with its source file.
struct DecompressedLine {
    data: Vec<u8>,
    source: Arc<S3ObjectInfo>,
}

/// Where filter workers send their matches.
enum FilterOutput {
    Http(flume::Sender<Vec<u8>>),
    File(SharedFileWriter),
}

/// Bridges async S3 `ByteStream` chunks into synchronous [`Read`] for
/// `spawn_blocking`.  A bounded `flume` channel provides backpressure so the
/// async chunk-forwarding loop slows down when the decompressor can't keep up.
struct ChunkReader {
    rx: flume::Receiver<Bytes>,
    /// Leftover bytes from the last chunk not yet consumed by `read()`.
    remainder: Bytes,
}

impl ChunkReader {
    fn new(rx: flume::Receiver<Bytes>) -> Self {
        Self {
            rx,
            remainder: Bytes::new(),
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Serve leftover bytes from the previous chunk first.
        if self.remainder.is_empty() {
            match self.rx.recv() {
                Ok(chunk) => self.remainder = chunk,
                // Sender dropped — EOF.
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.remainder.len());
        buf[..n].copy_from_slice(&self.remainder[..n]);
        self.remainder = self.remainder.slice(n..);
        Ok(n)
    }
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
        http_sender: flume::Sender<Vec<u8>>,
        observer: PipelineObserver,
        fatal_error: Arc<AtomicBool>,
    ) -> Result<(usize, usize)> {
        let output = FilterOutput::Http(http_sender);
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
    /// [sem N] → S3 stream → ChunkReader → decompress → lines → [line_ch] → filter workers → exporter
    /// ```
    ///
    /// Download tasks acquire a semaphore permit, then stream S3 chunks through
    /// a bounded channel into a synchronous decompressor (`spawn_blocking`).
    /// Lines are emitted into the line channel. The permit is held for the
    /// entire download+decompress duration (S3 connection stays open).
    /// On transient errors, range-based resume retries from the last byte offset.
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
                let source = Arc::new(obj_clone);
                let size = Self::download_and_stream(
                    &client, &source, source.clone(), tx, &config, &dl_obs, fe,
                )
                .await?;

                // Release permit AFTER streaming+decompress completes
                // (S3 connection was open throughout).
                drop(permit);
                Ok(size)
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

    /// Stream-decompress and emit lines into the channel.
    /// Runs inside `spawn_blocking` (synchronous IO).
    ///
    /// `reader` is any [`Read`] — either a `ChunkReader` (streaming from S3)
    /// or a `Cursor<Bytes>` (tests).
    ///
    /// Checks `fatal_error` every 1024 lines so we stop emitting early when the
    /// HTTP pipeline is dead — avoids blocking on a full line channel.
    fn emit_lines(
        reader: impl Read,
        source: &Arc<S3ObjectInfo>,
        line_tx: &flume::Sender<DecompressedLine>,
        fatal_error: Option<Arc<AtomicBool>>,
    ) -> Result<()> {
        let reader: Box<dyn Read> = if source.key.ends_with(".gz") {
            Box::new(flate2::read::GzDecoder::new(reader))
        } else if source.key.ends_with(".zst") || source.key.ends_with(".zstd") {
            Box::new(zstd::Decoder::new(reader)?)
        } else {
            Box::new(reader)
        };

        let mut buf_reader = BufReader::new(reader);
        let mut buf = Vec::new();
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

            buf.clear();
            if buf_reader.read_until(b'\n', &mut buf)? == 0 {
                break;
            }
            line_tx
                .send(DecompressedLine {
                    data: buf.clone(),
                    source: source.clone(),
                })
                .map_err(|_| anyhow::anyhow!("Filter workers gone, channel closed"))?;
            lines_emitted += 1;
        }

        Ok(())
    }

    /// Stream an S3 object's body directly into the decompressor via a bounded
    /// chunk channel, with range-based resume on transient errors.
    ///
    /// The decompressor (running in `spawn_blocking`) sees a seamless byte
    /// stream — retries are invisible because range requests resume the
    /// compressed stream exactly where it left off.
    ///
    /// Returns the total number of compressed bytes streamed.
    #[allow(clippy::too_many_arguments)]
    async fn download_and_stream(
        client: &Client,
        obj: &S3ObjectInfo,
        source: Arc<S3ObjectInfo>,
        line_tx: flume::Sender<DecompressedLine>,
        config: &StreamingDownloaderConfig,
        download_observer: &DownloadObserver,
        fatal_error: Option<Arc<AtomicBool>>,
    ) -> Result<usize> {
        // Bounded channel for async→sync chunk bridging.
        // Capacity 4 ≈ 256 KB of S3 chunks in flight (typical chunk ~64 KB).
        let (chunk_tx, chunk_rx) = flume::bounded::<Bytes>(4);

        // Spawn the synchronous decompressor side.
        let emit_source = source.clone();
        let emit_handle = tokio::task::spawn_blocking(move || {
            let reader = ChunkReader::new(chunk_rx);
            Self::emit_lines(reader, &emit_source, &line_tx, fatal_error)
        });

        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            bytes = obj.size,
            "Streaming download"
        );

        let mut bytes_forwarded: usize = 0;
        let mut succeeded = false;

        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                // Exponential backoff with simple jitter (±25%).
                let base = config
                    .initial_retry_delay
                    .mul_f64(2.0f64.powi(attempt as i32 - 1))
                    .min(Duration::from_secs(60));
                // Jitter: vary by ±25% using low bits of the current instant.
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos();
                let jitter_factor = 0.75 + (nanos % 500) as f64 / 1000.0;
                let delay = base.mul_f64(jitter_factor);

                warn!(
                    bucket = %obj.bucket,
                    key = %obj.key,
                    attempt,
                    bytes_forwarded,
                    retry_in_s = delay.as_secs_f64(),
                    "Retry scheduled (range resume)"
                );
                tokio::time::sleep(delay).await;
            }

            // Build GetObject request, adding Range header when resuming.
            let mut req = client
                .get_object()
                .bucket(&obj.bucket)
                .key(&obj.key);
            if bytes_forwarded > 0 {
                req = req.range(format!("bytes={bytes_forwarded}-"));
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("{e}");
                    if !s3::is_recoverable_s3_error(&msg) {
                        drop(chunk_tx);
                        // Abort the emit task — we won't send more data.
                        emit_handle.abort();
                        return Err(anyhow::anyhow!("Fatal S3 error: {e}"));
                    }
                    warn!(
                        bucket = %obj.bucket,
                        key = %obj.key,
                        attempt,
                        error = %e,
                        "S3 request failed"
                    );
                    continue;
                }
            };

            // Stream body chunks into the decompressor channel.
            let mut body = resp.body;
            let mut stream_failed = false;

            while let Some(chunk_result) = body.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        bytes_forwarded += chunk.len();
                        download_observer.add_bytes(chunk.len());

                        if chunk_tx.send_async(chunk).await.is_err() {
                            // Receiver dropped — emit_lines errored or was cancelled.
                            drop(chunk_tx);
                            return emit_handle
                                .await
                                .map_err(|e| {
                                    anyhow::anyhow!("Streaming emit task panic: {e}")
                                })?
                                .map(|()| bytes_forwarded);
                        }
                    }
                    Err(e) => {
                        warn!(
                            bucket = %obj.bucket,
                            key = %obj.key,
                            attempt,
                            bytes_forwarded,
                            error = %e,
                            "S3 body stream error"
                        );
                        stream_failed = true;
                        break;
                    }
                }
            }

            if !stream_failed {
                succeeded = true;
                break;
            }
        }

        // Drop sender to signal EOF to ChunkReader.
        drop(chunk_tx);

        if !succeeded {
            // Abort the emit task — partial data was sent but we can't finish.
            emit_handle.abort();
            return Err(anyhow::anyhow!(
                "S3 download failed after {} retries (streamed {bytes_forwarded} bytes): {}/{}",
                config.max_retries,
                obj.bucket,
                obj.key,
            ));
        }

        // Wait for the decompressor to finish.
        emit_handle
            .await
            .map_err(|e| anyhow::anyhow!("Streaming emit task panic: {e}"))??;

        debug!(
            bucket = %obj.bucket,
            key = %obj.key,
            compressed_bytes = bytes_forwarded,
            "Streamed"
        );

        Ok(bytes_forwarded)
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

                if searcher.matches_line(&line.data) {
                    match output.as_ref() {
                        FilterOutput::Http(sender) => {
                            sender
                                .send(line.data)
                                .map_err(|_| anyhow::anyhow!("HTTP consumer gone, channel closed"))?;
                        }
                        FilterOutput::File(writer) => {
                            writer.write_match(&line.source.prefix, &line.data)?;
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
