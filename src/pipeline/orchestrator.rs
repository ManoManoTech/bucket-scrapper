//! Pipeline orchestrator: download → decompress → filter → output.
//!
//! Downloads stream S3 response chunks directly into the decompressor via a
//! bounded channel (`ChunkReader`), avoiding full-object buffering.  Retries
//! resume mid-object using S3 range requests (`bytes=N-`).

use super::observer::{ChannelObserver, DownloadObserver};
use super::output::OutputSink;
use super::prefix_progress::PrefixProgress;
use crate::matcher::LineMatcher;
use crate::progress::PipelineProgress;
use crate::s3::{self, S3ObjectInfo};
use anyhow::Result;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

/// A single decompressed line tagged with its source file. Carries an
/// `Arc<PrefixProgress>` so the filter worker can increment `processed`
/// and trigger close-on-completion without a per-line map lookup.
struct DecompressedLine {
    data: Vec<u8>,
    source: Arc<S3ObjectInfo>,
    progress: Arc<PrefixProgress>,
}

/// Best-effort early close of a prefix's S3 upload once its downloads
/// have all completed and the channel has been fully drained for it.
/// Cheap-and-safe to call from both the sync filter-worker context and
/// the async download-task-completion context — `OutputSink::close_prefix`
/// is non-blocking (it does its own `spawn_blocking` internally for the
/// codec frame finalization).
///
/// If the close-ready condition isn't met yet, this is a no-op: the next
/// caller to bump a counter will re-evaluate. If the condition holds, the
/// `try_claim_close` CAS guarantees exactly one caller wins and invokes
/// `close_prefix`.
#[inline]
fn maybe_close_prefix(sink: &dyn OutputSink, progress: &PrefixProgress) {
    if !progress.is_drained() {
        return;
    }
    if progress.try_claim_close() {
        sink.close_prefix(&progress.name);
    }
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
    pub async fn search_objects(
        &self,
        objects: &[S3ObjectInfo],
        searcher: Arc<LineMatcher>,
        sink: Arc<dyn OutputSink>,
    ) -> Result<(usize, usize)> {
        let pipeline = sink.observer();
        let fatal_error = sink.fatal_error_flag();
        if objects.is_empty() {
            return Ok((0, 0));
        }

        // Sort objects by source prefix so the bounded download semaphore
        // clusters in-flight downloads on a small window of prefixes.
        // Combined with the close-on-completion machinery below, this caps
        // the number of simultaneously-open S3 uploads at the sink.
        let mut objects = objects.to_vec();
        objects.sort_by(|a, b| a.prefix.cmp(&b.prefix).then_with(|| a.key.cmp(&b.key)));

        // One `PrefixProgress` per distinct source prefix in the run, built
        // once and threaded through the pipeline via `DecompressedLine`.
        // Lookup map is consulted only at download-dispatch time (small N,
        // not per line). Vec preserves the canonical iteration order for
        // end-of-run convergence assertions.
        //
        // **Important**: `downloads_pending[p]` is initialized to the *total*
        // object count for the prefix up-front, then decremented as each
        // download task exits. If we instead incremented per-dispatch
        // (`fetch_add(1)` before each spawn), there would be a window
        // between iter N and iter N+1 where a fast-finishing D1 could
        // bring downloads_pending to 0 *before* D2's dispatch, letting a
        // filter worker observe "drained" and fire close_prefix
        // prematurely — the next line would re-open with seq+1.
        let mut progress_vec: Vec<Arc<PrefixProgress>> = Vec::new();
        let mut progress_lookup: HashMap<String, Arc<PrefixProgress>> = HashMap::new();
        for obj in &objects {
            let entry = progress_lookup
                .entry(obj.prefix.clone())
                .or_insert_with(|| {
                    let p = PrefixProgress::new(obj.prefix.clone());
                    progress_vec.push(p.clone());
                    p
                });
            entry.downloads_pending.fetch_add(1, Ordering::Relaxed);
        }

        let total_bytes: usize = objects.iter().map(|o| o.size).sum();
        info!(
            objects = objects.len(),
            prefixes = progress_vec.len(),
            mb = total_bytes / 1_000_000,
            download_concurrency = self.config.max_concurrent_downloads,
            filter_workers = self.config.filter_tasks,
            line_buffer = self.config.line_buffer_size,
            "Starting search"
        );

        // Line channel between download+decompress and filter workers
        let (line_tx, line_rx) = flume::bounded::<DecompressedLine>(self.config.line_buffer_size);

        let download_observer = DownloadObserver::new();
        let match_count = Arc::new(AtomicUsize::new(0));
        let match_bytes = Arc::new(AtomicUsize::new(0));
        let filter_lines_in = Arc::new(AtomicUsize::new(0));
        let filter_bytes_in = Arc::new(AtomicUsize::new(0));
        let workers_alive = Arc::new(AtomicUsize::new(self.config.filter_tasks));
        // Sink-agnostic "how many filter workers are inside sink.ingest
        // right now" gauge. Filter workers bump it via a RAII guard
        // around each `sink.ingest` call; the progress reporter samples
        // it instantaneously to distinguish `filter` vs `sink_*` labels.
        let workers_in_ingest: crate::progress::IngestGauge = Arc::new(AtomicUsize::new(0));
        let sink_obs = sink.sink_observability();
        let sink_kind = sink.type_name();

        let progress = Arc::new(Mutex::new(PipelineProgress::new(
            objects.len(),
            total_bytes,
            self.config.progress_interval,
            pipeline,
            ChannelObserver::from_sender(&line_tx),
            download_observer.clone(),
            match_count.clone(),
            match_bytes.clone(),
            filter_lines_in.clone(),
            filter_bytes_in.clone(),
            workers_alive.clone(),
            self.config.filter_tasks,
            workers_in_ingest.clone(),
            sink_obs,
            sink_kind,
        )));

        // Emit initial progress at t=0 so charts always have a starting point
        {
            let mut prog = progress.lock().await;
            prog.report();
        }

        // --- Spawn download coordinator ---
        let mut download_handle = {
            let client = self.client.clone();
            let config = self.config.clone();
            let semaphore = self.download_semaphore.clone();
            let objects = objects.clone();
            let progress_lookup = progress_lookup.clone();
            let tx = line_tx;
            let progress = progress.clone();
            let fe = fatal_error.clone();
            let sink = sink.clone();

            tokio::spawn(async move {
                let result = Self::download_coordinator(
                    client,
                    &objects,
                    progress_lookup,
                    sink,
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
        let mut worker_set: JoinSet<Result<usize>> = JoinSet::new();

        for worker_id in 0..self.config.filter_tasks {
            let rx = line_rx.clone();
            let searcher = searcher.clone();
            let sink = sink.clone();
            let match_count = match_count.clone();
            let match_bytes = match_bytes.clone();
            let filter_lines_in = filter_lines_in.clone();
            let filter_bytes_in = filter_bytes_in.clone();
            let fe = fatal_error.clone();
            let wa = workers_alive.clone();
            let wii = workers_in_ingest.clone();

            worker_set.spawn(async move {
                let result = Self::filter_worker(
                    worker_id,
                    rx,
                    searcher,
                    sink,
                    match_count,
                    match_bytes,
                    filter_lines_in,
                    filter_bytes_in,
                    fe,
                    wii,
                )
                .await;
                wa.fetch_sub(1, Ordering::Relaxed);
                match &result {
                    Ok(matches) => {
                        info!(worker = worker_id, matches, "Filter worker exited");
                    }
                    Err(e) => {
                        warn!(worker = worker_id, error = %e, "Filter worker failed");
                    }
                }
                result
            });
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

        // --- Join download coordinator + watch for early worker death ---
        // Use select! so that if any filter worker dies before the download
        // coordinator finishes, we detect it immediately instead of deadlocking
        // (the coordinator would block on a full line channel forever).
        //
        // Invariant: if all workers exit before the coordinator, the line
        // channel's receivers are gone, so the next `emit_lines` send returns
        // SendError, which `download_coordinator` propagates as Err (see
        // `emit_lines` — "Filter workers gone, channel closed"). The
        // coordinator therefore exits promptly; this loop will not hang.
        let mut download_done = false;
        let mut total_matches = 0usize;

        let total_workers = self.config.filter_tasks;
        let mut workers_finished = 0usize;

        loop {
            tokio::select! {
                dl_result = &mut download_handle, if !download_done => {
                    match dl_result {
                        Ok(Ok(files_processed)) => {
                            debug!(files = files_processed, "Download coordinator finished");
                            download_done = true;
                        }
                        Ok(Err(e)) => {
                            progress_ticker.abort();
                            worker_set.abort_all();
                            return Err(e);
                        }
                        Err(e) => {
                            progress_ticker.abort();
                            worker_set.abort_all();
                            return Err(anyhow::anyhow!("Download coordinator panicked: {e}"));
                        }
                    }
                }
                Some(worker_result) = worker_set.join_next() => {
                    workers_finished += 1;
                    match worker_result {
                        Ok(Ok(matches)) => {
                            total_matches += matches;
                        }
                        Ok(Err(e)) => {
                            progress_ticker.abort();
                            download_handle.abort();
                            return Err(e);
                        }
                        Err(e) => {
                            progress_ticker.abort();
                            download_handle.abort();
                            return Err(anyhow::anyhow!("Filter worker panicked: {e}"));
                        }
                    }
                    if workers_finished == total_workers && !download_done {
                        warn!("All filter workers exited before download coordinator finished");
                    }
                }
            }

            if download_done && workers_finished == total_workers {
                break;
            }
        }

        // Stop the progress ticker
        progress_ticker.abort();

        // I4: at end of run every prefix's `sent == processed` and every
        // prefix was either closed early or will be closed by sink.finish().
        // Surfaces counter drift bugs early; if this fires we know the
        // close-on-completion accounting is off, not just a sink-side leak.
        for p in &progress_vec {
            let sent = p.sent.load(Ordering::Relaxed);
            let processed = p.processed.load(Ordering::Relaxed);
            debug_assert_eq!(
                sent, processed,
                "PrefixProgress mismatch at end of run: prefix={}, sent={}, processed={}",
                p.name, sent, processed,
            );
            let pending = p.downloads_pending.load(Ordering::Relaxed);
            debug_assert_eq!(
                pending, 0,
                "PrefixProgress.downloads_pending != 0 at end of run: prefix={}, pending={}",
                p.name, pending,
            );
        }

        // Emit a final progress report so the last log line carries the accurate
        // end-of-run totals (filter input volume, matched ratios, etc.) — useful
        // for short runs where the periodic ticker may only have fired at t=0.
        let files_searched = {
            let mut prog = progress.lock().await;
            prog.report();
            prog.files_processed
        };

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
        progress_lookup: HashMap<String, Arc<PrefixProgress>>,
        sink: Arc<dyn OutputSink>,
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
            fe.as_ref().is_some_and(|f| f.load(Ordering::Relaxed))
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
            // `downloads_pending` was pre-incremented once per object in
            // the run-startup pass above — don't double-count here. The
            // spawned task owns the matching `fetch_sub` in all exit
            // paths.
            let prefix_progress = progress_lookup
                .get(&obj.prefix)
                .cloned()
                .expect("progress_lookup built from same objects");
            let sink_for_task = sink.clone();

            join_set.spawn(async move {
                let source = Arc::new(obj_clone);
                let result = Self::download_and_stream(
                    &client,
                    &source,
                    source.clone(),
                    tx,
                    &config,
                    &dl_obs,
                    fe,
                    prefix_progress.clone(),
                )
                .await;

                // Release permit AFTER streaming+decompress completes
                // (S3 connection was open throughout).
                drop(permit);

                // Decrement downloads_pending unconditionally — even on
                // error this prefix has one fewer in-flight download.
                // Use `compare_exchange`-via-fetch_sub semantics: assert
                // we don't underflow (which would indicate a counter bug).
                let prev = prefix_progress
                    .downloads_pending
                    .fetch_sub(1, Ordering::Relaxed);
                debug_assert!(
                    prev > 0,
                    "downloads_pending underflow for prefix {}",
                    prefix_progress.name
                );
                // If this was the last download for the prefix and all
                // lines are processed, fire the early close.
                maybe_close_prefix(sink_for_task.as_ref(), &prefix_progress);

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
        progress: &Arc<PrefixProgress>,
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
            // Bump `sent` before send so the filter worker's `processed`
            // increment never observes a stale `sent` value lower than
            // its own count. Both counters use Relaxed; the channel
            // send/recv establishes happens-before across stages.
            progress.sent.fetch_add(1, Ordering::Relaxed);
            line_tx
                .send(DecompressedLine {
                    data: buf.clone(),
                    source: source.clone(),
                    progress: progress.clone(),
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
        progress: Arc<PrefixProgress>,
    ) -> Result<usize> {
        // Bounded channel for async→sync chunk bridging.
        // Capacity 4 ≈ 256 KB of S3 chunks in flight (typical chunk ~64 KB).
        let (chunk_tx, chunk_rx) = flume::bounded::<Bytes>(4);

        // Spawn the synchronous decompressor side.
        let emit_source = source.clone();
        let emit_progress = progress.clone();
        let emit_handle = tokio::task::spawn_blocking(move || {
            let reader = ChunkReader::new(chunk_rx);
            Self::emit_lines(reader, &emit_source, &line_tx, fatal_error, &emit_progress)
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
            let mut req = client.get_object().bucket(&obj.bucket).key(&obj.key);
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
                                .map_err(|e| anyhow::anyhow!("Streaming emit task panic: {e}"))?
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
    #[allow(clippy::too_many_arguments)]
    async fn filter_worker(
        worker_id: usize,
        rx: flume::Receiver<DecompressedLine>,
        searcher: Arc<LineMatcher>,
        sink: Arc<dyn OutputSink>,
        match_count: Arc<AtomicUsize>,
        match_bytes: Arc<AtomicUsize>,
        filter_lines_in: Arc<AtomicUsize>,
        filter_bytes_in: Arc<AtomicUsize>,
        fatal_error: Option<Arc<AtomicBool>>,
        workers_in_ingest: crate::progress::IngestGauge,
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

                let line_len = line.data.len();
                filter_lines_in.fetch_add(1, Ordering::Relaxed);
                filter_bytes_in.fetch_add(line_len, Ordering::Relaxed);

                if searcher.matches_line(&line.data) {
                    // RAII gauge so the progress reporter can tell whether
                    // filter workers are stuck inside the sink (codec /
                    // mpsc send / I/O) or actually doing filter-side work.
                    // Drop on the same line frees the slot regardless of
                    // panics / early returns.
                    let _ingest_guard = crate::progress::IngestGuard::new(&workers_in_ingest);
                    sink.ingest(&line.source.prefix, &line.data)?;
                    local += 1;
                    match_count.fetch_add(1, Ordering::Relaxed);
                    match_bytes.fetch_add(line_len, Ordering::Relaxed);
                }

                // Bump `processed` AFTER ingest — establishes
                // happens-before from this line's ingest to any
                // close-on-completion that subsequently observes
                // sent==processed. Worker that brings processed to sent
                // is the worker that just finished its ingest; all
                // earlier workers' processed bumps already happened,
                // which means their ingests already completed too.
                // Swap was diagnosed against an off-by-one upload race
                // in `s3_output_end_to_end_*` where a fast worker's
                // close_prefix landed between another worker's lock
                // acquisition and write, causing a spurious re-open
                // with seq+1.
                let old_processed = line.progress.processed.fetch_add(1, Ordering::Relaxed);
                debug_assert!(
                    line.progress.sent.load(Ordering::Relaxed) > old_processed,
                    "processed ({}) overran sent for prefix {}",
                    old_processed + 1,
                    line.progress.name,
                );

                // Trigger close-on-completion if this was the last line
                // for the prefix and all downloads have already exited.
                // CAS guards against double-close from racing workers.
                maybe_close_prefix(sink.as_ref(), &line.progress);
            }
            Ok(local)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Filter worker panic: {e}"))??;

        debug!(
            worker = worker_id,
            matches = result,
            "Filter worker finished"
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::thread;

    #[test]
    fn chunk_reader_returns_eof_when_sender_dropped() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        drop(tx);
        let mut reader = ChunkReader::new(rx);
        let mut buf = [0u8; 16];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn chunk_reader_reads_single_chunk_in_one_call() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        tx.send(Bytes::from_static(b"hello")).unwrap();
        drop(tx);
        let mut reader = ChunkReader::new(rx);
        let mut buf = [0u8; 16];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
        // Next read hits EOF.
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn chunk_reader_serves_remainder_when_buf_smaller_than_chunk() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        tx.send(Bytes::from_static(b"abcdef")).unwrap();
        drop(tx);
        let mut reader = ChunkReader::new(rx);
        let mut buf = [0u8; 2];
        assert_eq!(reader.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"ab");
        assert_eq!(reader.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"cd");
        assert_eq!(reader.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"ef");
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn chunk_reader_concatenates_multiple_chunks_via_bufreader_lines() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        // Producer thread: split a multi-line payload across chunk boundaries
        // that don't align with newlines, exercising the remainder path.
        let producer = thread::spawn(move || {
            tx.send(Bytes::from_static(b"line-one\nlin")).unwrap();
            tx.send(Bytes::from_static(b"e-two\nline-th")).unwrap();
            tx.send(Bytes::from_static(b"ree\n")).unwrap();
            // tx dropped here -> EOF
        });

        let reader = ChunkReader::new(rx);
        let lines: Vec<String> = BufReader::new(reader)
            .lines()
            .collect::<io::Result<_>>()
            .unwrap();
        producer.join().unwrap();
        assert_eq!(lines, vec!["line-one", "line-two", "line-three"]);
    }

    /// Documents a quirk: an empty `Bytes` chunk mid-stream is currently
    /// treated as EOF by `ChunkReader::read` (since `remainder` stays empty
    /// and `n = 0`, which `Read` callers interpret as EOF). S3 streams don't
    /// emit empty chunks in practice, so this hasn't bitten us — but if it
    /// ever does, the fix is to loop on empty `recv()` results.
    #[test]
    fn chunk_reader_treats_empty_chunk_as_premature_eof() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        tx.send(Bytes::from_static(b"foo")).unwrap();
        tx.send(Bytes::new()).unwrap();
        tx.send(Bytes::from_static(b"bar")).unwrap();
        drop(tx);
        let mut reader = ChunkReader::new(rx);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(
            out, b"foo",
            "current behavior: empty chunk short-circuits to EOF"
        );
    }
}
