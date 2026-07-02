//! Pipeline orchestrator: download → decompress → filter → output.
//!
//! Downloads stream S3 response chunks directly into the decompressor via a
//! bounded channel (`ChunkReader`), avoiding full-object buffering.  Retries
//! resume mid-object using S3 range requests (`bytes=N-`).

use super::mem_pool::InputBufferPool;
use super::observer::{ChannelObserver, DownloadObserver, InputWaitGuard, ReadPathMetrics};
use super::output::OutputSink;
use super::prefix_progress::PrefixProgress;
use crate::control::server::{ControlContext, StatusHandles};
use crate::control::RuntimeControls;
use crate::matcher::LineMatcher;
use crate::progress::PipelineProgress;
use crate::s3::{self, S3ObjectInfo};
use anyhow::Result;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
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
    /// Read-path gauges: this is the B2 (decode-input) consumer, so it
    /// decrements `b2_used_bytes` as it pulls chunks and marks
    /// `decoders_input_wait` while blocked waiting for input.
    metrics: Arc<ReadPathMetrics>,
}

impl ChunkReader {
    fn new(rx: flume::Receiver<Bytes>, metrics: Arc<ReadPathMetrics>) -> Self {
        Self {
            rx,
            remainder: Bytes::new(),
            metrics,
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Serve leftover bytes from the previous chunk first.
        if self.remainder.is_empty() {
            // While blocked here the decoder is starved of input (B2 empty) —
            // the signal that distinguishes upstream-bound from decompress-bound.
            let chunk = {
                let _wait = InputWaitGuard::new(&self.metrics.decoders_input_wait);
                match self.rx.recv() {
                    Ok(chunk) => chunk,
                    // Sender dropped — EOF.
                    Err(_) => return Ok(0),
                }
            };
            // These bytes have left B2 and are now being decompressed.
            self.metrics
                .b2_used_bytes
                .fetch_sub(chunk.len() as u64, Ordering::Relaxed);
            self.remainder = chunk;
        }
        let n = buf.len().min(self.remainder.len());
        buf[..n].copy_from_slice(&self.remainder[..n]);
        self.remainder = self.remainder.slice(n..);
        Ok(n)
    }
}

/// Forward bytes into the B2 (decode-input) channel, accounting the occupancy.
/// Increments `b2_used_bytes` **before** the send so the consumer's matching
/// `fetch_sub` can't underflow. Returns `Err` if the receiver is gone.
async fn forward_to_decoder(
    chunk_tx: &flume::Sender<Bytes>,
    chunk: Bytes,
    metrics: &ReadPathMetrics,
) -> std::result::Result<(), flume::SendError<Bytes>> {
    metrics
        .b2_used_bytes
        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
    chunk_tx.send_async(chunk).await
}

/// Outcome of a producer (single-stream or chunked reassembler) feeding the
/// decoder. `ReceiverGone` means the decoder side dropped its receiver (it
/// errored/cancelled) — the caller awaits the emit task to surface that error.
enum Produced {
    Done(usize),
    ReceiverGone(usize),
}

/// RAII guard that bumps the `dl_active` (in-flight range GETs) gauge for the
/// duration of a GET, decrementing on every exit path.
struct ActiveGetGuard(Arc<AtomicUsize>);

impl ActiveGetGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for ActiveGetGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Configuration for the streaming downloader
#[derive(Clone)]
pub struct StreamingDownloaderConfig {
    /// Max concurrent S3 range GETs (chunks). One whole object counts as one
    /// "range" when chunking is off, so small-file behavior is unchanged.
    pub max_concurrent_downloads: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
    pub progress_interval: Duration,
    /// Number of filter worker tasks (default: cpu_count / 2)
    pub filter_tasks: usize,
    /// Line channel capacity between download+decompress and filter workers
    /// (RAM ≈ this × ~200 bytes avg line)
    pub line_buffer_size: usize,
    /// Chunk size in bytes for parallel ranged download. `None` disables
    /// chunking (one streamed GET per object — the original path). Objects
    /// `≤ chunk_size` also take the single-stream path.
    pub chunk_size: Option<usize>,
    /// Max concurrent live file tasks (decoders). Bounds decoder threads and
    /// the number of in-order reassemblers.
    pub file_slots: usize,
    /// B1 capacity: total resident chunk bytes the input-buffer pool admits.
    pub max_input_buffer_bytes: usize,
    /// B2 capacity per file: decoder-input channel size in bytes.
    pub decode_input_buffer_bytes: usize,
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
            chunk_size: None,
            file_slots: 32,
            max_input_buffer_bytes: 4096 * 1_000_000,
            decode_input_buffer_bytes: 128 * 1_000_000,
        }
    }
}

/// Downloads S3 objects, stream-decompresses them into lines, and feeds them
/// to filter workers that apply regex matching.
pub struct StreamingDownloader {
    client: Client,
    config: StreamingDownloaderConfig,
    /// Live tuning state: the download + file semaphores, the filter-retire
    /// counter, and the active part size. Shared with the control server.
    controls: Arc<RuntimeControls>,
    /// B1 input-buffer pool. Always present so part size can be enabled at
    /// runtime even if the run started with chunking off — the pool is just a
    /// byte-counting semaphore and holds no buffers until reservations happen.
    pool: Arc<InputBufferPool>,
    /// Optional control-socket path; when set, `search_objects` spawns the
    /// UDS control server for the duration of the run.
    control_socket: Option<PathBuf>,
}

impl StreamingDownloader {
    pub fn new(client: Client, config: StreamingDownloaderConfig) -> Self {
        let controls = RuntimeControls::new(
            config.file_slots,
            config.max_concurrent_downloads,
            config.chunk_size.unwrap_or(0),
        );
        let pool = InputBufferPool::new(config.max_input_buffer_bytes);

        Self {
            client,
            config,
            controls,
            pool,
            control_socket: None,
        }
    }

    /// Enable the runtime control socket at `path` for the next run. No-op
    /// when `path` is `None` (the default — zero overhead).
    pub fn with_control_socket(mut self, path: Option<PathBuf>) -> Self {
        self.control_socket = path;
        self
    }

    /// Shared tuning handle, e.g. for tests or an in-process tuner.
    pub fn controls(&self) -> Arc<RuntimeControls> {
        self.controls.clone()
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
        let total_chunks: usize = objects
            .iter()
            .map(|o| Self::chunk_count(o.size, self.config.chunk_size))
            .sum();
        info!(
            objects = objects.len(),
            chunks = total_chunks,
            prefixes = progress_vec.len(),
            mb = total_bytes / 1_000_000,
            range_get_concurrency = self.config.max_concurrent_downloads,
            file_slots = self.config.file_slots,
            chunk_size_mb = self.config.chunk_size.map(|c| c / 1_000_000),
            filter_workers = self.config.filter_tasks,
            line_buffer = self.config.line_buffer_size,
            "Starting search"
        );

        // Shared read-path gauges (chunked-download + buffering metrics).
        let b2_capacity =
            (self.config.decode_input_buffer_bytes as u64) * self.config.file_slots as u64;
        let metrics = ReadPathMetrics::new(b2_capacity, Some(self.pool.clone()));
        metrics
            .chunks_remaining
            .store(total_chunks, Ordering::Relaxed);

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
            metrics.clone(),
        )));

        // Emit initial progress at t=0 so charts always have a starting point
        {
            let mut prog = progress.lock().await;
            prog.report();
        }

        // Observer for the control server's `status` (a second WeakSender view
        // of the line channel — must be built before `line_tx` is moved).
        let control_line_obs = ChannelObserver::from_sender(&line_tx);

        // --- Spawn download coordinator ---
        let mut download_handle = {
            let client = self.client.clone();
            let config = self.config.clone();
            let semaphore = self.controls.download_semaphore.clone();
            let file_semaphore = self.controls.file_semaphore.clone();
            let chunk_size = self.controls.chunk_size.clone();
            let pool = self.pool.clone();
            let objects = objects.clone();
            let progress_lookup = progress_lookup.clone();
            let tx = line_tx;
            let progress = progress.clone();
            let fe = fatal_error.clone();
            let sink = sink.clone();
            let metrics = metrics.clone();
            let dl_obs = download_observer.clone();

            tokio::spawn(async move {
                let result = Self::download_coordinator(
                    client,
                    &objects,
                    progress_lookup,
                    sink,
                    config,
                    semaphore,
                    file_semaphore,
                    chunk_size,
                    pool,
                    tx,
                    dl_obs,
                    progress,
                    fe,
                    metrics,
                )
                .await;
                // tx is dropped here → channel closes → workers drain and exit
                result
            })
        };

        // --- Spawn filter workers ---
        // Workers can be added at runtime (control plane), so factor the spawn
        // into a macro reused by the startup loop and the join-loop grow arm.
        // Each spawn clones the shared handles and bumps `workers_alive`; the
        // worker decrements it on exit. `worker_id` is monotonic across the run
        // (never reused) so log lines stay unambiguous.
        let mut worker_set: JoinSet<Result<usize>> = JoinSet::new();
        let filter_retire = self.controls.filter_retire.clone();

        macro_rules! spawn_filter_worker {
            ($worker_id:expr) => {{
                let worker_id = $worker_id;
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
                let retire = filter_retire.clone();
                wa.fetch_add(1, Ordering::Relaxed);
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
                        retire,
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
            }};
        }

        // `workers_alive` was pre-seeded to `filter_tasks` (so the t=0 progress
        // report reads right); the macro also bumps it, so reset to 0 first and
        // let the startup spawns set the true count.
        workers_alive.store(0, Ordering::Relaxed);
        let mut next_worker_id = 0usize;
        let mut workers_spawned = 0usize;
        for _ in 0..self.config.filter_tasks {
            spawn_filter_worker!(next_worker_id);
            next_worker_id += 1;
            workers_spawned += 1;
        }

        // Channel for runtime "grow filter workers by N" requests from the
        // control server; handled in the join loop below (it owns `worker_set`).
        let (grow_workers_tx, grow_workers_rx) = flume::unbounded::<usize>();

        // --- Spawn the control server (only if a socket was configured) ---
        // NOTE: we deliberately keep `line_rx` alive past here so the grow arm
        // can clone a fresh receiver for new workers. This means the implicit
        // "all workers died ⇒ coordinator sees SendError" safety net no longer
        // fires (a live receiver remains), so the join loop explicitly aborts
        // the coordinator if every worker exits before download completes.
        let control_server = self.control_socket.clone().map(|socket_path| {
            let ctx = ControlContext::new(
                self.controls.clone(),
                grow_workers_tx,
                StatusHandles {
                    workers_alive: workers_alive.clone(),
                    metrics: metrics.clone(),
                    download_observer: download_observer.clone(),
                    filter_bytes_in: filter_bytes_in.clone(),
                    match_count: match_count.clone(),
                    workers_in_ingest: workers_in_ingest.clone(),
                    sink_obs: sink.sink_observability(),
                    sink_kind: sink.type_name(),
                    line_channel: control_line_obs,
                    line_buffer_size: self.config.line_buffer_size,
                },
            );
            tokio::spawn(async move {
                if let Err(e) = crate::control::server::serve(socket_path, ctx).await {
                    warn!(error = %e, "Control server exited with error");
                }
            })
        });

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
        // `workers_spawned` is dynamic: the control plane can grow the pool at
        // runtime via `grow_workers_rx`. The run is done when the coordinator
        // has finished AND every worker ever spawned has exited.
        //
        // Because we keep `line_rx` alive (to clone for runtime-spawned
        // workers), the old "all workers died ⇒ coordinator hits SendError"
        // safety net no longer fires — a live receiver remains. So if every
        // worker exits before download completes, we abort the coordinator
        // explicitly here rather than waiting for a send error that won't come.
        let mut download_done = false;
        let mut total_matches = 0usize;
        let mut workers_finished = 0usize;
        let mut can_grow = true;

        let run_result: Result<()> = loop {
            tokio::select! {
                dl_result = &mut download_handle, if !download_done => {
                    match dl_result {
                        Ok(Ok(files_processed)) => {
                            debug!(files = files_processed, "Download coordinator finished");
                            download_done = true;
                        }
                        Ok(Err(e)) => break Err(e),
                        Err(e) => break Err(anyhow::anyhow!("Download coordinator panicked: {e}")),
                    }
                }
                grow = grow_workers_rx.recv_async(), if can_grow && !download_done => {
                    match grow {
                        Ok(n) => {
                            for _ in 0..n {
                                spawn_filter_worker!(next_worker_id);
                                next_worker_id += 1;
                                workers_spawned += 1;
                            }
                            info!(added = n, total_spawned = workers_spawned, "Filter workers added");
                        }
                        // All grow senders dropped (no control server / run ending):
                        // disable this arm so the select doesn't busy-spin on Err.
                        Err(_) => { can_grow = false; }
                    }
                }
                Some(worker_result) = worker_set.join_next() => {
                    workers_finished += 1;
                    match worker_result {
                        Ok(Ok(matches)) => {
                            total_matches += matches;
                        }
                        Ok(Err(e)) => {
                            download_handle.abort();
                            break Err(e);
                        }
                        Err(e) => {
                            download_handle.abort();
                            break Err(anyhow::anyhow!("Filter worker panicked: {e}"));
                        }
                    }
                    if workers_finished == workers_spawned && !download_done {
                        // All workers have exited. The normal end-of-run shape:
                        // the coordinator dropped `tx`, closing the channel, so
                        // workers drained and returned — `select!` just observed
                        // their completions before the coordinator's result.
                        // We can't rely on the coordinator hitting SendError to
                        // unblock (a spare `line_rx` is held open for runtime
                        // worker growth), so await its result directly. The
                        // timeout bounds a genuinely wedged coordinator so the
                        // run can't hang.
                        match tokio::time::timeout(Duration::from_secs(30), &mut download_handle)
                            .await
                        {
                            Ok(Ok(Ok(files_processed))) => {
                                debug!(files = files_processed, "Download coordinator finished");
                                download_done = true;
                            }
                            Ok(Ok(Err(e))) => break Err(e),
                            Ok(Err(e)) => {
                                break Err(anyhow::anyhow!("Download coordinator panicked: {e}"))
                            }
                            Err(_) => {
                                warn!("All filter workers exited before download coordinator finished");
                                download_handle.abort();
                                break Err(anyhow::anyhow!(
                                    "All filter workers exited before downloads completed"
                                ));
                            }
                        }
                    }
                }
            }

            if download_done && workers_finished == workers_spawned {
                break Ok(());
            }
        };

        // Cleanup on every path: stop the ticker, the control server, and
        // release our retained line receiver.
        progress_ticker.abort();
        if let Some(handle) = &control_server {
            handle.abort();
        }
        drop(line_rx);
        run_result?;

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
        file_semaphore: Arc<Semaphore>,
        chunk_size: Arc<AtomicUsize>,
        pool: Arc<InputBufferPool>,
        line_tx: flume::Sender<DecompressedLine>,
        download_observer: DownloadObserver,
        progress: Arc<Mutex<PipelineProgress>>,
        fatal_error: Option<Arc<AtomicBool>>,
        metrics: Arc<ReadPathMetrics>,
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

            // Acquire a FILE slot before spawn — bounds live decoders. Range-GET
            // concurrency is bounded separately by `semaphore`, acquired per
            // chunk inside `download_and_stream`.
            let file_permit = file_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("File semaphore closed: {e}"))?;

            drain_completed!();

            let obj_clone = obj.clone();
            let client = client.clone();
            let config = config.clone();
            let dl_obs = download_observer.clone();
            let tx = line_tx.clone();
            let fe = fatal_error.clone();
            let dl_sem = semaphore.clone();
            let pool = pool.clone();
            let metrics = metrics.clone();
            // Snapshot the (live-tunable) part size once per object so the
            // chunk/single decision and the chunk math stay consistent even if
            // an operator retunes mid-run.
            let cur_chunk = chunk_size.load(Ordering::Relaxed);
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
                metrics.files_in_flight.fetch_add(1, Ordering::Relaxed);
                let result = Self::download_and_stream(
                    &client,
                    &source,
                    source.clone(),
                    tx,
                    &config,
                    cur_chunk,
                    &dl_obs,
                    fe,
                    prefix_progress.clone(),
                    dl_sem,
                    pool,
                    metrics.clone(),
                )
                .await;
                metrics.files_in_flight.fetch_sub(1, Ordering::Relaxed);

                // Release the file slot AFTER streaming+decompress completes.
                drop(file_permit);

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
    /// How many chunks an object splits into under the given chunking config.
    /// `None` / objects `≤ chunk_size` → 1 (single-stream path).
    fn chunk_count(size: usize, chunk_size: Option<usize>) -> usize {
        match chunk_size {
            Some(cs) if cs > 0 && size > cs => size.div_ceil(cs),
            _ => 1,
        }
    }

    /// Retry backoff delay for `attempt` (1-based), exp + ±25% jitter, ≤ 60 s.
    fn retry_delay(config: &StreamingDownloaderConfig, attempt: u32) -> Duration {
        let base = config
            .initial_retry_delay
            .mul_f64(2.0f64.powi(attempt as i32 - 1))
            .min(Duration::from_secs(60));
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let jitter_factor = 0.75 + (nanos % 500) as f64 / 1000.0;
        base.mul_f64(jitter_factor)
    }

    /// Set up the decoder side (shared by both producers) and dispatch to the
    /// single-stream or chunked-reassembler producer based on config + size.
    #[allow(clippy::too_many_arguments)]
    async fn download_and_stream(
        client: &Client,
        obj: &S3ObjectInfo,
        source: Arc<S3ObjectInfo>,
        line_tx: flume::Sender<DecompressedLine>,
        config: &StreamingDownloaderConfig,
        chunk_size: usize,
        download_observer: &DownloadObserver,
        fatal_error: Option<Arc<AtomicBool>>,
        progress: Arc<PrefixProgress>,
        dl_sem: Arc<Semaphore>,
        pool: Arc<InputBufferPool>,
        metrics: Arc<ReadPathMetrics>,
    ) -> Result<usize> {
        // `chunk_size` is a per-object snapshot of the live part-size knob
        // (`0` ⇒ chunking disabled). The B1 pool is always present, so the
        // decision rests solely on the part size vs. object size.
        let chunked = chunk_size > 0 && obj.size > chunk_size;

        // B2 (decode-input) channel. Chunked items are whole ranges, so size by
        // how many ranges fit the per-file decode-input budget; the single
        // stream uses ~64 KB SDK chunks, so keep the original small capacity.
        let chunk_cap = if chunked {
            (config.decode_input_buffer_bytes / chunk_size).max(2)
        } else {
            4
        };
        let (chunk_tx, chunk_rx) = flume::bounded::<Bytes>(chunk_cap);

        let emit_source = source.clone();
        let emit_progress = progress.clone();
        let emit_metrics = metrics.clone();
        let emit_handle = tokio::task::spawn_blocking(move || {
            let reader = ChunkReader::new(chunk_rx, emit_metrics);
            Self::emit_lines(reader, &emit_source, &line_tx, fatal_error, &emit_progress)
        });

        debug!(bucket = %obj.bucket, key = %obj.key, bytes = obj.size, chunked, "Streaming download");

        let produced = if chunked {
            Self::stream_chunked(
                client,
                obj,
                config,
                chunk_size,
                download_observer,
                &chunk_tx,
                &dl_sem,
                &pool,
                &metrics,
            )
            .await
        } else {
            Self::stream_single(
                client,
                obj,
                config,
                download_observer,
                &chunk_tx,
                &dl_sem,
                &metrics,
            )
            .await
        };

        // Drop the sender to signal EOF to the decoder before joining it.
        drop(chunk_tx);

        match produced {
            Ok(Produced::Done(bytes)) => {
                emit_handle
                    .await
                    .map_err(|e| anyhow::anyhow!("Streaming emit task panic: {e}"))??;
                Ok(bytes)
            }
            Ok(Produced::ReceiverGone(bytes)) => {
                // The decoder dropped its receiver — surface its error.
                emit_handle
                    .await
                    .map_err(|e| anyhow::anyhow!("Streaming emit task panic: {e}"))?
                    .map(|()| bytes)
            }
            Err(e) => {
                emit_handle.abort();
                Err(e)
            }
        }
    }

    /// Original single-stream producer: one open-ended GET, range-resume on
    /// transient errors, forwarding SDK chunks straight into the decoder.
    /// Holds one download-semaphore permit for the whole stream.
    #[allow(clippy::too_many_arguments)]
    async fn stream_single(
        client: &Client,
        obj: &S3ObjectInfo,
        config: &StreamingDownloaderConfig,
        download_observer: &DownloadObserver,
        chunk_tx: &flume::Sender<Bytes>,
        dl_sem: &Arc<Semaphore>,
        metrics: &ReadPathMetrics,
    ) -> Result<Produced> {
        let _permit = dl_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| anyhow::anyhow!("Download semaphore closed: {e}"))?;
        let _dl_guard = ActiveGetGuard::new(metrics.dl_active.clone());

        let mut bytes_forwarded: usize = 0;
        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(Self::retry_delay(config, attempt)).await;
            }
            let mut req = client.get_object().bucket(&obj.bucket).key(&obj.key);
            if bytes_forwarded > 0 {
                req = req.range(format!("bytes={bytes_forwarded}-"));
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("{e}");
                    if !s3::is_recoverable_s3_error(&msg) {
                        return Err(anyhow::anyhow!("Fatal S3 error: {e}"));
                    }
                    warn!(bucket = %obj.bucket, key = %obj.key, attempt, error = %e, "S3 request failed");
                    continue;
                }
            };
            let mut body = resp.body;
            let mut stream_failed = false;
            while let Some(chunk_result) = body.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        bytes_forwarded += chunk.len();
                        download_observer.add_bytes(chunk.len());
                        if forward_to_decoder(chunk_tx, chunk, metrics).await.is_err() {
                            return Ok(Produced::ReceiverGone(bytes_forwarded));
                        }
                    }
                    Err(e) => {
                        warn!(bucket = %obj.bucket, key = %obj.key, attempt, bytes_forwarded, error = %e, "S3 body stream error");
                        stream_failed = true;
                        break;
                    }
                }
            }
            if !stream_failed {
                metrics.chunks_remaining.fetch_sub(1, Ordering::Relaxed);
                return Ok(Produced::Done(bytes_forwarded));
            }
        }
        Err(anyhow::anyhow!(
            "S3 download failed after {} retries (streamed {bytes_forwarded} bytes): {}/{}",
            config.max_retries,
            obj.bucket,
            obj.key,
        ))
    }

    /// Chunked reassembler: fetch byte-ranges concurrently (ordered dispatch,
    /// lowest index first) into pool-reserved buffers, forward them to the
    /// decoder strictly in order. Bounded by the download semaphore (range-GET
    /// concurrency) and the input-buffer pool (resident bytes).
    #[allow(clippy::too_many_arguments)]
    async fn stream_chunked(
        client: &Client,
        obj: &S3ObjectInfo,
        config: &StreamingDownloaderConfig,
        chunk_size: usize,
        download_observer: &DownloadObserver,
        chunk_tx: &flume::Sender<Bytes>,
        dl_sem: &Arc<Semaphore>,
        pool: &Arc<InputBufferPool>,
        metrics: &ReadPathMetrics,
    ) -> Result<Produced> {
        let cs = chunk_size;
        let n = obj.size.div_ceil(cs);
        // Per-file look-ahead window; the global download semaphore is the real
        // cap, so let a lone big file use full range-GET concurrency.
        let window = config.max_concurrent_downloads.max(1);

        let mut fetches: tokio::task::JoinSet<Result<(usize, Bytes, super::mem_pool::Loan)>> =
            tokio::task::JoinSet::new();
        let mut next_fetch = 0usize;
        let mut next_forward = 0usize;
        let mut ready: std::collections::BTreeMap<usize, (Bytes, super::mem_pool::Loan)> =
            std::collections::BTreeMap::new();
        let mut blocked = false;
        // Chains pool *reservations* into ascending index order: chunk `i`
        // reserves only after `i-1` has. This keeps the in-order `next_forward`
        // chunk first in the pool's FIFO queue, so an out-of-order later chunk
        // can never grab the file's last reservation and starve the one we're
        // waiting to forward (within-file deadlock). The GETs themselves still
        // run concurrently once reserved.
        let mut prev_reserved: Option<tokio::sync::oneshot::Receiver<()>> = None;

        loop {
            // Ordered dispatch: keep the window full, always issuing the lowest
            // outstanding index first.
            while next_fetch < n && fetches.len() < window {
                let idx = next_fetch;
                next_fetch += 1;
                let start = idx * cs;
                let end = ((idx + 1) * cs).min(obj.size);
                let len = end - start;
                let client = client.clone();
                let bucket = obj.bucket.clone();
                let key = obj.key.clone();
                let dl_sem = dl_sem.clone();
                let pool = pool.clone();
                let dl_obs = download_observer.clone();
                let metrics = metrics.clone();
                let max_retries = config.max_retries;
                let initial_delay = config.initial_retry_delay;
                let prev = prev_reserved.take();
                let (reserved_tx, reserved_rx) = tokio::sync::oneshot::channel();
                prev_reserved = Some(reserved_rx);
                fetches.spawn(async move {
                    // Wait our turn to reserve (ascending index order), reserve
                    // memory, then let the next index reserve. Then acquire a
                    // range-GET permit and fetch.
                    if let Some(prev) = prev {
                        let _ = prev.await;
                    }
                    let loan = pool.reserve(len).await?;
                    let _ = reserved_tx.send(());
                    let _permit = dl_sem
                        .acquire_owned()
                        .await
                        .map_err(|e| anyhow::anyhow!("Download semaphore closed: {e}"))?;
                    let _dl_guard = ActiveGetGuard::new(metrics.dl_active.clone());
                    let bytes = Self::fetch_range_buffered(
                        &client,
                        &bucket,
                        &key,
                        start,
                        end,
                        max_retries,
                        initial_delay,
                        &dl_obs,
                    )
                    .await?;
                    Ok((idx, bytes, loan))
                });
            }

            if fetches.is_empty() && next_forward >= n {
                break;
            }

            let joined = match fetches.join_next().await {
                Some(j) => j,
                None => break,
            };
            let (idx, bytes, loan) = match joined {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    fetches.abort_all();
                    return Err(e);
                }
                Err(e) => {
                    fetches.abort_all();
                    return Err(anyhow::anyhow!("Chunk fetch task panic: {e}"));
                }
            };
            metrics
                .b1_held_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            ready.insert(idx, (bytes, loan));

            // Forward all now-contiguous chunks in order.
            while let Some((bytes, loan)) = ready.remove(&next_forward) {
                metrics
                    .b1_held_bytes
                    .fetch_sub(bytes.len() as u64, Ordering::Relaxed);
                if forward_to_decoder(chunk_tx, bytes, metrics).await.is_err() {
                    fetches.abort_all();
                    if blocked {
                        metrics.reassembly_blocked.fetch_sub(1, Ordering::Relaxed);
                    }
                    return Ok(Produced::ReceiverGone(next_forward * cs));
                }
                drop(loan); // release pool reservation once forwarded into B2
                metrics.chunks_remaining.fetch_sub(1, Ordering::Relaxed);
                next_forward += 1;
            }

            // Head-of-line: we hold later chunks but not `next_forward`.
            let now_blocked = !ready.is_empty();
            if now_blocked != blocked {
                if now_blocked {
                    metrics.reassembly_blocked.fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics.reassembly_blocked.fetch_sub(1, Ordering::Relaxed);
                }
                blocked = now_blocked;
            }
        }

        if blocked {
            metrics.reassembly_blocked.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(Produced::Done(obj.size))
    }

    /// Fetch a single `[start, end)` byte-range fully into memory, with
    /// range-resume retry on transient errors. Returns the assembled bytes.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_range_buffered(
        client: &Client,
        bucket: &str,
        key: &str,
        start: usize,
        end: usize,
        max_retries: u32,
        initial_delay: Duration,
        download_observer: &DownloadObserver,
    ) -> Result<Bytes> {
        let len = end - start;
        let mut buf: Vec<u8> = Vec::with_capacity(len);
        for attempt in 0..=max_retries {
            if attempt > 0 {
                let base = initial_delay
                    .mul_f64(2.0f64.powi(attempt as i32 - 1))
                    .min(Duration::from_secs(60));
                tokio::time::sleep(base).await;
                buf.clear(); // re-fetch the whole range on retry (idempotent)
            }
            let from = start + buf.len();
            let req = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(format!("bytes={from}-{}", end - 1));
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("{e}");
                    if !s3::is_recoverable_s3_error(&msg) {
                        return Err(anyhow::anyhow!("Fatal S3 error: {e}"));
                    }
                    warn!(bucket, key, attempt, range_start = start, error = %e, "S3 range request failed");
                    continue;
                }
            };
            let mut body = resp.body;
            let mut stream_failed = false;
            while let Some(chunk_result) = body.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        download_observer.add_bytes(chunk.len());
                        buf.extend_from_slice(&chunk);
                    }
                    Err(e) => {
                        warn!(bucket, key, attempt, range_start = start, error = %e, "S3 range body error");
                        stream_failed = true;
                        break;
                    }
                }
            }
            if !stream_failed {
                return Ok(Bytes::from(buf));
            }
        }
        Err(anyhow::anyhow!(
            "S3 range [{start},{end}) failed after {max_retries} retries: {bucket}/{key}"
        ))
    }

    /// Filter worker: pulls lines from channel, applies regex, emits matches.
    /// Runs entirely in spawn_blocking (CPU-bound regex + blocking channel recv).
    ///
    /// Checks `fatal_error` and the `retire` counter every 1024 lines so the
    /// worker exits promptly when the HTTP pipeline is dead, and so the control
    /// plane can shrink the pool: a retiring worker claims one retirement via
    /// CAS and returns, leaving the rest of the pool running.
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
        retire: Arc<AtomicUsize>,
    ) -> Result<usize> {
        let result = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut local = 0usize;
            let mut lines_processed = 0u64;
            while let Ok(line) = rx.recv() {
                // Every 1024 lines: bail on fatal error (so the channel drains
                // and downloads can stop), then check for a retire request.
                lines_processed += 1;
                if lines_processed & 0x3FF == 0 {
                    if let Some(ref fe) = fatal_error {
                        if fe.load(Ordering::Relaxed) {
                            return Ok(local);
                        }
                    }
                    // Claim a pending retirement (if any) via CAS so exactly one
                    // worker exits per requested shrink, then stop.
                    let mut pending = retire.load(Ordering::Relaxed);
                    while pending > 0 {
                        match retire.compare_exchange_weak(
                            pending,
                            pending - 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                debug!(worker = worker_id, "Filter worker retiring on request");
                                return Ok(local);
                            }
                            Err(observed) => pending = observed,
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
    fn chunk_count_splits_on_size_and_config() {
        // Disabled / single-stream cases → 1.
        assert_eq!(StreamingDownloader::chunk_count(1000, None), 1);
        assert_eq!(StreamingDownloader::chunk_count(1000, Some(0)), 1);
        assert_eq!(StreamingDownloader::chunk_count(50, Some(50)), 1); // size == cs
        assert_eq!(StreamingDownloader::chunk_count(10, Some(50)), 1); // size < cs
                                                                       // Chunked: ceil(size / cs).
        assert_eq!(StreamingDownloader::chunk_count(100, Some(50)), 2);
        assert_eq!(StreamingDownloader::chunk_count(120, Some(50)), 3); // last chunk partial
        assert_eq!(StreamingDownloader::chunk_count(101, Some(50)), 3);
    }

    #[test]
    fn chunk_reader_returns_eof_when_sender_dropped() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        drop(tx);
        let mut reader = ChunkReader::new(rx, ReadPathMetrics::new(0, None));
        let mut buf = [0u8; 16];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn chunk_reader_reads_single_chunk_in_one_call() {
        let (tx, rx) = flume::bounded::<Bytes>(4);
        tx.send(Bytes::from_static(b"hello")).unwrap();
        drop(tx);
        let mut reader = ChunkReader::new(rx, ReadPathMetrics::new(0, None));
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
        let mut reader = ChunkReader::new(rx, ReadPathMetrics::new(0, None));
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

        let reader = ChunkReader::new(rx, ReadPathMetrics::new(0, None));
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
        let mut reader = ChunkReader::new(rx, ReadPathMetrics::new(0, None));
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(
            out, b"foo",
            "current behavior: empty chunk short-circuits to EOF"
        );
    }
}
