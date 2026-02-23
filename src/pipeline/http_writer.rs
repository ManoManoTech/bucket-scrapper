use super::observer::PipelineObserver;
use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::Client;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// A zstd-compressed batch ready for HTTP upload.
struct CompressedBatch {
    body: Bytes,
    lines: usize,
    plaintext_bytes: usize,
}

/// Result of a batch send attempt.
enum SendResult {
    /// Batch was accepted by the server.
    Ok,
    /// Retryable failure (server error, timeout) — log and continue with next batch.
    Retryable(anyhow::Error),
    /// Fatal failure (4xx client error) — stop the entire pipeline.
    Fatal(anyhow::Error),
    /// Server asked us to slow down (HTTP 429). Trigger AIMD decrease and retry.
    Throttled {
        retry_after: Option<Duration>,
    },
}

// ---------------------------------------------------------------------------
// AIMD upload throttle (adaptive-increase / multiplicative-decrease)
// ---------------------------------------------------------------------------

/// Floor rate — never throttle below 2 MB/s.
const MIN_RATE_BYTES_PER_SEC: f64 = 2.0 * 1_000_000.0;

/// Shared token-bucket rate limiter with AIMD-controlled refill rate.
///
/// All uploader tasks share one `UploadThrottle`.  When a batch takes longer
/// than `max_submission_time`, the rate is **halved** (multiplicative decrease).
/// When batches are healthy, the rate grows by a fixed increment (additive
/// increase).  The asymmetry reacts quickly to overload but recovers cautiously.
pub struct UploadThrottle {
    state: tokio::sync::Mutex<ThrottleState>,
    /// `None` = AIMD disabled (static-only mode).
    max_submission_time: Option<Duration>,
    /// Observable rate for progress reporting — stores f64 bits as u64.
    current_rate_bits: Arc<AtomicU64>,
    /// AIMD multiplicative decrease factor (e.g. 0.15 → multiply rate by 0.85).
    decrease_factor: f64,
    /// AIMD additive increase in bytes/sec per healthy batch.
    increase_bytes: f64,
}

struct ThrottleState {
    /// Current allowed rate in bytes/sec.  Starts at `f64::INFINITY` (unlimited).
    rate: f64,
    /// First-congestion ceiling — observed aggregate throughput.  Set once.
    ceiling: f64,
    /// Available byte budget.
    tokens: f64,
    /// Max token bucket size (2 × batch_max_bytes).
    max_tokens: f64,
    /// Last time we refilled tokens.
    last_refill: Instant,
    /// Last time we performed a multiplicative decrease (cooldown guard).
    last_decrease: Instant,
    /// Cumulative bytes uploaded (for first-congestion throughput estimate).
    total_bytes: u64,
    /// Pipeline start time (for first-congestion throughput estimate).
    start_time: Instant,
    /// Number of congestion events seen.
    congestion_count: u64,
}

impl UploadThrottle {
    /// Create a new throttle.  `batch_max_bytes` sizes the token bucket.
    ///
    /// - `max_submission_time`: AIMD congestion threshold. `None` = no AIMD.
    /// - `static_rate_limit`: hard ceiling in bytes/sec. `None` = unlimited.
    /// - `decrease_factor`: fraction to reduce rate by on congestion (e.g. 0.15 → rate × 0.85).
    /// - `increase_bytes`: additive increase in bytes/sec per healthy batch.
    fn new(
        max_submission_time: Option<Duration>,
        batch_max_bytes: usize,
        static_rate_limit: Option<f64>,
        decrease_factor: f64,
        increase_bytes: f64,
    ) -> Self {
        let max_tokens = 2.0 * batch_max_bytes as f64;
        let now = Instant::now();
        let (initial_rate, initial_ceiling) = match static_rate_limit {
            Some(limit) => (limit, limit),
            None => (f64::INFINITY, 0.0),
        };
        Self {
            state: tokio::sync::Mutex::new(ThrottleState {
                rate: initial_rate,
                ceiling: initial_ceiling,
                tokens: max_tokens,
                max_tokens,
                last_refill: now,
                last_decrease: now - Duration::from_secs(60), // allow immediate first decrease
                total_bytes: 0,
                start_time: now,
                congestion_count: 0,
            }),
            max_submission_time,
            current_rate_bits: Arc::new(AtomicU64::new(initial_rate.to_bits())),
            decrease_factor,
            increase_bytes,
        }
    }

    /// Wait until the token bucket has `bytes` available.
    async fn acquire(&self, bytes: usize) {
        let bytes_f = bytes as f64;
        loop {
            let sleep_dur = {
                let mut st = self.state.lock().await;
                // Refill tokens based on elapsed time
                let now = Instant::now();
                let elapsed = now.duration_since(st.last_refill).as_secs_f64();
                if elapsed > 0.0 {
                    let added = st.rate * elapsed;
                    st.tokens = (st.tokens + added).min(st.max_tokens);
                    st.last_refill = now;
                }
                // If unlimited or enough tokens, deduct and return
                if st.rate.is_infinite() || st.tokens >= bytes_f {
                    st.tokens -= bytes_f;
                    return;
                }
                // Compute how long to wait for enough tokens
                let deficit = bytes_f - st.tokens;
                Duration::from_secs_f64(deficit / st.rate)
            };
            // Sleep without holding the lock
            tokio::time::sleep(sleep_dur).await;
        }
    }

    /// Apply multiplicative decrease.  Single code path for both timing-based
    /// and 429-based congestion signals.
    ///
    /// Returns `true` if the decrease was applied, `false` if blocked by cooldown.
    fn apply_decrease(
        st: &mut ThrottleState,
        cooldown: Duration,
        decrease_factor: f64,
        current_rate_bits: &AtomicU64,
        reason: &str,
    ) -> bool {
        if st.last_decrease.elapsed() < cooldown {
            return false;
        }
        st.congestion_count += 1;

        // Ensure ceiling is estimated from observed throughput
        if st.ceiling == 0.0 {
            let elapsed = st.start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 && st.total_bytes > 0 {
                st.ceiling = st.total_bytes as f64 / elapsed;
            } else {
                st.ceiling = MIN_RATE_BYTES_PER_SEC * 10.0;
            }
        }

        let keep = 1.0 - decrease_factor;
        let new_rate = if st.rate.is_infinite() {
            st.ceiling * keep
        } else {
            (st.rate * keep).max(MIN_RATE_BYTES_PER_SEC)
        };

        info!(
            old_rate_mbps = format_args!("{:.2}", if st.rate.is_infinite() { f64::INFINITY } else { st.rate / 1_000_000.0 }),
            new_rate_mbps = format_args!("{:.2}", new_rate / 1_000_000.0),
            ceiling_mbps = format_args!("{:.2}", st.ceiling / 1_000_000.0),
            congestion_count = st.congestion_count,
            reason = reason,
            "Upload throttle: rate decreased"
        );

        st.rate = new_rate;
        st.tokens = 0.0;
        st.last_decrease = Instant::now();
        st.last_refill = Instant::now();
        current_rate_bits.store(st.rate.to_bits(), Ordering::Relaxed);
        true
    }

    /// Record a completed upload and apply AIMD logic.
    ///
    /// Called only on successful sends — failed sends do not adjust the rate.
    /// When `max_submission_time` is `None`, AIMD is skipped (static-only mode).
    fn record_upload_sync(
        st: &mut ThrottleState,
        bytes: usize,
        duration: Duration,
        max_submission_time: Option<Duration>,
        current_rate_bits: &AtomicU64,
        decrease_factor: f64,
        increase_bytes: f64,
    ) {
        st.total_bytes += bytes as u64;

        // Without AIMD threshold, only static token-bucket rate applies — nothing to adjust.
        let mst = match max_submission_time {
            Some(d) => d,
            None => return,
        };

        let cooldown = Duration::from_secs_f64((mst.as_secs_f64() / 2.0).max(1.0));

        if duration > mst {
            Self::apply_decrease(
                st,
                cooldown,
                decrease_factor,
                current_rate_bits,
                "batch exceeded submission time threshold",
            );
        } else if st.ceiling > 0.0 && !st.rate.is_infinite() {
            // --- Additive Increase ---
            let new_rate = (st.rate + increase_bytes).min(st.ceiling);
            if new_rate > st.rate {
                debug!(
                    old_rate_mbps = format_args!("{:.2}", st.rate / 1_000_000.0),
                    new_rate_mbps = format_args!("{:.2}", new_rate / 1_000_000.0),
                    ceiling_mbps = format_args!("{:.2}", st.ceiling / 1_000_000.0),
                    "Upload throttle: additive increase"
                );
                st.rate = new_rate;
                current_rate_bits.store(st.rate.to_bits(), Ordering::Relaxed);
            }
        }
    }

    /// Record a completed upload and apply AIMD logic.
    async fn record_upload(&self, bytes: usize, duration: Duration) {
        let mut st = self.state.lock().await;
        Self::record_upload_sync(
            &mut st,
            bytes,
            duration,
            self.max_submission_time,
            &self.current_rate_bits,
            self.decrease_factor,
            self.increase_bytes,
        );
    }

    /// Force a multiplicative decrease (e.g. on HTTP 429).
    ///
    /// Respects the same cooldown as timing-based decreases.
    async fn force_decrease(&self) {
        let mut st = self.state.lock().await;
        let mst = self.max_submission_time.unwrap_or(Duration::from_secs(4));
        let cooldown = Duration::from_secs_f64((mst.as_secs_f64() / 2.0).max(1.0));
        Self::apply_decrease(
            &mut st,
            cooldown,
            self.decrease_factor,
            &self.current_rate_bits,
            "server returned HTTP 429 Too Many Requests",
        );
    }

    /// Get current rate in MB/s for progress reporting.
    /// Returns `None` if unlimited (no throttling engaged).
    pub fn current_rate_mbps(&self) -> Option<f64> {
        let bits = self.current_rate_bits.load(Ordering::Relaxed);
        let rate = f64::from_bits(bits);
        if rate.is_infinite() {
            None
        } else {
            Some(rate / 1_000_000.0)
        }
    }

    /// Get the Arc<AtomicU64> for sharing with PipelineObserver.
    fn rate_bits(&self) -> Arc<AtomicU64> {
        self.current_rate_bits.clone()
    }
}

/// Configuration for the HTTP writer
#[derive(Debug, Clone)]
pub struct HttpWriterConfig {
    /// The URL to send logs to (e.g., https://intake.handy-mango.http.com/api/v1/logs)
    pub url: String,
    /// API key for authentication
    pub api_key: String,
    /// Maximum batch size in compressed bytes. Default limit is 30MB.
    pub batch_max_bytes: usize,
    /// Timeout for HTTP requests in seconds
    pub timeout_secs: u64,
    /// Maximum retry attempts for failed requests
    pub max_retries: u32,
    /// Channel buffer size for backpressure control (line channel)
    pub channel_buffer_size: usize,
    /// Number of CPU-bound compressor tasks
    pub num_compressor_tasks: usize,
    /// Number of IO-bound upload tasks (default: 4× compressor tasks)
    pub num_upload_tasks: usize,
    /// Batch channel capacity between compressors and uploaders (RAM ≈ this × batch_max_bytes)
    pub upload_channel_size: usize,
    /// Zstd compression level (1-22)
    pub compression_level: i32,
    /// Per-batch submission time threshold for AIMD throttle.
    /// `None` = no AIMD throttling.  Default: `Some(2.5s)`.
    pub max_submission_time: Option<Duration>,
    /// Static global upload rate limit in bytes/sec.
    /// `None` = unlimited.  When set, the token bucket starts at this rate
    /// instead of `INFINITY`.  AIMD can decrease below it, then recover back.
    pub max_upload_rate: Option<f64>,
    /// AIMD multiplicative decrease factor (0.15 = reduce rate by 15%).
    /// Applied as `rate * (1.0 - factor)`.
    pub aimd_decrease_factor: f64,
    /// AIMD additive increase in bytes/sec per healthy batch.
    pub aimd_increase_bytes: f64,
}

impl Default for HttpWriterConfig {
    fn default() -> Self {
        let num_compressor_tasks = std::thread::available_parallelism()
            .map(|n| n.get() / 8)
            .unwrap_or(1)
            .max(1);
        Self {
            url: String::new(),
            api_key: String::new(),
            batch_max_bytes: 2 * 1024 * 1024, // 2MB default
            timeout_secs: 30,
            max_retries: 3,
            channel_buffer_size: 1000,
            num_compressor_tasks,
            num_upload_tasks: 4 * num_compressor_tasks,
            upload_channel_size: 4,
            compression_level: 3,
            max_submission_time: Some(Duration::from_secs(4)),
            max_upload_rate: None,
            aimd_decrease_factor: 0.15,
            aimd_increase_bytes: 1_000_000.0,
        }
    }
}

/// Stats returned by `HttpResultWriter::finish()`
#[derive(Debug, Clone)]
pub struct HttpWriterStats {
    pub lines_sent: usize,
    pub lines_dropped: usize,
    pub compressed_bytes_sent: usize,
    pub plaintext_bytes_sent: usize,
}

/// Shared atomic counters for uploader tasks.
#[derive(Clone)]
struct UploaderCounters {
    lines_sent: Arc<AtomicUsize>,
    lines_dropped: Arc<AtomicUsize>,
    compressed_bytes_sent: Arc<AtomicUsize>,
    plaintext_bytes_sent: Arc<AtomicUsize>,
    batches_uploaded: Arc<AtomicUsize>,
    upload_time_us: Arc<AtomicUsize>,
}

impl Default for UploaderCounters {
    fn default() -> Self {
        Self {
            lines_sent: Arc::new(AtomicUsize::new(0)),
            lines_dropped: Arc::new(AtomicUsize::new(0)),
            compressed_bytes_sent: Arc::new(AtomicUsize::new(0)),
            plaintext_bytes_sent: Arc::new(AtomicUsize::new(0)),
            batches_uploaded: Arc::new(AtomicUsize::new(0)),
            upload_time_us: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Manages streaming writes of search results to an HTTP API
pub struct HttpResultWriter {
    write_tx: flume::Sender<String>,
    /// Kept for `observer()` — dropped in `finish()` before joining compressors
    batch_tx: flume::Sender<CompressedBatch>,
    compressor_handles: Vec<tokio::task::JoinHandle<()>>,
    upload_handles: Vec<tokio::task::JoinHandle<usize>>,
    counters: UploaderCounters,
    /// Set on fatal (4xx) errors to stop the entire pipeline
    fatal_error: Arc<AtomicBool>,
    /// AIMD upload throttle (None = disabled)
    throttle: Option<Arc<UploadThrottle>>,
    /// URL for display purposes
    url: String,
}

impl HttpResultWriter {
    /// Create a new HTTP writer with separate compressor and uploader pools
    pub fn new(config: HttpWriterConfig) -> Result<Self> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs));

        if let Some(path) = crate::utils::proxy::resolve_ca_bundle_path() {
            let pem = std::fs::read(&path)
                .with_context(|| format!("Failed to read CA bundle: {path}"))?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .with_context(|| format!("Failed to parse CA certificate: {path}"))?;
            builder = builder.add_root_certificate(cert);
            info!(path = %path, "Loaded custom CA bundle for HTTP writer");
        }

        let client = builder.build()
            .context("Failed to create HTTP client")?;

        // Line channel: searchers → compressors
        let (write_tx, line_rx) = flume::bounded::<String>(config.channel_buffer_size);
        // Batch channel: compressors → uploaders
        let (batch_tx, batch_rx) = flume::bounded::<CompressedBatch>(config.upload_channel_size);

        let counters = UploaderCounters::default();
        let fatal_error = Arc::new(AtomicBool::new(false));
        let url = config.url.clone();

        // Create upload throttle if either AIMD or static rate limit is configured
        let throttle = if config.max_submission_time.is_some() || config.max_upload_rate.is_some() {
            Some(Arc::new(UploadThrottle::new(
                config.max_submission_time,
                config.batch_max_bytes,
                config.max_upload_rate,
                config.aimd_decrease_factor,
                config.aimd_increase_bytes,
            )))
        } else {
            None
        };

        // Spawn compressor tasks (CPU-bound: line_rx → batch_tx)
        let mut compressor_handles = Vec::with_capacity(config.num_compressor_tasks);
        for task_id in 0..config.num_compressor_tasks {
            let rx = line_rx.clone();
            let tx = batch_tx.clone();
            let cfg = config.clone();
            let ld = counters.lines_dropped.clone();
            let fe = fatal_error.clone();
            let handle = tokio::spawn(async move {
                Self::compressor_task(task_id, rx, tx, cfg, ld, fe).await;
            });
            compressor_handles.push(handle);
        }

        // Spawn uploader tasks (IO-bound: batch_rx → HTTP)
        let mut upload_handles = Vec::with_capacity(config.num_upload_tasks);
        for task_id in 0..config.num_upload_tasks {
            let rx = batch_rx.clone();
            let client = client.clone();
            let cfg = config.clone();
            let ctrs = counters.clone();
            let fe = fatal_error.clone();
            let th = throttle.clone();
            let handle = tokio::spawn(async move {
                Self::uploader_task(task_id, rx, client, cfg, ctrs, fe, th).await
            });
            upload_handles.push(handle);
        }

        // Drop original receivers — only task clones hold them,
        // so channel closure propagates automatically when tasks exit.
        // Keep batch_tx for observer(); dropped explicitly in finish().
        drop(line_rx);
        drop(batch_rx);

        Ok(Self {
            write_tx,
            batch_tx,
            compressor_handles,
            upload_handles,
            counters,
            fatal_error,
            throttle,
            url,
        })
    }

    /// Get a sender that can be cloned for use in multiple tasks
    pub fn get_sender(&self) -> flume::Sender<String> {
        self.write_tx.clone()
    }

    /// Get a read-only observer for pipeline channel stats.
    pub fn observer(&self) -> PipelineObserver {
        PipelineObserver::new(
            &self.write_tx,
            &self.batch_tx,
            self.counters.batches_uploaded.clone(),
            self.counters.upload_time_us.clone(),
            self.counters.compressed_bytes_sent.clone(),
            self.throttle.as_ref().map(|t| t.rate_bits()),
        )
    }

    /// Get current lines sent count
    pub fn lines_sent(&self) -> usize {
        self.counters.lines_sent.load(Ordering::Relaxed)
    }

    /// CPU-bound task that reads lines from the line channel, compresses them into
    /// zstd batches, and sends CompressedBatch to the batch channel for uploaders.
    async fn compressor_task(
        task_id: usize,
        line_rx: flume::Receiver<String>,
        batch_tx: flume::Sender<CompressedBatch>,
        config: HttpWriterConfig,
        lines_dropped: Arc<AtomicUsize>,
        fatal_error: Arc<AtomicBool>,
    ) {
        let mut batches_produced: u64 = 0;
        let mut channel_closed = false;

        'outer: loop {
            // Stop immediately if an uploader hit a fatal error
            if fatal_error.load(Ordering::Relaxed) {
                break 'outer;
            }

            // 1. Wait for first line (or break if channel closed)
            let first_line = match line_rx.recv_async().await {
                Ok(line) => line,
                Err(_) => break 'outer,
            };

            // 2. Compress lines into buffer
            let mut encoder = match zstd::Encoder::new(Vec::new(), config.compression_level) {
                Ok(enc) => enc,
                Err(e) => {
                    error!(task = task_id, error = %e, "Failed to create zstd encoder");
                    break 'outer;
                }
            };

            let mut batch_lines = 0usize;
            let mut batch_plaintext_bytes = 0usize;

            // Write first line (Vec<u8> writes are infallible barring OOM)
            encoder.write_all(first_line.as_bytes()).expect("write to Vec");
            batch_lines += 1;
            batch_plaintext_bytes += first_line.len();

            // Fill batch until compressed output reaches batch_max_bytes or channel closes
            loop {
                if encoder.get_ref().len() >= config.batch_max_bytes {
                    break;
                }

                let line = match line_rx.try_recv() {
                    Ok(line) => line,
                    Err(flume::TryRecvError::Empty) => {
                        match tokio::time::timeout(
                            Duration::from_millis(50),
                            line_rx.recv_async(),
                        )
                        .await
                        {
                            Ok(Ok(line)) => line,
                            Ok(Err(_)) => {
                                channel_closed = true;
                                break;
                            }
                            Err(_) => continue, // timeout, re-check compressed size
                        }
                    }
                    Err(flume::TryRecvError::Disconnected) => {
                        channel_closed = true;
                        break;
                    }
                };

                encoder.write_all(line.as_bytes()).expect("write to Vec");
                batch_lines += 1;
                batch_plaintext_bytes += line.len();
            }

            // 3. Finalize zstd frame → CompressedBatch
            let compressed = match encoder.finish() {
                Ok(buf) => buf,
                Err(e) => {
                    error!(task = task_id, error = %e, lines = batch_lines, "Failed to finalize zstd frame");
                    lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                    if channel_closed { break 'outer; }
                    continue 'outer;
                }
            };
            let body = Bytes::from(compressed);

            let batch = CompressedBatch {
                body,
                lines: batch_lines,
                plaintext_bytes: batch_plaintext_bytes,
            };

            // Send to uploader pool — backpressure if uploaders fall behind
            if batch_tx.send_async(batch).await.is_err() {
                // All uploaders gone
                error!(task = task_id, lines = batch_lines, "Batch channel closed, uploaders gone");
                lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                break 'outer;
            }

            batches_produced += 1;

            if channel_closed {
                break 'outer;
            }
        }

        debug!(task = task_id, batches_produced = batches_produced, "Compressor task finished");
    }

    /// IO-bound task that receives compressed batches and uploads them via HTTP.
    async fn uploader_task(
        task_id: usize,
        batch_rx: flume::Receiver<CompressedBatch>,
        client: Client,
        config: HttpWriterConfig,
        ctrs: UploaderCounters,
        fatal_error: Arc<AtomicBool>,
        throttle: Option<Arc<UploadThrottle>>,
    ) -> usize {
        let mut total_sent = 0usize;
        let mut batches_sent: u64 = 0;
        let mut last_stats_log = Instant::now();

        while let Ok(batch) = batch_rx.recv_async().await {
            let batch_compressed_bytes = batch.body.len();

            // Wait for token budget from AIMD throttle (no-op if disabled or unlimited)
            if let Some(ref th) = throttle {
                th.acquire(batch_compressed_bytes).await;
            }

            // Try sending with 429-aware retry loop
            let mut throttle_retries = 0u32;
            let result = loop {
                let send_start = Instant::now();
                match Self::send_batch(&client, &config, &batch.body, batch.lines).await {
                    SendResult::Ok => {
                        let send_elapsed = send_start.elapsed();
                        let elapsed_us = send_elapsed.as_micros() as usize;
                        total_sent += batch.lines;
                        batches_sent += 1;
                        ctrs.lines_sent.fetch_add(batch.lines, Ordering::Relaxed);
                        ctrs.compressed_bytes_sent.fetch_add(batch_compressed_bytes, Ordering::Relaxed);
                        ctrs.plaintext_bytes_sent.fetch_add(batch.plaintext_bytes, Ordering::Relaxed);
                        ctrs.batches_uploaded.fetch_add(1, Ordering::Relaxed);
                        ctrs.upload_time_us.fetch_add(elapsed_us, Ordering::Relaxed);

                        // Feed AIMD throttle with timing data
                        if let Some(ref th) = throttle {
                            th.record_upload(batch_compressed_bytes, send_elapsed).await;
                        }

                        debug!(
                            task = task_id,
                            lines = batch.lines,
                            compressed_bytes = batch_compressed_bytes,
                            plaintext_bytes = batch.plaintext_bytes,
                            upload_ms = send_elapsed.as_millis() as u64,
                            url = %config.url,
                            "Sent batch"
                        );
                        break None; // success
                    }
                    SendResult::Throttled { retry_after } => {
                        // Trigger AIMD decrease
                        if let Some(ref th) = throttle {
                            th.force_decrease().await;
                        }

                        throttle_retries += 1;
                        if throttle_retries > config.max_retries {
                            warn!(
                                task = task_id,
                                retries = throttle_retries,
                                "429 retries exhausted, dropping batch"
                            );
                            break Some(SendResult::Retryable(anyhow::anyhow!(
                                "HTTP 429 retries exhausted after {} attempts",
                                throttle_retries
                            )));
                        }

                        let delay = retry_after.unwrap_or_else(|| {
                            Duration::from_millis(100 * 2u64.pow(throttle_retries - 1))
                        });
                        warn!(
                            task = task_id,
                            retry = throttle_retries,
                            delay_ms = delay.as_millis() as u64,
                            retry_after_header = retry_after.is_some(),
                            "HTTP 429, backing off"
                        );
                        tokio::time::sleep(delay).await;

                        // Re-acquire throttle tokens before retry
                        if let Some(ref th) = throttle {
                            th.acquire(batch_compressed_bytes).await;
                        }
                        continue; // retry the same batch
                    }
                    other => break Some(other),
                }
            };

            // Handle non-429 results
            if let Some(result) = result {
                match result {
                    SendResult::Retryable(e) => {
                        ctrs.lines_dropped.fetch_add(batch.lines, Ordering::Relaxed);
                        error!(
                            task = task_id,
                            lines = batch.lines,
                            compressed_bytes = batch_compressed_bytes,
                            error = %e,
                            "Failed to send batch"
                        );
                    }
                    SendResult::Fatal(e) => {
                        ctrs.lines_dropped.fetch_add(batch.lines, Ordering::Relaxed);
                        error!(
                            task = task_id,
                            lines = batch.lines,
                            compressed_bytes = batch_compressed_bytes,
                            error = %e,
                            "Fatal error, stopping pipeline"
                        );
                        fatal_error.store(true, Ordering::Relaxed);
                        break;
                    }
                    SendResult::Ok | SendResult::Throttled { .. } => unreachable!(),
                }
            }

            if last_stats_log.elapsed() > Duration::from_secs(30) {
                debug!(
                    task = task_id,
                    batch_channel_len = batch_rx.len(),
                    batches_sent = batches_sent,
                    "Uploader task stats"
                );
                last_stats_log = Instant::now();
            }
        }

        debug!(task = task_id, total_lines = total_sent, batches_sent = batches_sent, "Uploader task finished");
        total_sent
    }

    /// Send a zstd-compressed batch to the HTTP API with retries.
    /// `body` has a known length so reqwest sets Content-Length, enabling connection reuse.
    /// Returns `Fatal` on 4xx client errors (caller should stop the pipeline).
    async fn send_batch(
        client: &Client,
        config: &HttpWriterConfig,
        body: &Bytes,
        count: usize,
    ) -> SendResult {
        let mut last_error = None;

        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                let delay = Duration::from_millis(100 * 2u64.pow(attempt - 1));
                warn!(
                    attempt = attempt + 1,
                    max_attempts = config.max_retries + 1,
                    delay_ms = delay.as_millis() as u64,
                    "Retrying HTTP request"
                );
                tokio::time::sleep(delay).await;
            }

            let result = client
                .post(&config.url)
                .header("Content-Type", "application/x-ndjson")
                .header("Content-Encoding", "zstd")
                .header("Authorization", format!("Bearer {}", config.api_key))
                .body(body.clone()) // O(1) Arc bump
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        let retry_after = response
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<f64>().ok())
                            .map(Duration::from_secs_f64);
                        return SendResult::Throttled { retry_after };
                    }
                    let response_body = response.text().await.unwrap_or_default();

                    if status.is_success() {
                        debug!(
                            lines = count,
                            bytes = body.len(),
                            status = %status,
                            response = %response_body,
                            "HTTP batch sent"
                        );
                        return SendResult::Ok;
                    } else if status.is_client_error() {
                        return SendResult::Fatal(anyhow::anyhow!(
                            "HTTP {status}: {response_body}"
                        ));
                    } else {
                        last_error = Some(anyhow::anyhow!(
                            "HTTP {status} from API: {response_body}"
                        ));
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("HTTP request failed: {e}"));
                }
            }
        }

        SendResult::Retryable(
            last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error sending batch")),
        )
    }

    /// Finish writing and return stats for all tasks.
    ///
    /// Ordered shutdown:
    /// 1. Close line channel → compressors drain and exit
    /// 2. Join compressors → their batch_tx clones drop → batch channel closes
    /// 3. Join uploaders → they drain remaining batches and exit
    pub async fn finish(self) -> Result<HttpWriterStats> {
        // 1. Close the line channel to signal compressors to drain and exit
        drop(self.write_tx);
        // Drop our batch_tx clone so the batch channel closes once compressor
        // tasks finish and drop their clones
        drop(self.batch_tx);

        // 2. Join all compressor handles — they drain lines, produce final batches, then exit
        //    Their batch_tx clones are dropped on exit, closing the batch channel
        for handle in self.compressor_handles {
            if let Err(e) = handle.await {
                return Err(anyhow::anyhow!("Compressor task panicked: {e}"));
            }
        }

        // 3. Batch channel is now closed (all senders gone).
        //    Join all uploader handles — they drain remaining batches and exit
        let mut total = 0usize;
        for handle in self.upload_handles {
            match handle.await {
                Ok(count) => total += count,
                Err(e) => {
                    return Err(anyhow::anyhow!("Uploader task panicked: {e}"));
                }
            }
        }

        let stats = HttpWriterStats {
            lines_sent: total,
            lines_dropped: self.counters.lines_dropped.load(Ordering::Relaxed),
            compressed_bytes_sent: self.counters.compressed_bytes_sent.load(Ordering::Relaxed),
            plaintext_bytes_sent: self.counters.plaintext_bytes_sent.load(Ordering::Relaxed),
        };

        if self.fatal_error.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!(
                "Pipeline aborted due to fatal HTTP error (lines_sent={}, lines_dropped={})",
                stats.lines_sent,
                stats.lines_dropped,
            ));
        }

        Ok(stats)
    }

    /// Get the fatal-error flag so callers can observe pipeline aborts.
    ///
    /// When an uploader task encounters a non-retryable (4xx) HTTP error it
    /// stores `true` into this flag.  External code (e.g. download coordinator)
    /// should poll it to abort early instead of continuing to download data
    /// that will never be uploaded.
    pub fn fatal_error_flag(&self) -> Arc<AtomicBool> {
        self.fatal_error.clone()
    }

    /// Get the configured URL
    pub fn url(&self) -> &str {
        &self.url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn throttle_starts_unlimited() {
        let th = UploadThrottle::new(Some(Duration::from_secs_f64(2.5)), 2 * 1024 * 1024, None, 0.15, 1_000_000.0);
        assert!(th.current_rate_mbps().is_none(), "should be unlimited initially");
    }

    #[tokio::test]
    async fn throttle_acquire_instant_when_unlimited() {
        let th = UploadThrottle::new(Some(Duration::from_secs_f64(2.5)), 2 * 1024 * 1024, None, 0.15, 1_000_000.0);
        let start = Instant::now();
        th.acquire(1_000_000).await;
        // Unlimited — should return near-instantly
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn throttle_decreases_on_slow_upload() {
        let mst = Duration::from_secs_f64(1.0);
        let th = UploadThrottle::new(Some(mst), 1024, None, 0.15, 1_000_000.0);

        // Simulate a few healthy uploads to build total_bytes
        for _ in 0..5 {
            th.record_upload(100_000, Duration::from_millis(200)).await;
        }
        assert!(th.current_rate_mbps().is_none(), "still unlimited before congestion");

        // Simulate a slow upload (exceeds max_submission_time)
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate = th.current_rate_mbps();
        assert!(rate.is_some(), "throttle should be engaged after slow upload");
        assert!(rate.unwrap() > 0.0, "rate should be positive");
    }

    #[tokio::test]
    async fn throttle_additive_increase_after_decrease() {
        let mst = Duration::from_secs_f64(1.0);
        let th = UploadThrottle::new(Some(mst), 1024, None, 0.15, 1_000_000.0);

        // Build up bytes
        for _ in 0..5 {
            th.record_upload(100_000, Duration::from_millis(200)).await;
        }
        // Trigger decrease
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate_after_decrease = th.current_rate_mbps().unwrap();

        // Healthy upload → additive increase
        th.record_upload(100_000, Duration::from_millis(200)).await;
        let rate_after_increase = th.current_rate_mbps().unwrap();
        assert!(
            rate_after_increase > rate_after_decrease,
            "rate should increase: {rate_after_increase} > {rate_after_decrease}"
        );
    }

    #[tokio::test]
    async fn throttle_respects_cooldown() {
        let mst = Duration::from_secs_f64(1.0);
        let th = UploadThrottle::new(Some(mst), 1024, None, 0.15, 1_000_000.0);

        // Build up bytes
        for _ in 0..5 {
            th.record_upload(100_000, Duration::from_millis(200)).await;
        }
        // First decrease
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate1 = th.current_rate_mbps().unwrap();

        // Second slow upload immediately — should be ignored (cooldown)
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate2 = th.current_rate_mbps().unwrap();

        // Rate should have *increased* (additive increase applied, decrease blocked by cooldown)
        // or at least not halved
        assert!(
            rate2 >= rate1 * 0.9,
            "rate should not halve during cooldown: rate1={rate1}, rate2={rate2}"
        );
    }

    #[tokio::test]
    async fn throttle_rate_never_below_floor() {
        let mst = Duration::from_secs_f64(0.01); // very low threshold
        let th = UploadThrottle::new(Some(mst), 1024, None, 0.15, 1_000_000.0);

        // Build up some bytes
        th.record_upload(1000, Duration::from_millis(5)).await;

        // Trigger many decreases (with pauses to clear cooldown)
        for _ in 0..20 {
            th.record_upload(1000, Duration::from_secs(1)).await;
            // Wait out the cooldown
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let rate = th.current_rate_mbps().unwrap();
        let floor_mbps = MIN_RATE_BYTES_PER_SEC / 1_000_000.0;
        assert!(
            rate >= floor_mbps * 0.99,
            "rate should not drop below floor: {rate} >= {floor_mbps}"
        );
    }

    #[tokio::test]
    async fn throttle_static_limit_enforced_from_start() {
        // 10 MB/s static limit, no AIMD
        let limit = 10.0 * 1_000_000.0;
        let th = UploadThrottle::new(None, 2 * 1024 * 1024, Some(limit), 0.15, 1_000_000.0);

        // Rate should be visible immediately (not None/infinite)
        let rate = th.current_rate_mbps();
        assert!(rate.is_some(), "static limit should show rate immediately");
        assert!(
            (rate.unwrap() - 10.0).abs() < 0.01,
            "rate should be ~10 MB/s, got {}",
            rate.unwrap()
        );

        // Acquire should succeed (token bucket starts full)
        let start = Instant::now();
        th.acquire(1_000_000).await;
        assert!(start.elapsed() < Duration::from_millis(50), "first acquire should be instant (bucket starts full)");

        // Record upload — with no AIMD, rate should stay at limit
        th.record_upload(1_000_000, Duration::from_millis(100)).await;
        let rate_after = th.current_rate_mbps().unwrap();
        assert!(
            (rate_after - 10.0).abs() < 0.01,
            "rate should remain ~10 MB/s without AIMD, got {}",
            rate_after
        );
    }

    #[tokio::test]
    async fn force_decrease_reduces_rate() {
        let mst = Duration::from_secs_f64(1.0);
        let decrease_factor = 0.15;
        let th = UploadThrottle::new(Some(mst), 1024, None, decrease_factor, 1_000_000.0);

        // Build up total_bytes with healthy uploads
        for _ in 0..5 {
            th.record_upload(100_000, Duration::from_millis(200)).await;
        }
        // Trigger first congestion to establish ceiling and finite rate
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate_after_congestion = th.current_rate_mbps().unwrap();

        // Wait out cooldown (min 1s) before force_decrease can fire
        tokio::time::sleep(Duration::from_millis(1100)).await;

        th.force_decrease().await;
        let rate_after_force = th.current_rate_mbps().unwrap();

        let expected = rate_after_congestion * (1.0 - decrease_factor);
        let floor_mbps = MIN_RATE_BYTES_PER_SEC / 1_000_000.0;
        let expected = expected.max(floor_mbps);
        assert!(
            (rate_after_force - expected).abs() < 0.01,
            "force_decrease should reduce rate by {decrease_factor}: \
             expected ~{expected:.4}, got {rate_after_force:.4}"
        );
    }

    #[tokio::test]
    async fn configurable_factors_produce_expected_values() {
        let mst = Duration::from_secs_f64(1.0);
        let decrease_factor = 0.30;
        let increase_bytes = 2_000_000.0;
        let th = UploadThrottle::new(Some(mst), 1024, None, decrease_factor, increase_bytes);

        // Build up total_bytes with healthy uploads
        for _ in 0..5 {
            th.record_upload(100_000, Duration::from_millis(200)).await;
        }

        // Trigger congestion — ceiling is estimated from throughput, rate = ceiling * 0.70
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate_after_decrease = th.current_rate_mbps().unwrap();

        // Read ceiling from the rate: rate = ceiling * (1 - 0.30) = ceiling * 0.70
        let implied_ceiling_mbps = rate_after_decrease / (1.0 - decrease_factor);
        // Verify the rate is consistent with a 0.30 decrease from ceiling
        assert!(
            (rate_after_decrease - implied_ceiling_mbps * 0.70).abs() < 0.01,
            "rate should be ceiling * 0.70, got {rate_after_decrease:.4}"
        );

        // Healthy upload → additive increase of exactly 2 MB/s
        th.record_upload(100_000, Duration::from_millis(200)).await;
        let rate_after_increase = th.current_rate_mbps().unwrap();
        let increase_mbps = increase_bytes / 1_000_000.0;
        let expected = (rate_after_decrease + increase_mbps).min(implied_ceiling_mbps);
        assert!(
            (rate_after_increase - expected).abs() < 0.01,
            "additive increase should add {increase_mbps} MB/s (capped at ceiling): \
             expected ~{expected:.4}, got {rate_after_increase:.4}"
        );
    }

    #[tokio::test]
    async fn pipeline_handles_429_and_recovers() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // First 3 requests → 429 with Retry-After header
        Mock::given(method("POST"))
            .and(path("/api/v1/logs"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "0.05"),
            )
            .up_to_n_times(3)
            .with_priority(1) // Higher priority, consumed first
            .mount(&mock_server)
            .await;

        // Subsequent requests → 200
        Mock::given(method("POST"))
            .and(path("/api/v1/logs"))
            .respond_with(ResponseTemplate::new(200))
            .with_priority(2) // Lower priority, used after 429s exhausted
            .mount(&mock_server)
            .await;

        let config = HttpWriterConfig {
            url: format!("{}/api/v1/logs", mock_server.uri()),
            api_key: "test-key".to_string(),
            batch_max_bytes: 4 * 1024, // 4KB — small batches flush quickly
            timeout_secs: 5,
            max_retries: 5,
            channel_buffer_size: 10,
            num_compressor_tasks: 1,
            num_upload_tasks: 1,
            upload_channel_size: 4,
            compression_level: 1,
            // Enable AIMD so force_decrease fires on 429
            max_submission_time: Some(Duration::from_secs(3)),
            max_upload_rate: None,
            aimd_decrease_factor: 0.15,
            aimd_increase_bytes: 1_000_000.0,
        };

        let writer = HttpResultWriter::new(config).expect("writer creation should succeed");
        let sender = writer.get_sender();

        // Send 50 lines
        let num_lines = 50;
        for i in 0..num_lines {
            sender
                .send_async(format!("{{\"msg\": \"test line {i}\"}}\n"))
                .await
                .expect("send should succeed");
        }
        drop(sender);

        let stats = writer.finish().await.expect("finish should succeed");

        assert_eq!(
            stats.lines_sent, num_lines,
            "all lines should be delivered: sent={}, expected={}",
            stats.lines_sent, num_lines
        );
        assert_eq!(
            stats.lines_dropped, 0,
            "no lines should be dropped: dropped={}",
            stats.lines_dropped
        );

        // Verify the mock server received more requests than it would have without 429s
        // (the 3 rejected + at least 1 successful = at least 4 requests)
        let received = mock_server.received_requests().await.unwrap();
        assert!(
            received.len() > 3,
            "mock server should have received retried requests: got {} requests",
            received.len()
        );
    }

    #[tokio::test]
    async fn throttle_static_limit_with_aimd() {
        // 10 MB/s static limit + AIMD with 1s threshold
        let limit = 10.0 * 1_000_000.0;
        let mst = Duration::from_secs_f64(1.0);
        let th = UploadThrottle::new(Some(mst), 1024, Some(limit), 0.15, 1_000_000.0);

        // Starts at static limit
        assert!(
            (th.current_rate_mbps().unwrap() - 10.0).abs() < 0.01,
            "should start at static limit"
        );

        // Build up bytes with healthy uploads
        for _ in 0..5 {
            th.record_upload(100_000, Duration::from_millis(200)).await;
        }
        // Rate should still be at static limit (additive increase capped by ceiling)
        assert!(
            (th.current_rate_mbps().unwrap() - 10.0).abs() < 0.01,
            "should stay at static limit during healthy uploads"
        );

        // Trigger congestion → rate should drop below static limit
        th.record_upload(100_000, Duration::from_secs(2)).await;
        let rate_after_congestion = th.current_rate_mbps().unwrap();
        assert!(
            rate_after_congestion < 10.0,
            "AIMD should decrease below static limit, got {}",
            rate_after_congestion
        );
    }
}
