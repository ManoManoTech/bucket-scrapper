// src/search/http_writer.rs
use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::Client;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use super::result_exporter::SearchExporter;

/// A zstd-compressed batch ready for HTTP upload.
struct CompressedBatch {
    body: Bytes,
    lines: usize,
    plaintext_bytes: usize,
}

/// Read-only view of pipeline channel fill levels.
///
/// Cloning the underlying senders is cheap (Arc bump) and does not interfere
/// with backpressure — senders only block on `send`, not on creation.
pub struct PipelineObserver {
    line_tx: flume::Sender<String>,
    batch_tx: flume::Sender<CompressedBatch>,
    batches_uploaded: Arc<AtomicUsize>,
    upload_time_us: Arc<AtomicUsize>,
    compressed_bytes_sent: Arc<AtomicUsize>,
}

impl PipelineObserver {
    pub fn line_len(&self) -> usize {
        self.line_tx.len()
    }

    pub fn line_capacity(&self) -> usize {
        self.line_tx.capacity().unwrap_or(0)
    }

    pub fn batch_len(&self) -> usize {
        self.batch_tx.len()
    }

    pub fn batch_capacity(&self) -> usize {
        self.batch_tx.capacity().unwrap_or(0)
    }

    pub fn batches_uploaded(&self) -> usize {
        self.batches_uploaded.load(Ordering::Relaxed)
    }

    pub fn compressed_bytes_sent(&self) -> usize {
        self.compressed_bytes_sent.load(Ordering::Relaxed)
    }

    /// Average batch upload time in milliseconds, or 0.0 if no batches yet.
    pub fn avg_upload_ms(&self) -> f64 {
        let count = self.batches_uploaded();
        if count == 0 {
            return 0.0;
        }
        let total_us = self.upload_time_us.load(Ordering::Relaxed) as f64;
        total_us / count as f64 / 1000.0
    }

    /// Heuristic bottleneck indicator based on channel fill %.
    ///
    /// - `batch_ch > 80%` → **upload** (uploaders can't drain fast enough)
    /// - `line_ch > 80%`  → **compress** (compressors can't keep up)
    /// - `line_ch < 20%`  → **download** (searchers aren't producing fast enough)
    /// - otherwise        → **-** (balanced)
    pub fn bottleneck(&self) -> &'static str {
        let batch_cap = self.batch_capacity().max(1);
        let line_cap = self.line_capacity().max(1);
        let batch_pct = self.batch_len() * 100 / batch_cap;
        let line_pct = self.line_len() * 100 / line_cap;

        if batch_pct > 80 {
            "upload"
        } else if line_pct > 80 {
            "compress"
        } else if line_pct < 20 {
            "download"
        } else {
            "-"
        }
    }
}

/// Result of a batch send attempt.
enum SendResult {
    /// Batch was accepted by the server.
    Ok,
    /// Retryable failure (server error, timeout) — log and continue with next batch.
    Retryable(anyhow::Error),
    /// Fatal failure (4xx client error) — stop the entire pipeline.
    Fatal(anyhow::Error),
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

/// Manages streaming writes of search results to an HTTP API
pub struct HttpResultWriter {
    write_tx: flume::Sender<String>,
    /// Kept for `observer()` — dropped in `finish()` before joining compressors
    batch_tx: flume::Sender<CompressedBatch>,
    compressor_handles: Vec<tokio::task::JoinHandle<()>>,
    upload_handles: Vec<tokio::task::JoinHandle<usize>>,
    /// Tracks total lines sent (shared with uploader tasks)
    lines_sent: Arc<AtomicUsize>,
    /// Tracks total lines dropped on batch failures (shared with tasks)
    lines_dropped: Arc<AtomicUsize>,
    /// Tracks total compressed bytes sent (shared with uploader tasks)
    compressed_bytes_sent: Arc<AtomicUsize>,
    /// Tracks total plaintext bytes sent (shared with uploader tasks)
    plaintext_bytes_sent: Arc<AtomicUsize>,
    /// Tracks total batches successfully uploaded (shared with uploader tasks)
    batches_uploaded: Arc<AtomicUsize>,
    /// Tracks cumulative upload time in microseconds (shared with uploader tasks)
    upload_time_us: Arc<AtomicUsize>,
    /// Set on fatal (4xx) errors to stop the entire pipeline
    fatal_error: Arc<AtomicBool>,
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

        let lines_sent = Arc::new(AtomicUsize::new(0));
        let lines_dropped = Arc::new(AtomicUsize::new(0));
        let compressed_bytes_sent = Arc::new(AtomicUsize::new(0));
        let plaintext_bytes_sent = Arc::new(AtomicUsize::new(0));
        let batches_uploaded = Arc::new(AtomicUsize::new(0));
        let upload_time_us = Arc::new(AtomicUsize::new(0));
        let fatal_error = Arc::new(AtomicBool::new(false));
        let url = config.url.clone();

        // Spawn compressor tasks (CPU-bound: line_rx → batch_tx)
        let mut compressor_handles = Vec::with_capacity(config.num_compressor_tasks);
        for task_id in 0..config.num_compressor_tasks {
            let rx = line_rx.clone();
            let tx = batch_tx.clone();
            let cfg = config.clone();
            let ld = lines_dropped.clone();
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
            let ls = lines_sent.clone();
            let ld = lines_dropped.clone();
            let cb = compressed_bytes_sent.clone();
            let pb = plaintext_bytes_sent.clone();
            let bu = batches_uploaded.clone();
            let ut = upload_time_us.clone();
            let fe = fatal_error.clone();
            let handle = tokio::spawn(async move {
                Self::uploader_task(task_id, rx, client, cfg, ls, ld, cb, pb, bu, ut, fe).await
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
            lines_sent,
            lines_dropped,
            compressed_bytes_sent,
            plaintext_bytes_sent,
            batches_uploaded,
            upload_time_us,
            fatal_error,
            url,
        })
    }

    /// Get a sender that can be cloned for use in multiple tasks
    pub fn get_sender(&self) -> flume::Sender<String> {
        self.write_tx.clone()
    }

    /// Get a read-only observer for pipeline channel stats.
    pub fn observer(&self) -> PipelineObserver {
        PipelineObserver {
            line_tx: self.write_tx.clone(),
            batch_tx: self.batch_tx.clone(),
            batches_uploaded: self.batches_uploaded.clone(),
            upload_time_us: self.upload_time_us.clone(),
            compressed_bytes_sent: self.compressed_bytes_sent.clone(),
        }
    }

    /// Get current lines sent count
    pub fn lines_sent(&self) -> usize {
        self.lines_sent.load(Ordering::Relaxed)
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
    #[allow(clippy::too_many_arguments)]
    async fn uploader_task(
        task_id: usize,
        batch_rx: flume::Receiver<CompressedBatch>,
        client: Client,
        config: HttpWriterConfig,
        lines_sent: Arc<AtomicUsize>,
        lines_dropped: Arc<AtomicUsize>,
        compressed_bytes_sent: Arc<AtomicUsize>,
        plaintext_bytes_sent: Arc<AtomicUsize>,
        batches_uploaded: Arc<AtomicUsize>,
        upload_time_us: Arc<AtomicUsize>,
        fatal_error: Arc<AtomicBool>,
    ) -> usize {
        let mut total_sent = 0usize;
        let mut batches_sent: u64 = 0;
        let mut last_stats_log = Instant::now();

        while let Ok(batch) = batch_rx.recv_async().await {
            let batch_compressed_bytes = batch.body.len();

            let send_start = Instant::now();
            match Self::send_batch(&client, &config, &batch.body, batch.lines).await {
                SendResult::Ok => {
                    let elapsed_us = send_start.elapsed().as_micros() as usize;
                    total_sent += batch.lines;
                    batches_sent += 1;
                    lines_sent.fetch_add(batch.lines, Ordering::Relaxed);
                    compressed_bytes_sent.fetch_add(batch_compressed_bytes, Ordering::Relaxed);
                    plaintext_bytes_sent.fetch_add(batch.plaintext_bytes, Ordering::Relaxed);
                    batches_uploaded.fetch_add(1, Ordering::Relaxed);
                    upload_time_us.fetch_add(elapsed_us, Ordering::Relaxed);
                    debug!(
                        task = task_id,
                        lines = batch.lines,
                        compressed_bytes = batch_compressed_bytes,
                        plaintext_bytes = batch.plaintext_bytes,
                        upload_ms = send_start.elapsed().as_millis() as u64,
                        url = %config.url,
                        "Sent batch"
                    );
                }
                SendResult::Retryable(e) => {
                    lines_dropped.fetch_add(batch.lines, Ordering::Relaxed);
                    error!(
                        task = task_id,
                        lines = batch.lines,
                        compressed_bytes = batch_compressed_bytes,
                        error = %e,
                        "Failed to send batch"
                    );
                }
                SendResult::Fatal(e) => {
                    lines_dropped.fetch_add(batch.lines, Ordering::Relaxed);
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
            lines_dropped: self.lines_dropped.load(Ordering::Relaxed),
            compressed_bytes_sent: self.compressed_bytes_sent.load(Ordering::Relaxed),
            plaintext_bytes_sent: self.plaintext_bytes_sent.load(Ordering::Relaxed),
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

    /// Get the configured URL
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// An exporter that streams results to an HTTP API instead of storing in memory.
/// Implements SearchExporter trait for use with generic search functions.
pub struct HttpStreamingExporter {
    sender: flume::Sender<String>,
    match_count: usize,
}

impl HttpStreamingExporter {
    pub fn new(sender: flume::Sender<String>) -> Self {
        Self {
            sender,
            match_count: 0,
        }
    }
}

impl SearchExporter for HttpStreamingExporter {
    fn add_match(&mut self, line: &str) -> Result<()> {
        match self.sender.send(line.to_string()) {
            Ok(()) => {
                self.match_count += 1;
                Ok(())
            }
            Err(_) => {
                Err(anyhow::anyhow!("HTTP consumer gone, channel closed"))
            }
        }
    }

    fn match_count(&self) -> usize {
        self.match_count
    }
}
