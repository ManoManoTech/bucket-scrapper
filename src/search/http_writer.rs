// src/search/http_writer.rs
use anyhow::{Context, Result};
use async_compression::tokio::write::ZstdEncoder;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, warn};

use super::result_exporter::SearchExporter;

/// Wraps a `DuplexStream` write-half, counting bytes written through it.
struct CountedWriter {
    inner: DuplexStream,
    counter: Arc<AtomicUsize>,
}

impl CountedWriter {
    fn new(inner: DuplexStream, counter: Arc<AtomicUsize>) -> Self {
        Self { inner, counter }
    }
}

impl AsyncWrite for CountedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let pin = Pin::new(&mut self.inner);
        match pin.poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.counter.fetch_add(n, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
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
    /// Channel buffer size for backpressure control
    pub channel_buffer_size: usize,
    /// Number of concurrent upload tasks consuming from the shared channel
    pub num_upload_tasks: usize,
    /// Zstd compression level (1-22)
    pub compression_level: i32,
}

impl Default for HttpWriterConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: String::new(),
            batch_max_bytes: 2 * 1024 * 1024, // 2MB default
            timeout_secs: 30,
            max_retries: 3,
            channel_buffer_size: 1000,
            num_upload_tasks: std::thread::available_parallelism()
                .map(|n| n.get() / 8)
                .unwrap_or(1)
                .max(1),
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
    write_handles: Vec<tokio::task::JoinHandle<usize>>,
    /// Tracks total lines sent (shared with writer tasks)
    lines_sent: Arc<AtomicUsize>,
    /// Tracks total lines dropped on batch failures (shared with writer tasks)
    lines_dropped: Arc<AtomicUsize>,
    /// Tracks total compressed bytes sent (shared with writer tasks)
    compressed_bytes_sent: Arc<AtomicUsize>,
    /// Tracks total plaintext bytes sent (shared with writer tasks)
    plaintext_bytes_sent: Arc<AtomicUsize>,
    /// URL for display purposes
    url: String,
}

impl HttpResultWriter {
    /// Create a new HTTP writer with N concurrent upload tasks
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

        let (write_tx, write_rx) = flume::bounded::<String>(config.channel_buffer_size);
        let lines_sent = Arc::new(AtomicUsize::new(0));
        let lines_dropped = Arc::new(AtomicUsize::new(0));
        let compressed_bytes_sent = Arc::new(AtomicUsize::new(0));
        let plaintext_bytes_sent = Arc::new(AtomicUsize::new(0));
        let url = config.url.clone();
        let num_tasks = config.num_upload_tasks;

        let mut write_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let rx = write_rx.clone();
            let client = client.clone();
            let cfg = config.clone();
            let ls = lines_sent.clone();
            let ld = lines_dropped.clone();
            let cb = compressed_bytes_sent.clone();
            let pb = plaintext_bytes_sent.clone();
            let handle = tokio::spawn(async move {
                Self::writer_task(task_id, rx, client, cfg, ls, ld, cb, pb).await
            });
            write_handles.push(handle);
        }

        Ok(Self {
            write_tx,
            write_handles,
            lines_sent,
            lines_dropped,
            compressed_bytes_sent,
            plaintext_bytes_sent,
            url,
        })
    }

    /// Get a sender that can be cloned for use in multiple tasks
    pub fn get_sender(&self) -> flume::Sender<String> {
        self.write_tx.clone()
    }

    /// Get current lines sent count
    pub fn lines_sent(&self) -> usize {
        self.lines_sent.load(Ordering::Relaxed)
    }

    /// Background task that batches and sends lines to the HTTP API with zstd compression.
    /// Multiple instances run concurrently, each pulling from the shared channel.
    ///
    /// Each batch cycle:
    /// 1. Wait for first line (or exit if channel closed)
    /// 2. Set up pipeline: duplex → CountedWriter → ZstdEncoder
    ///    Tee stream from read-half captures data into retry_buffer; spawn POST task
    /// 3. Write lines to encoder until compressed_counter >= batch_max_bytes or channel closed
    /// 4. Finalize zstd frame (encoder.shutdown()), close write-half → POST sees EOF
    /// 5. Await POST response; on failure, retry from buffered bytes
    #[allow(clippy::too_many_arguments)]
    async fn writer_task(
        task_id: usize,
        write_rx: flume::Receiver<String>,
        client: Client,
        config: HttpWriterConfig,
        lines_sent: Arc<AtomicUsize>,
        lines_dropped: Arc<AtomicUsize>,
        compressed_bytes_sent: Arc<AtomicUsize>,
        plaintext_bytes_sent: Arc<AtomicUsize>,
    ) -> usize {
        let mut total_sent = 0usize;
        let mut batches_sent: u64 = 0;
        let mut last_stats_log = Instant::now();
        let mut channel_closed = false;

        'outer: loop {
            // 1. Wait for first line (or break if channel closed)
            let first_line = match write_rx.recv_async().await {
                Ok(line) => line,
                Err(_) => break 'outer,
            };

            // 2. Set up pipeline: duplex → CountedWriter → ZstdEncoder
            //    read-half → ReaderStream → tee → reqwest Body
            let compressed_counter = Arc::new(AtomicUsize::new(0));
            let (write_half, read_half) = tokio::io::duplex(128 * 1024);
            let counted = CountedWriter::new(write_half, compressed_counter.clone());
            let mut encoder = ZstdEncoder::with_quality(counted, async_compression::Level::Precise(config.compression_level));

            let retry_buffer = Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new()));
            let tee_buf = retry_buffer.clone();
            let tee_stream = ReaderStream::new(read_half).map(move |chunk_result| {
                match chunk_result {
                    Ok(chunk) => {
                        // Tee: copy chunk into retry buffer (best-effort, non-blocking lock)
                        let buf = tee_buf.clone();
                        let bytes = chunk.clone();
                        tokio::spawn(async move {
                            buf.lock().await.extend_from_slice(&bytes);
                        });
                        Ok(chunk)
                    }
                    Err(e) => Err(e),
                }
            });

            let body = reqwest::Body::wrap_stream(tee_stream);
            let send_future = client
                .post(&config.url)
                .header("Content-Type", "application/x-ndjson")
                .header("Content-Encoding", "zstd")
                .header("Authorization", format!("Bearer {}", config.api_key))
                .body(body)
                .send();
            let send_handle = tokio::spawn(send_future);

            // 3. Write first line to encoder
            let mut batch_lines = 0usize;
            let mut batch_plaintext_bytes = 0usize;

            let first_bytes = first_line.as_bytes();
            batch_plaintext_bytes += first_bytes.len();
            if let Err(e) = encoder.write_all(first_bytes).await {
                error!(task = task_id, error = %e, "Failed to write to zstd encoder");
                // Encoder write failed — POST task will see broken pipe
                let _ = encoder.shutdown().await;
                drop(encoder);
                let _ = send_handle.await;
                lines_dropped.fetch_add(1, Ordering::Relaxed);
                continue 'outer;
            }
            batch_lines += 1;

            // 4. Inner loop: recv line, write to encoder, check compressed_counter
            loop {
                // Check if batch is full (compressed bytes)
                if compressed_counter.load(Ordering::Relaxed) >= config.batch_max_bytes {
                    break;
                }

                // Non-blocking try first for throughput, then async recv with a small timeout
                // to allow checking compressed counter periodically
                let line = match write_rx.try_recv() {
                    Ok(line) => line,
                    Err(flume::TryRecvError::Empty) => {
                        // Brief async wait — allows checking compressed size regularly
                        match tokio::time::timeout(
                            Duration::from_millis(50),
                            write_rx.recv_async(),
                        )
                        .await
                        {
                            Ok(Ok(line)) => line,
                            Ok(Err(_)) => {
                                channel_closed = true;
                                break;
                            }
                            Err(_) => continue, // timeout, re-check compressed counter
                        }
                    }
                    Err(flume::TryRecvError::Disconnected) => {
                        channel_closed = true;
                        break;
                    }
                };

                let line_bytes = line.as_bytes();
                batch_plaintext_bytes += line_bytes.len();
                if let Err(e) = encoder.write_all(line_bytes).await {
                    // BrokenPipe means POST task failed mid-stream
                    if e.kind() == std::io::ErrorKind::BrokenPipe {
                        warn!(task = task_id, "POST connection closed mid-stream (broken pipe)");
                    } else {
                        error!(task = task_id, error = %e, "Failed to write to zstd encoder");
                    }
                    // Lines already written are in the pipe; this line is lost
                    lines_dropped.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                batch_lines += 1;
            }

            // 5. Finalize zstd frame — encoder.shutdown() flushes and writes end marker
            if let Err(e) = encoder.shutdown().await {
                if e.kind() != std::io::ErrorKind::BrokenPipe {
                    warn!(task = task_id, error = %e, "Failed to finalize zstd frame");
                }
            }
            // 6. Drop encoder to close the write-half of the duplex → POST sees EOF
            drop(encoder);

            // 7. Await POST response
            let batch_compressed_bytes = compressed_counter.load(Ordering::Relaxed);
            match send_handle.await {
                Ok(Ok(response)) => {
                    let status = response.status();
                    let response_body = response.text().await.unwrap_or_default();

                    if status.is_success() {
                        total_sent += batch_lines;
                        batches_sent += 1;
                        lines_sent.fetch_add(batch_lines, Ordering::Relaxed);
                        compressed_bytes_sent.fetch_add(batch_compressed_bytes, Ordering::Relaxed);
                        plaintext_bytes_sent.fetch_add(batch_plaintext_bytes, Ordering::Relaxed);
                        debug!(
                            task = task_id,
                            lines = batch_lines,
                            compressed_bytes = batch_compressed_bytes,
                            plaintext_bytes = batch_plaintext_bytes,
                            status = %status,
                            response = %response_body,
                            url = %config.url,
                            "Sent batch"
                        );
                    } else if status.is_client_error() {
                        // 4xx — don't retry, log and drop
                        lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                        error!(
                            task = task_id,
                            lines = batch_lines,
                            status = %status,
                            response = %response_body,
                            "Batch rejected (client error, not retrying)"
                        );
                    } else {
                        // 5xx — retry from buffer
                        Self::retry_from_buffer(
                            task_id,
                            &client,
                            &config,
                            &retry_buffer,
                            batch_compressed_bytes,
                            batch_lines,
                            batch_plaintext_bytes,
                            &mut total_sent,
                            &mut batches_sent,
                            &lines_sent,
                            &lines_dropped,
                            &compressed_bytes_sent,
                            &plaintext_bytes_sent,
                            status,
                            &response_body,
                        )
                        .await;
                    }
                }
                Ok(Err(e)) => {
                    // Network error — retry from buffer
                    Self::retry_from_buffer(
                        task_id,
                        &client,
                        &config,
                        &retry_buffer,
                        batch_compressed_bytes,
                        batch_lines,
                        batch_plaintext_bytes,
                        &mut total_sent,
                        &mut batches_sent,
                        &lines_sent,
                        &lines_dropped,
                        &compressed_bytes_sent,
                        &plaintext_bytes_sent,
                        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("HTTP request failed: {e}"),
                    )
                    .await;
                }
                Err(e) => {
                    // JoinError — task panicked
                    lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                    error!(task = task_id, error = %e, "HTTP send task panicked");
                }
            }

            if last_stats_log.elapsed() > Duration::from_secs(30) {
                debug!(
                    task = task_id,
                    channel_len = write_rx.len(),
                    channel_full = write_rx.is_full(),
                    batches_sent = batches_sent,
                    "Upload task stats"
                );
                last_stats_log = Instant::now();
            }

            if channel_closed {
                break 'outer;
            }
        }

        info!(task = task_id, total_lines = total_sent, batches_sent = batches_sent, "HTTP writer task finished");
        total_sent
    }

    /// Retry a failed batch from the tee buffer.
    #[allow(clippy::too_many_arguments)]
    async fn retry_from_buffer(
        task_id: usize,
        client: &Client,
        config: &HttpWriterConfig,
        retry_buffer: &tokio::sync::Mutex<Vec<u8>>,
        batch_compressed_bytes: usize,
        batch_lines: usize,
        batch_plaintext_bytes: usize,
        total_sent: &mut usize,
        batches_sent: &mut u64,
        lines_sent: &Arc<AtomicUsize>,
        lines_dropped: &Arc<AtomicUsize>,
        compressed_bytes_sent: &Arc<AtomicUsize>,
        plaintext_bytes_sent: &Arc<AtomicUsize>,
        original_status: reqwest::StatusCode,
        original_error: &str,
    ) {
        let buffer = retry_buffer.lock().await;
        if buffer.len() < batch_compressed_bytes {
            // Incomplete buffer — can't retry safely
            lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
            error!(
                task = task_id,
                lines = batch_lines,
                buffer_len = buffer.len(),
                expected = batch_compressed_bytes,
                status = %original_status,
                error = %original_error,
                "Batch failed, retry buffer incomplete — dropping"
            );
            return;
        }

        let body_bytes = Bytes::from(buffer.clone());
        drop(buffer);

        for attempt in 1..=config.max_retries {
            let delay = Duration::from_millis(100 * 2u64.pow(attempt - 1));
            warn!(
                task = task_id,
                attempt = attempt,
                max_attempts = config.max_retries,
                delay_ms = delay.as_millis() as u64,
                "Retrying batch from buffer"
            );
            tokio::time::sleep(delay).await;

            let result = client
                .post(&config.url)
                .header("Content-Type", "application/x-ndjson")
                .header("Content-Encoding", "zstd")
                .header("Authorization", format!("Bearer {}", config.api_key))
                .body(body_bytes.clone())
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    let resp_body = response.text().await.unwrap_or_default();

                    if status.is_success() {
                        *total_sent += batch_lines;
                        *batches_sent += 1;
                        lines_sent.fetch_add(batch_lines, Ordering::Relaxed);
                        compressed_bytes_sent.fetch_add(batch_compressed_bytes, Ordering::Relaxed);
                        plaintext_bytes_sent.fetch_add(batch_plaintext_bytes, Ordering::Relaxed);
                        debug!(
                            task = task_id,
                            lines = batch_lines,
                            attempt = attempt,
                            "Retry succeeded"
                        );
                        return;
                    }
                    if status.is_client_error() {
                        // 4xx on retry — stop retrying
                        lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                        error!(
                            task = task_id,
                            lines = batch_lines,
                            status = %status,
                            response = %resp_body,
                            "Retry got client error, giving up"
                        );
                        return;
                    }
                    // 5xx — continue retrying
                }
                Err(_) => {
                    // Network error — continue retrying
                }
            }
        }

        // All retries exhausted
        lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
        error!(
            task = task_id,
            lines = batch_lines,
            attempts = config.max_retries,
            "All retries exhausted, dropping batch"
        );
    }

    /// Finish writing and return stats for all tasks
    pub async fn finish(self) -> Result<HttpWriterStats> {
        // Close the channel to signal all writer tasks to drain and exit
        drop(self.write_tx);

        let mut total = 0usize;

        for handle in self.write_handles {
            match handle.await {
                Ok(count) => total += count,
                Err(e) => {
                    return Err(anyhow::anyhow!("Writer task panicked: {e}"));
                }
            }
        }

        Ok(HttpWriterStats {
            lines_sent: total,
            lines_dropped: self.lines_dropped.load(Ordering::Relaxed),
            compressed_bytes_sent: self.compressed_bytes_sent.load(Ordering::Relaxed),
            plaintext_bytes_sent: self.plaintext_bytes_sent.load(Ordering::Relaxed),
        })
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
