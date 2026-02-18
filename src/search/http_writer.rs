// src/search/http_writer.rs
use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

use super::result_exporter::SearchExporter;

/// Configuration for the HTTP writer
#[derive(Debug, Clone)]
pub struct HttpWriterConfig {
    /// The URL to send logs to (e.g., https://intake.handy-mango.http.com/api/v1/logs)
    pub url: String,
    /// API key for authentication
    pub api_key: String,
    /// Maximum batch size in bytes. Default limit is 30MB.
    pub batch_max_bytes: usize,
    /// Timeout for HTTP requests in seconds
    pub timeout_secs: u64,
    /// Maximum retry attempts for failed requests
    pub max_retries: u32,
    /// Channel buffer size for backpressure control
    pub channel_buffer_size: usize,
    /// Number of concurrent upload tasks consuming from the shared channel
    pub num_upload_tasks: usize,
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
        }
    }
}

/// Stats returned by `HttpResultWriter::finish()`
#[derive(Debug, Clone)]
pub struct HttpWriterStats {
    pub lines_sent: usize,
    pub lines_dropped: usize,
}

/// Manages streaming writes of search results to an HTTP API
pub struct HttpResultWriter {
    write_tx: flume::Sender<String>,
    write_handles: Vec<tokio::task::JoinHandle<usize>>,
    /// Tracks total lines sent (shared with writer tasks)
    lines_sent: Arc<AtomicUsize>,
    /// Tracks total lines dropped on batch failures (shared with writer tasks)
    lines_dropped: Arc<AtomicUsize>,
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
        let url = config.url.clone();
        let num_tasks = config.num_upload_tasks;

        let mut write_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let rx = write_rx.clone();
            let client = client.clone();
            let cfg = config.clone();
            let ls = lines_sent.clone();
            let ld = lines_dropped.clone();
            let handle = tokio::spawn(async move {
                Self::writer_task(task_id, rx, client, cfg, ls, ld).await
            });
            write_handles.push(handle);
        }

        Ok(Self {
            write_tx,
            write_handles,
            lines_sent,
            lines_dropped,
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

    /// Background task that batches and sends lines to the HTTP API.
    /// Multiple instances run concurrently, each pulling from the shared channel.
    async fn writer_task(
        task_id: usize,
        write_rx: flume::Receiver<String>,
        client: Client,
        config: HttpWriterConfig,
        lines_sent: Arc<AtomicUsize>,
        lines_dropped: Arc<AtomicUsize>,
    ) -> usize {
        let mut batch_body = String::with_capacity(config.batch_max_bytes);
        let mut batch_lines = 0usize;
        let mut total_sent = 0usize;
        let mut batches_sent: u64 = 0;
        let mut last_stats_log = Instant::now();

        while let Ok(line) = write_rx.recv_async().await {
            let line_bytes = line.len();

            // Flush if adding this line would exceed the byte limit
            if batch_lines > 0 && (batch_body.len() + line_bytes > config.batch_max_bytes) {
                match Self::send_batch(&client, &config, &batch_body, batch_lines).await {
                    Ok(count) => {
                        total_sent += count;
                        batches_sent += 1;
                        lines_sent.fetch_add(count, Ordering::Relaxed);
                        debug!(task = task_id, lines = count, bytes = batch_body.len(), url = %config.url, "Sent batch");
                    }
                    Err(e) => {
                        lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                        error!(task = task_id, lines = batch_lines, bytes = batch_body.len(), error = %e, "Failed to send batch");
                    }
                }
                batch_body.clear();
                batch_lines = 0;
            }

            batch_body.push_str(&line);
            batch_lines += 1;

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
        }

        // Send remaining items in the final batch
        if batch_lines > 0 {
            match Self::send_batch(&client, &config, &batch_body, batch_lines).await {
                Ok(count) => {
                    total_sent += count;
                    lines_sent.fetch_add(count, Ordering::Relaxed);
                    info!(task = task_id, lines = count, url = %config.url, "Sent final batch");
                }
                Err(e) => {
                    lines_dropped.fetch_add(batch_lines, Ordering::Relaxed);
                    error!(task = task_id, lines = batch_lines, bytes = batch_body.len(), error = %e, "Failed to send final batch");
                }
            }
        }

        info!(task = task_id, total_lines = total_sent, batches_sent = batches_sent, "HTTP writer task finished");
        total_sent
    }

    /// Send a batch of log lines to the HTTP API.
    /// `body` is a pre-built NDJSON string; `count` is the number of lines it contains.
    async fn send_batch(
        client: &Client,
        config: &HttpWriterConfig,
        body: &str,
        count: usize,
    ) -> Result<usize> {
        // Convert to Bytes once — clone() is an O(1) Arc bump, not a full copy.
        let body_bytes = Bytes::from(body.to_owned());
        let body_len = body_bytes.len();

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
                .header("Authorization", format!("Bearer {}", config.api_key))
                .body(body_bytes.clone())
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    let response_body = response.text().await.unwrap_or_default();

                    if status.is_success() {
                        debug!(
                            lines = count,
                            bytes = body_len,
                            status = %status,
                            response = %response_body,
                            "HTTP batch sent"
                        );
                        trace!(payload = %std::str::from_utf8(&body_bytes).unwrap_or("<invalid utf8>"), "HTTP batch payload");
                        return Ok(count);
                    } else {
                        last_error = Some(anyhow::anyhow!(
                            "HTTP {status} from API: {response_body}"
                        ));

                        // Don't retry on 4xx errors (client errors)
                        if status.is_client_error() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("HTTP request failed: {e}"));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error sending batch")))
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
