// src/search/http_writer.rs
use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::result_collector::SearchCollector;

/// A match to be sent to the HTTP API
#[derive(Debug, Clone)]
pub struct HttpMatchToSend {
    pub bucket: String,
    pub key: String,
    pub content: String,
}

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
    /// Hostname to include in log entries (defaults to machine hostname)
    pub hostname: String,
    /// Service name to include in log entries
    pub service: String,
    /// Channel buffer size for backpressure control
    pub channel_buffer_size: usize,
}

impl Default for HttpWriterConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: String::new(),
            batch_max_bytes: 2 * 1024 * 1024, // 2MB default
            timeout_secs: 30,
            max_retries: 3,
            hostname: gethostname::gethostname()
                .to_string_lossy()
                .to_string(),
            service: "bucket-scrapper".to_string(),
            channel_buffer_size: 1000,
        }
    }
}

/// Manages streaming writes of search results to an HTTP API
pub struct HttpResultWriter {
    write_tx: mpsc::Sender<HttpMatchToSend>,
    write_handle: Option<tokio::task::JoinHandle<Result<usize>>>,
    /// Tracks total lines sent (shared with writer task)
    lines_sent: Arc<AtomicUsize>,
    /// Tracks files searched
    files_searched: Arc<AtomicUsize>,
    /// URL for display purposes
    url: String,
}

impl HttpResultWriter {
    /// Create a new HTTP writer
    pub fn new(config: HttpWriterConfig) -> Result<Self> {
        let (write_tx, write_rx) = mpsc::channel::<HttpMatchToSend>(config.channel_buffer_size);
        let lines_sent = Arc::new(AtomicUsize::new(0));
        let lines_sent_clone = lines_sent.clone();
        let url = config.url.clone();

        let write_handle = tokio::spawn(async move {
            Self::writer_task(write_rx, config, lines_sent_clone).await
        });

        Ok(Self {
            write_tx,
            write_handle: Some(write_handle),
            lines_sent,
            files_searched: Arc::new(AtomicUsize::new(0)),
            url,
        })
    }

    /// Get a sender that can be cloned for use in multiple tasks
    pub fn get_sender(&self) -> mpsc::Sender<HttpMatchToSend> {
        self.write_tx.clone()
    }

    /// Get current lines sent count
    pub fn lines_sent(&self) -> usize {
        self.lines_sent.load(Ordering::Relaxed)
    }

    /// Increment files searched counter
    pub fn mark_file_searched(&self) {
        self.files_searched.fetch_add(1, Ordering::Relaxed);
    }

    /// Get files searched count
    pub fn files_searched(&self) -> usize {
        self.files_searched.load(Ordering::Relaxed)
    }

    /// Background task that batches and sends matches to the HTTP API
    async fn writer_task(
        mut write_rx: mpsc::Receiver<HttpMatchToSend>,
        config: HttpWriterConfig,
        lines_sent: Arc<AtomicUsize>,
    ) -> Result<usize> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .context("Failed to create HTTP client")?;

        let mut batch: Vec<String> = Vec::new();
        let mut batch_bytes = 0usize;
        let mut total_sent = 0usize;

        while let Some(match_item) = write_rx.recv().await {
            // Format as JSON line per HTTP API spec
            // Required: message, hostname, service
            // Optional: level, custom attributes (file, bucket, key)
            let log_line = json!({
                "message": match_item.content.trim(),
                "hostname": config.hostname,
                "service": config.service,
                "timestamp": Utc::now().to_rfc3339(),
                "file": format!("{}/{}", match_item.bucket, match_item.key),
                "bucket": match_item.bucket,
                "key": match_item.key
            });
            let log_line_str = log_line.to_string();
            let line_bytes = log_line_str.len() + 1; // +1 for newline

            // Flush if adding this line would exceed the byte limit
            if !batch.is_empty() && (batch_bytes + line_bytes > config.batch_max_bytes) {
                match Self::send_batch(&client, &config, &batch).await {
                    Ok(count) => {
                        total_sent += count;
                        lines_sent.fetch_add(count, Ordering::Relaxed);
                        debug!("Sent batch of {} log lines ({} bytes) to {}", count, batch_bytes, config.url);
                    }
                    Err(e) => {
                        error!("Failed to send batch ({} lines, {} bytes): {}", batch.len(), batch_bytes, e);
                    }
                }
                batch.clear();
                batch_bytes = 0;
            }

            batch.push(log_line_str);
            batch_bytes += line_bytes;
        }

        // Send remaining items in the final batch
        if !batch.is_empty() {
            let final_batch_bytes = batch_bytes;
            let final_batch_lines = batch.len();
            match Self::send_batch(&client, &config, &batch).await {
                Ok(count) => {
                    total_sent += count;
                    lines_sent.fetch_add(count, Ordering::Relaxed);
                    info!("Sent final batch of {} log lines to {}", count, config.url);
                }
                Err(e) => {
                    error!("Failed to send final batch ({} lines, {} bytes): {}", final_batch_lines, final_batch_bytes, e);
                }
            }
        }

        info!("HTTP writer finished. Total lines sent: {}", total_sent);
        Ok(total_sent)
    }

    /// Send a batch of log lines to the HTTP API
    async fn send_batch(
        client: &Client,
        config: &HttpWriterConfig,
        batch: &[String],
    ) -> Result<usize> {
        let count = batch.len();
        // Join with newlines for newline-delimited format
        let body = batch.join("\n");

        let mut last_error = None;

        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                let delay = Duration::from_millis(100 * 2u64.pow(attempt - 1));
                warn!(
                    "Retrying HTTP request (attempt {}/{}), waiting {:?}",
                    attempt + 1,
                    config.max_retries + 1,
                    delay
                );
                tokio::time::sleep(delay).await;
            }

            let result = client
                .post(&config.url)
                .header("Content-Type", "application/x-ndjson")
                .header("Authorization", format!("Bearer {}", config.api_key))
                .body(body.clone())
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    let response_body = response.text().await.unwrap_or_default();

                    if status.is_success() {
                        debug!(
                            "HTTP batch sent: {} lines, {} bytes, status={}, response={}",
                            count,
                            body.len(),
                            status,
                            response_body
                        );
                        trace!(
                            "--- PAYLOAD ---\n{}\n--- END PAYLOAD ---",
                            body
                        );
                        return Ok(count);
                    } else {
                        last_error = Some(anyhow::anyhow!(
                            "HTTP {} from API: {}",
                            status,
                            response_body
                        ));

                        // Don't retry on 4xx errors (client errors)
                        if status.is_client_error() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("HTTP request failed: {}", e));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error sending batch")))
    }

    /// Send a match to be written
    pub async fn write_match(&self, match_item: HttpMatchToSend) -> Result<()> {
        self.write_tx
            .send(match_item)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send match for HTTP writing: {}", e))
    }

    /// Finish writing and return the number of lines sent
    pub async fn finish(mut self) -> Result<usize> {
        // Close the channel to signal we're done
        drop(self.write_tx);

        // Wait for the writer task to complete
        if let Some(handle) = self.write_handle.take() {
            handle.await?
        } else {
            Ok(0)
        }
    }

    /// Get the configured URL
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// A collector that streams results to an HTTP API instead of storing in memory.
/// This is used during the search phase to send matches as they're found.
/// Implements SearchCollector trait for use with generic search functions.
pub struct HttpStreamingCollector {
    sender: mpsc::Sender<HttpMatchToSend>,
    match_count: usize,
    files_searched: usize,
}

impl HttpStreamingCollector {
    /// Create a new streaming collector
    pub fn new(sender: mpsc::Sender<HttpMatchToSend>) -> Self {
        Self {
            sender,
            match_count: 0,
            files_searched: 0,
        }
    }
}

impl SearchCollector for HttpStreamingCollector {
    fn add_match(&mut self, bucket: &str, key: &str, _line_number: u64, line: &str) -> bool {
        self.match_count += 1;

        // Try to send - use try_send to avoid blocking in sync context
        // If channel is full, we'll drop this match (backpressure)
        // Returns false if channel is closed (receiver dropped)
        match self.sender.try_send(HttpMatchToSend {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content: line.to_string(),
        }) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => true, // channel full but still alive
            Err(mpsc::error::TrySendError::Closed(_)) => false, // consumer gone
        }
    }

    fn add_count(&mut self, _bucket: &str, _key: &str, _count: u64) {
        // Count-only mode doesn't send to HTTP
    }

    fn mark_file_searched(&mut self) {
        self.files_searched += 1;
    }

    fn match_count(&self) -> usize {
        self.match_count
    }
}
