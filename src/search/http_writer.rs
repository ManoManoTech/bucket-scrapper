// src/search/http_writer.rs
use anyhow::{Context, Result};
use reqwest::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
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
        }
    }
}

/// Manages streaming writes of search results to an HTTP API
pub struct HttpResultWriter {
    write_tx: mpsc::Sender<String>,
    write_handle: Option<tokio::task::JoinHandle<Result<usize>>>,
    /// Tracks total lines sent (shared with writer task)
    lines_sent: Arc<AtomicUsize>,
    /// URL for display purposes
    url: String,
}

impl HttpResultWriter {
    /// Create a new HTTP writer
    pub fn new(config: HttpWriterConfig) -> Result<Self> {
        let (write_tx, write_rx) = mpsc::channel::<String>(config.channel_buffer_size);
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
            url,
        })
    }

    /// Get a sender that can be cloned for use in multiple tasks
    pub fn get_sender(&self) -> mpsc::Sender<String> {
        self.write_tx.clone()
    }

    /// Get current lines sent count
    pub fn lines_sent(&self) -> usize {
        self.lines_sent.load(Ordering::Relaxed)
    }

    /// Background task that batches and sends lines to the HTTP API
    async fn writer_task(
        mut write_rx: mpsc::Receiver<String>,
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

        while let Some(line) = write_rx.recv().await {
            let line_bytes = line.len() + 1; // +1 for newline

            // Flush if adding this line would exceed the byte limit
            if !batch.is_empty() && (batch_bytes + line_bytes > config.batch_max_bytes) {
                match Self::send_batch(&client, &config, &batch).await {
                    Ok(count) => {
                        total_sent += count;
                        lines_sent.fetch_add(count, Ordering::Relaxed);
                        debug!(lines = count, bytes = batch_bytes, url = %config.url, "Sent batch");
                    }
                    Err(e) => {
                        error!(lines = batch.len(), bytes = batch_bytes, error = %e, "Failed to send batch");
                    }
                }
                batch.clear();
                batch_bytes = 0;
            }

            batch.push(line);
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
                    info!(lines = count, url = %config.url, "Sent final batch");
                }
                Err(e) => {
                    error!(lines = final_batch_lines, bytes = final_batch_bytes, error = %e, "Failed to send final batch");
                }
            }
        }

        info!(total_lines = total_sent, "HTTP writer finished");
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
                .body(body.clone())
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
                        trace!(payload = %body, "HTTP batch payload");
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

/// An exporter that streams results to an HTTP API instead of storing in memory.
/// Implements SearchExporter trait for use with generic search functions.
pub struct HttpStreamingExporter {
    sender: mpsc::Sender<String>,
    match_count: usize,
}

impl HttpStreamingExporter {
    pub fn new(sender: mpsc::Sender<String>) -> Self {
        Self {
            sender,
            match_count: 0,
        }
    }
}

impl SearchExporter for HttpStreamingExporter {
    fn add_match(&mut self, line: &str) -> Result<()> {
        match self.sender.try_send(line.to_string()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {
                self.match_count += 1;
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("HTTP consumer gone, channel closed"))
            }
        }
    }

    fn match_count(&self) -> usize {
        self.match_count
    }
}
