// src/search/streaming_writer.rs
use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// A match to be written to the output
#[derive(Debug, Clone)]
pub struct MatchToWrite {
    pub bucket: String,
    pub key: String,
    pub content: String,
    pub prefix: String, // e.g., "20240115H10"
}

/// Manages streaming writes of search results to compressed files
pub struct StreamingResultWriter {
    output_dir: String,
    encoders: Arc<Mutex<HashMap<String, GzEncoder<File>>>>,
    write_tx: mpsc::Sender<MatchToWrite>,
    write_handle: Option<tokio::task::JoinHandle<Result<usize>>>,
}

impl StreamingResultWriter {
    /// Create a new streaming writer
    pub fn new(output_dir: String) -> Result<Self> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(&output_dir)?;

        let encoders = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel::<MatchToWrite>(1000);

        let encoders_clone = encoders.clone();
        let output_dir_clone = output_dir.clone();

        // Spawn a task to handle writes
        let write_handle = tokio::spawn(async move {
            let mut files_written = 0usize;
            let mut local_encoders: HashMap<String, GzEncoder<File>> = HashMap::new();

            while let Some(match_item) = write_rx.recv().await {
                // Get or create encoder for this prefix
                let encoder = match local_encoders.get_mut(&match_item.prefix) {
                    Some(enc) => enc,
                    None => {
                        // Create new file and encoder
                        let output_file = format!("{}/{}.gz", output_dir_clone, match_item.prefix);
                        debug!("Creating new output file: {}", output_file);

                        let file = match File::create(&output_file) {
                            Ok(f) => f,
                            Err(e) => {
                                eprintln!("Failed to create output file {}: {}", output_file, e);
                                continue;
                            }
                        };

                        let encoder = GzEncoder::new(file, Compression::default());
                        local_encoders.insert(match_item.prefix.clone(), encoder);
                        files_written += 1;

                        local_encoders.get_mut(&match_item.prefix).unwrap()
                    }
                };

                // Write the match as JSON
                let output_line = json!({
                    "file": format!("{}/{}", match_item.bucket, match_item.key),
                    "content": match_item.content.trim()
                });

                if let Err(e) = writeln!(encoder, "{}", output_line) {
                    eprintln!("Failed to write match: {}", e);
                }
            }

            // Finish all encoders
            for (prefix, encoder) in local_encoders {
                if let Err(e) = encoder.finish() {
                    eprintln!("Failed to finish encoder for {}: {}", prefix, e);
                } else {
                    info!("Finished writing {}.gz", prefix);
                }
            }

            // Update shared state with final encoders (for cleanup if needed)
            *encoders_clone.lock().await = HashMap::new();

            Ok(files_written)
        });

        Ok(Self {
            output_dir,
            encoders,
            write_tx,
            write_handle: Some(write_handle),
        })
    }

    /// Send a match to be written
    pub async fn write_match(&self, match_item: MatchToWrite) -> Result<()> {
        self.write_tx
            .send(match_item)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send match for writing: {}", e))
    }

    /// Finish writing and return the number of files written
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
}

/// Extract date/hour prefix from S3 key for grouping
pub fn extract_prefix(key: &str) -> String {
    // Try to extract dt=YYYYMMDD/hour=HH pattern
    if let Some(dt_pos) = key.find("dt=") {
        if let Some(hour_pos) = key.find("hour=") {
            if dt_pos < hour_pos {
                let dt_start = dt_pos + 3;
                let dt_end = dt_start + 8; // YYYYMMDD
                let hour_start = hour_pos + 5;
                let hour_end = hour_start + 2; // HH

                if dt_end <= key.len() && hour_end <= key.len() {
                    if let (Ok(date), Ok(hour)) = (
                        key[dt_start..dt_end].parse::<String>(),
                        key[hour_start..hour_end].parse::<String>(),
                    ) {
                        return format!("{}H{}", date, hour);
                    }
                }
            }
        }
    }

    // Fallback: use directory structure
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() >= 2 {
        parts[..parts.len() - 1].join("_")
    } else {
        "unknown".to_string()
    }
}
