// src/search/streaming_writer.rs
use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{info, warn};
use zstd::Encoder as ZstdEncoder;

type PrefixEncoder = ZstdEncoder<'static, File>;

/// Shared file writer using per-prefix locking.
///
/// Two-level locking:
/// 1. Outer RwLock<HashMap> — read-locked for lookups (concurrent), write-locked only to insert a new prefix (rare).
/// 2. Inner Mutex<ZstdEncoder<File>> per prefix — only tasks writing to the *same* date/hour file contend.
pub struct SharedFileWriter {
    encoders: Arc<RwLock<HashMap<String, Arc<Mutex<PrefixEncoder>>>>>,
    output_dir: String,
    matches_written: Arc<AtomicUsize>,
    files_created: Arc<AtomicUsize>,
}

impl Clone for SharedFileWriter {
    fn clone(&self) -> Self {
        Self {
            encoders: Arc::clone(&self.encoders),
            output_dir: self.output_dir.clone(),
            matches_written: Arc::clone(&self.matches_written),
            files_created: Arc::clone(&self.files_created),
        }
    }
}

impl SharedFileWriter {
    pub fn new(output_dir: String) -> Result<Self> {
        fs::create_dir_all(&output_dir)?;
        Ok(Self {
            encoders: Arc::new(RwLock::new(HashMap::new())),
            output_dir,
            matches_written: Arc::new(AtomicUsize::new(0)),
            files_created: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Write a single match to the appropriate zstd file.
    /// Called from spawn_blocking search tasks — fully synchronous.
    pub fn write_match(&self, prefix: &str, bucket: &str, key: &str, content: &str) -> Result<()> {
        // Fast path: read-lock to find existing encoder
        let encoder_arc = {
            let map = self.encoders.read().unwrap_or_else(|e| e.into_inner());
            map.get(prefix).cloned()
        };

        let encoder_arc = match encoder_arc {
            Some(arc) => arc,
            None => {
                // Slow path: write-lock to insert new encoder
                let mut map = self.encoders.write().unwrap_or_else(|e| e.into_inner());
                // Double-check after acquiring write lock
                if let Some(arc) = map.get(prefix) {
                    arc.clone()
                } else {
                    let output_file = format!("{}/{}.zst", self.output_dir, prefix);
                    let file = File::create(&output_file)?;
                    let encoder = ZstdEncoder::new(file, 3)?;
                    let arc = Arc::new(Mutex::new(encoder));
                    map.insert(prefix.to_string(), Arc::clone(&arc));
                    self.files_created.fetch_add(1, Ordering::Relaxed);
                    arc
                }
            }
        };

        // Lock only this prefix's encoder
        let mut encoder = encoder_arc.lock().unwrap_or_else(|e| e.into_inner());

        let output_line = json!({
            "file": format!("{}/{}", bucket, key),
            "content": content.trim()
        });

        writeln!(encoder, "{}", output_line)?;

        self.matches_written.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Finalize all encoders. Must be called after all search tasks have completed.
    /// Returns the number of files written.
    pub fn finish(self) -> Result<usize> {
        let rwlock = Arc::try_unwrap(self.encoders)
            .unwrap_or_else(|arc| {
                // Fallback: shouldn't happen if called after all tasks complete.
                // Clone the inner map into a fresh RwLock.
                let guard = arc.read().unwrap_or_else(|e| e.into_inner());
                RwLock::new(guard.clone())
            });

        let map = rwlock.into_inner().unwrap_or_else(|e| e.into_inner());

        let mut files_written = 0usize;
        for (prefix, encoder_arc) in map {
            match Arc::try_unwrap(encoder_arc) {
                Ok(mutex) => {
                    let encoder = mutex.into_inner().unwrap_or_else(|e| e.into_inner());
                    if let Err(e) = encoder.finish() {
                        warn!("Failed to finish encoder for {}: {}", prefix, e);
                    } else {
                        files_written += 1;
                    }
                }
                Err(_) => {
                    warn!("Could not unwrap encoder Arc for {} — still referenced", prefix);
                }
            }
        }

        let total_matches = self.matches_written.load(Ordering::Relaxed);
        info!(
            "File output complete: {} matches written to {} files",
            total_matches, files_written
        );

        Ok(files_written)
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
