// src/search/streaming_writer.rs
use anyhow::Result;
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
    pub fn write_match(&self, prefix: &str, content: &str) -> Result<()> {
        let encoder_arc = self.get_or_create_encoder(prefix)?;

        let mut encoder = encoder_arc.lock().unwrap_or_else(|e| e.into_inner());
        encoder.write_all(content.as_bytes())?;

        self.matches_written.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Look up the encoder for a prefix, creating one if needed.
    fn get_or_create_encoder(&self, prefix: &str) -> Result<Arc<Mutex<PrefixEncoder>>> {
        // Fast path: read-lock to find existing encoder
        {
            let map = self.encoders.read().unwrap_or_else(|e| e.into_inner());
            if let Some(arc) = map.get(prefix) {
                return Ok(arc.clone());
            }
        }

        // Slow path
        self.create_encoder(prefix)
    }

    /// Create a new zstd encoder for a prefix.
    /// Cold path: write-locks the map, creates the output file and encoder.
    #[cold]
    fn create_encoder(&self, prefix: &str) -> Result<Arc<Mutex<PrefixEncoder>>> {
        let mut map = self.encoders.write().unwrap_or_else(|e| e.into_inner());
        // Double-check after acquiring write lock
        if let Some(arc) = map.get(prefix) {
            return Ok(arc.clone());
        }

        let output_file = format!("{}/{}.zst", self.output_dir, prefix);
        let file = File::create(&output_file)?;
        let encoder = ZstdEncoder::new(file, 3)?;
        let arc = Arc::new(Mutex::new(encoder));
        map.insert(prefix.to_string(), Arc::clone(&arc));
        self.files_created.fetch_add(1, Ordering::Relaxed);
        Ok(arc)
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

