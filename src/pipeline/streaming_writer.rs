use anyhow::Result;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use tracing::warn;
use zstd::Encoder as ZstdEncoder;

type PrefixEncoder = ZstdEncoder<'static, File>;

/// Shared file writer using per-prefix locking.
///
/// Two-level locking:
/// 1. Outer RwLock<HashMap> — read-locked for lookups (concurrent), write-locked only to insert a new prefix (rare).
/// 2. Inner Mutex<ZstdEncoder<File>> per prefix — only tasks writing to the *same* date/hour file contend.
///
/// Held behind `Arc` by callers (e.g. the file output sink) for cheap sharing
/// across filter workers. Calling [`SharedFileWriter::finalize`] drains all
/// encoders in place, so a single instance can be wrapped in `Arc<dyn …>` and
/// finalized via `&self` — no `try_unwrap` dance required.
pub struct SharedFileWriter {
    encoders: RwLock<HashMap<String, std::sync::Arc<Mutex<PrefixEncoder>>>>,
    output_dir: String,
    compression_level: i32,
    lines_written: AtomicUsize,
    bytes_written: AtomicUsize,
    files_created: AtomicUsize,
}

impl SharedFileWriter {
    pub fn new(output_dir: String, compression_level: i32) -> Result<Self> {
        fs::create_dir_all(&output_dir)?;
        Ok(Self {
            encoders: RwLock::new(HashMap::new()),
            output_dir,
            compression_level,
            lines_written: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            files_created: AtomicUsize::new(0),
        })
    }

    /// Write a single match to the appropriate zstd file.
    /// Called from spawn_blocking search tasks — fully synchronous.
    pub fn write_match(&self, prefix: &str, content: &[u8]) -> Result<()> {
        let encoder_arc = self.get_or_create_encoder(prefix)?;

        let mut encoder = encoder_arc.lock().unwrap_or_else(|e| e.into_inner());
        encoder.write_all(content)?;

        self.lines_written.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(content.len(), Ordering::Relaxed);

        Ok(())
    }

    /// Look up the encoder for a prefix, creating one if needed.
    fn get_or_create_encoder(&self, prefix: &str) -> Result<std::sync::Arc<Mutex<PrefixEncoder>>> {
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
    fn create_encoder(&self, prefix: &str) -> Result<std::sync::Arc<Mutex<PrefixEncoder>>> {
        let mut map = self.encoders.write().unwrap_or_else(|e| e.into_inner());
        // Double-check after acquiring write lock
        if let Some(arc) = map.get(prefix) {
            return Ok(arc.clone());
        }

        let output_file = format!("{}/{}.zst", self.output_dir, prefix);
        if let Some(parent) = std::path::Path::new(&output_file).parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&output_file)?;
        let encoder = ZstdEncoder::new(file, self.compression_level)?;
        let arc = std::sync::Arc::new(Mutex::new(encoder));
        map.insert(prefix.to_string(), std::sync::Arc::clone(&arc));
        self.files_created.fetch_add(1, Ordering::Relaxed);
        Ok(arc)
    }

    /// Finalize all encoders in place. Drains the per-prefix encoder map,
    /// closes each zstd frame, and aggregates totals.
    ///
    /// Must be called *after* every filter worker has stopped issuing
    /// `write_match`. Calling `write_match` again afterwards transparently
    /// re-creates encoders, but the closed files won't reopen — finalize is
    /// a one-shot terminal operation.
    pub fn finalize(&self) -> Result<FileWriterStats> {
        // mem::take the map so we own the Arc<Mutex<Encoder>> entries; once
        // ingest has stopped, no other clones exist and try_unwrap succeeds.
        let mut guard = self.encoders.write().unwrap_or_else(|e| e.into_inner());
        let map: HashMap<String, std::sync::Arc<Mutex<PrefixEncoder>>> =
            std::mem::take(&mut *guard);
        drop(guard);

        let mut files_written = 0usize;
        let mut compressed_bytes = 0u64;
        for (prefix, encoder_arc) in map {
            match std::sync::Arc::try_unwrap(encoder_arc) {
                Ok(mutex) => {
                    let encoder = mutex.into_inner().unwrap_or_else(|e| e.into_inner());
                    match encoder.finish() {
                        Ok(file) => {
                            files_written += 1;
                            if let Ok(meta) = file.metadata() {
                                compressed_bytes += meta.len();
                            }
                        }
                        Err(e) => {
                            warn!(prefix = %prefix, error = %e, "Failed to finish encoder");
                        }
                    }
                }
                Err(_) => {
                    warn!(prefix = %prefix, "Could not unwrap encoder Arc — still referenced");
                }
            }
        }

        Ok(FileWriterStats {
            files_written,
            lines_written: self.lines_written.load(Ordering::Relaxed),
            plaintext_bytes: self.bytes_written.load(Ordering::Relaxed) as u64,
            compressed_bytes,
        })
    }
}

pub struct FileWriterStats {
    pub files_written: usize,
    pub lines_written: usize,
    pub plaintext_bytes: u64,
    pub compressed_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn write_and_finish_single_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SharedFileWriter::new(dir.path().to_str().unwrap().to_string(), 1).unwrap();

        let line = "{\"msg\":\"hello\"}\n";
        for _ in 0..10 {
            writer
                .write_match("2025-02-23/14", line.as_bytes())
                .unwrap();
        }

        let stats = writer.finalize().unwrap();
        assert_eq!(stats.files_written, 1);
        assert_eq!(stats.lines_written, 10);
        assert_eq!(stats.plaintext_bytes, (line.len() * 10) as u64);

        // Verify the file is valid zstd and contains expected content
        let path = dir.path().join("2025-02-23/14.zst");
        assert!(path.exists(), "output file should exist");

        let file = File::open(&path).unwrap();
        let mut decoder = zstd::Decoder::new(file).unwrap();
        let mut content = String::new();
        decoder.read_to_string(&mut content).unwrap();
        assert_eq!(content, line.repeat(10));
    }

    #[test]
    fn write_multiple_prefixes_creates_separate_files() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SharedFileWriter::new(dir.path().to_str().unwrap().to_string(), 1).unwrap();

        let prefixes = ["2025-02-23/10", "2025-02-23/11", "2025-02-23/12"];
        for prefix in &prefixes {
            writer.write_match(prefix, b"line\n").unwrap();
        }

        let stats = writer.finalize().unwrap();
        assert_eq!(stats.files_written, 3);

        for prefix in &prefixes {
            let path = dir.path().join(format!("{prefix}.zst"));
            assert!(path.exists(), "file for prefix '{prefix}' should exist");
        }
    }

    #[test]
    fn finish_reports_compressed_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SharedFileWriter::new(dir.path().to_str().unwrap().to_string(), 1).unwrap();

        // Write enough repeated data that zstd compresses well
        let line = "{\"timestamp\":\"2025-02-23T14:00:00Z\",\"level\":\"INFO\",\"msg\":\"test\"}\n";
        for _ in 0..100 {
            writer
                .write_match("2025-02-23/14", line.as_bytes())
                .unwrap();
        }

        let stats = writer.finalize().unwrap();
        assert!(stats.compressed_bytes > 0, "should have compressed bytes");
        assert!(
            stats.compressed_bytes < stats.plaintext_bytes,
            "compressed ({}) should be smaller than plaintext ({})",
            stats.compressed_bytes,
            stats.plaintext_bytes
        );
    }
}
