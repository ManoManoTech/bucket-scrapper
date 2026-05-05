use super::codec::{Codec, CodecEncoder};
use super::path_template::{
    make_run_id, render_template, CollisionResult, CollisionTracker, TemplateValues,
};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use tracing::warn;

type PrefixEncoder = CodecEncoder<File>;

/// Shared file writer using per-prefix locking.
///
/// Two-level locking:
/// 1. Outer RwLock<HashMap> — read-locked for lookups (concurrent), write-locked only to insert a new prefix (rare).
/// 2. Inner Mutex<CodecEncoder<File>> per prefix — only tasks writing to the *same* output file contend.
///
/// Held behind `Arc` by callers (e.g. the file output sink) for cheap sharing
/// across filter workers. Calling [`SharedFileWriter::finalize`] drains all
/// encoders in place, so a single instance can be wrapped in `Arc<dyn …>` and
/// finalized via `&self` — no `try_unwrap` dance required.
pub struct SharedFileWriter {
    encoders: RwLock<HashMap<String, std::sync::Arc<Mutex<PrefixEncoder>>>>,
    /// Runtime collision detection: maps rendered output path → first
    /// prefix that wrote there. A second distinct prefix rendering to the
    /// same path is a hard error (two encoders cannot share one file).
    collisions: Mutex<CollisionTracker>,
    output_dir: String,
    path_template: String,
    codec: Codec,
    run_id: String,
    lines_written: AtomicUsize,
    bytes_written: AtomicUsize,
    files_created: AtomicUsize,
}

impl SharedFileWriter {
    pub fn new(output_dir: String, path_template: String, codec: Codec) -> Result<Self> {
        fs::create_dir_all(&output_dir)?;
        Ok(Self {
            encoders: RwLock::new(HashMap::new()),
            collisions: Mutex::new(CollisionTracker::new()),
            output_dir,
            path_template,
            codec,
            run_id: make_run_id(),
            lines_written: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            files_created: AtomicUsize::new(0),
        })
    }

    /// Render the absolute output path for a given source prefix.
    fn render_path(&self, prefix: &str) -> String {
        let rel = render_template(
            &self.path_template,
            &TemplateValues {
                prefix,
                run_id: &self.run_id,
                seq: 0,
                ext: self.codec.extension(),
            },
        );
        format!("{}/{}", self.output_dir.trim_end_matches('/'), rel)
    }

    /// Write a single match to the appropriate file.
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

    /// Create a new encoder for a prefix.
    /// Cold path: write-locks the map, creates the output file and encoder.
    #[cold]
    fn create_encoder(&self, prefix: &str) -> Result<std::sync::Arc<Mutex<PrefixEncoder>>> {
        let mut map = self.encoders.write().unwrap_or_else(|e| e.into_inner());
        // Double-check after acquiring write lock
        if let Some(arc) = map.get(prefix) {
            return Ok(arc.clone());
        }

        let output_file = self.render_path(prefix);

        // Check for cross-prefix collision before opening anything. Two
        // encoders writing to one file would interleave their compressed
        // frames and corrupt the output — fatal.
        {
            let mut tracker = self.collisions.lock().unwrap_or_else(|e| e.into_inner());
            if let CollisionResult::Collision { existing_prefix } =
                tracker.record(prefix, &output_file)
            {
                return Err(anyhow!(
                    "file output collision: prefixes `{existing_prefix}` and `{prefix}` both render to `{output_file}`. \
                     Add `{{prefix}}` or `{{prefix_hash}}` to the path_template to disambiguate."
                ));
            }
        }

        if let Some(parent) = std::path::Path::new(&output_file).parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&output_file)?;
        let encoder = self.codec.encoder(file)?;
        let arc = std::sync::Arc::new(Mutex::new(encoder));
        map.insert(prefix.to_string(), std::sync::Arc::clone(&arc));
        self.files_created.fetch_add(1, Ordering::Relaxed);
        Ok(arc)
    }

    /// Finalize all encoders in place. Drains the per-prefix encoder map,
    /// closes each frame, and aggregates totals.
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

    fn writer(dir: &std::path::Path, codec: Codec) -> SharedFileWriter {
        SharedFileWriter::new(
            dir.to_str().unwrap().to_string(),
            "{prefix}.{ext}".to_string(),
            codec,
        )
        .unwrap()
    }

    #[test]
    fn write_and_finish_single_prefix_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let w = writer(dir.path(), Codec::Zstd { level: 1 });

        let line = "{\"msg\":\"hello\"}\n";
        for _ in 0..10 {
            w.write_match("2025-02-23/14", line.as_bytes()).unwrap();
        }

        let stats = w.finalize().unwrap();
        assert_eq!(stats.files_written, 1);
        assert_eq!(stats.lines_written, 10);
        assert_eq!(stats.plaintext_bytes, (line.len() * 10) as u64);

        let path = dir.path().join("2025-02-23/14.zst");
        assert!(path.exists(), "zstd output file should exist");
        let file = File::open(&path).unwrap();
        let mut decoder = zstd::Decoder::new(file).unwrap();
        let mut content = String::new();
        decoder.read_to_string(&mut content).unwrap();
        assert_eq!(content, line.repeat(10));
    }

    #[test]
    fn write_and_finish_gzip() {
        let dir = tempfile::tempdir().unwrap();
        let w = writer(dir.path(), Codec::Gzip { level: 6 });

        let line = "gzip line\n";
        for _ in 0..50 {
            w.write_match("p1", line.as_bytes()).unwrap();
        }
        w.finalize().unwrap();

        let path = dir.path().join("p1.gz");
        assert!(path.exists(), "gzip output should be at {path:?}");
        let f = File::open(&path).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(f);
        let mut s = String::new();
        decoder.read_to_string(&mut s).unwrap();
        assert_eq!(s, line.repeat(50));
    }

    #[test]
    fn write_and_finish_none_drops_extension_dot() {
        let dir = tempfile::tempdir().unwrap();
        let w = writer(dir.path(), Codec::None);

        w.write_match("p", b"plaintext line\n").unwrap();
        w.finalize().unwrap();

        // {prefix}.{ext} with empty ext collapses to {prefix}, no trailing dot.
        let path = dir.path().join("p");
        assert!(path.exists(), "plaintext file should exist at {path:?}");
        let mut s = String::new();
        File::open(&path).unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "plaintext line\n");
    }

    #[test]
    fn write_multiple_prefixes_creates_separate_files() {
        let dir = tempfile::tempdir().unwrap();
        let w = writer(dir.path(), Codec::Zstd { level: 1 });

        let prefixes = ["2025-02-23/10", "2025-02-23/11", "2025-02-23/12"];
        for p in &prefixes {
            w.write_match(p, b"line\n").unwrap();
        }

        let stats = w.finalize().unwrap();
        assert_eq!(stats.files_written, 3);
        for p in &prefixes {
            assert!(dir.path().join(format!("{p}.zst")).exists());
        }
    }

    #[test]
    fn finish_reports_compressed_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let w = writer(dir.path(), Codec::Zstd { level: 1 });

        let line = "{\"timestamp\":\"2025-02-23T14:00:00Z\",\"level\":\"INFO\",\"msg\":\"test\"}\n";
        for _ in 0..100 {
            w.write_match("2025-02-23/14", line.as_bytes()).unwrap();
        }

        let stats = w.finalize().unwrap();
        assert!(stats.compressed_bytes > 0);
        assert!(stats.compressed_bytes < stats.plaintext_bytes);
    }

    #[test]
    fn collision_between_prefixes_errors() {
        // Template that ignores the prefix entirely — every prefix renders
        // to the same path. Static validation rejects this in production
        // (no `{prefix}` / `{prefix_hash}`); we bypass it here to exercise
        // the runtime guard.
        let dir = tempfile::tempdir().unwrap();
        let w = SharedFileWriter::new(
            dir.path().to_str().unwrap().to_string(),
            "shared.{ext}".to_string(),
            Codec::Zstd { level: 1 },
        )
        .unwrap();

        w.write_match("prefix-a", b"line\n").unwrap();
        let err = w.write_match("prefix-b", b"line\n").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("collision"), "{msg}");
        assert!(msg.contains("prefix-a"), "{msg}");
        assert!(msg.contains("prefix-b"), "{msg}");
    }
}
