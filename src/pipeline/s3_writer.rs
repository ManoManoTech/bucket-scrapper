//! S3 output sink: per-prefix zstd uploads.
//!
//! For each source S3 prefix the sink keeps an in-memory zstd encoder. By
//! default (no `batch_max_mb` set), each prefix produces exactly one output
//! object on `finish()` — N input objects collapse to 1 output object per
//! prefix, mirroring file sink semantics.
//!
//! When `batch_max_mb` is set, batch rollover is enabled: as soon as a
//! prefix's compressed buffer exceeds the threshold, the batch is finalized,
//! the destination key is rendered from `key_template`, the `{seq}` for
//! that prefix is incremented, and the bytes are handed to a bounded
//! uploader pool. Use rollover when output objects must stay under a size
//! cap (e.g. downstream import limits); leave it off for simple N:1
//! consolidation.
//!
//! ## Key template placeholders
//!
//! - `{prefix}` — the source S3 prefix (e.g. `logs/dt=20240315/hour=09`).
//! - `{prefix_hash}` — 8-char hex BLAKE3-style hash of the prefix (DefaultHasher).
//!   Useful when the source prefix contains characters you don't want in the
//!   destination key.
//! - `{seq}` — zero-padded 5-digit sequence number, incremented per prefix
//!   on every batch rollover.
//! - `{run_id}` — 8-char hex hash unique to this process invocation.
//!
//! ## Multipart uploads
//!
//! Currently every batch is uploaded via a single `PutObject` request. AWS
//! supports up to 5 GB per single PUT, so configurations with batches under
//! that limit work as-is. True multipart support (chunking a single batch
//! into parts) is left as future work — the `multipart_threshold_mb` and
//! `multipart_part_mb` config fields are accepted but not yet acted upon,
//! and the sink emits a warning at startup if you set them away from the
//! defaults so the gap is explicit.

use super::output::{BoxFinishFuture, OutputSink, OutputStats};
use crate::config::output::S3OutputConfig;
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use serde_json::json;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

/// In-flight per-prefix batch state.
struct PrefixBatch {
    encoder: zstd::Encoder<'static, Vec<u8>>,
    lines: u64,
    plaintext: u64,
    seq: u64,
}

/// Owned state shared between ingest path and uploader pool.
struct Inner {
    client: Client,
    bucket: String,
    key_template: String,
    /// `Some(n)` enables size-based rollover at `n` compressed bytes per
    /// prefix. `None` disables rollover — each prefix produces one object.
    batch_max_bytes: Option<u64>,
    compression_level: i32,
    run_id: String,
    /// Per-prefix mutable state. Outer Mutex protects map insertion only;
    /// per-prefix entries are `Mutex<PrefixBatch>` so different prefixes
    /// never block each other.
    prefixes: Mutex<HashMap<String, Arc<Mutex<PrefixBatch>>>>,
    /// Bounded queue for finalized batches awaiting upload.
    upload_tx: Mutex<Option<flume::Sender<UploadJob>>>,
    /// Counters for end-of-run stats.
    matched_lines: AtomicU64,
    plaintext_bytes: AtomicU64,
    compressed_bytes: AtomicU64,
    lines_dropped: AtomicU64,
    objects_written: AtomicU64,
    fatal: Arc<AtomicBool>,
}

struct UploadJob {
    key: String,
    body: Vec<u8>,
    lines: u64,
    plaintext: u64,
}

pub struct S3OutputSink {
    inner: Arc<Inner>,
    upload_handles: Mutex<Option<Vec<JoinHandle<()>>>>,
}

impl S3OutputSink {
    /// Build an S3 sink. The provided `client` is expected to be configured
    /// for `cfg.region` / `cfg.endpoint_url` already (the resolver wires that
    /// up — see `crate::config::resolve`).
    pub fn new(client: Client, cfg: &S3OutputConfig) -> Result<Self> {
        if let Some(mb) = cfg.batch_max_mb {
            if mb.is_nan() || mb <= 0.0 {
                return Err(anyhow!(
                    "S3 output: batch_max_mb must be > 0 when set (omit for N:1 per-prefix mode)"
                ));
            }
        }

        let mp_threshold = cfg.multipart_threshold_mb;
        let mp_part = cfg.multipart_part_mb;
        if mp_threshold != 64 || mp_part != 16 {
            warn!(
                multipart_threshold_mb = mp_threshold,
                multipart_part_mb = mp_part,
                "S3 output: multipart is not yet implemented; \
                 every batch is uploaded as a single PutObject regardless of these settings"
            );
        }

        let upload_tasks = cfg.upload_tasks.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get() / 4)
                .unwrap_or(2)
                .max(1)
        });

        let (upload_tx, upload_rx) = flume::bounded::<UploadJob>(upload_tasks * 2);

        let run_id = make_run_id();

        let inner = Arc::new(Inner {
            client,
            bucket: cfg.bucket.clone(),
            key_template: cfg.key_template.clone(),
            batch_max_bytes: cfg.batch_max_mb.map(|mb| (mb * 1_000_000.0) as u64),
            compression_level: cfg.compression_level.unwrap_or(3),
            run_id,
            prefixes: Mutex::new(HashMap::new()),
            upload_tx: Mutex::new(Some(upload_tx)),
            matched_lines: AtomicU64::new(0),
            plaintext_bytes: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            lines_dropped: AtomicU64::new(0),
            objects_written: AtomicU64::new(0),
            fatal: Arc::new(AtomicBool::new(false)),
        });

        let mut handles = Vec::with_capacity(upload_tasks);
        for task_id in 0..upload_tasks {
            let inner = inner.clone();
            let rx = upload_rx.clone();
            handles.push(tokio::spawn(async move {
                Self::uploader_task(task_id, inner, rx).await;
            }));
        }
        drop(upload_rx);

        Ok(Self {
            inner,
            upload_handles: Mutex::new(Some(handles)),
        })
    }

    async fn uploader_task(task_id: usize, inner: Arc<Inner>, rx: flume::Receiver<UploadJob>) {
        while let Ok(job) = rx.recv_async().await {
            let UploadJob {
                key,
                body,
                lines,
                plaintext,
            } = job;
            let body_len = body.len() as u64;
            let body_stream = ByteStream::from(body);

            let result = inner
                .client
                .put_object()
                .bucket(&inner.bucket)
                .key(&key)
                .body(body_stream)
                .content_type("application/x-ndjson")
                .content_encoding("zstd")
                .send()
                .await;

            match result {
                Ok(_) => {
                    inner
                        .compressed_bytes
                        .fetch_add(body_len, Ordering::Relaxed);
                    inner.objects_written.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        task = task_id,
                        bucket = %inner.bucket,
                        key = %key,
                        bytes = body_len,
                        lines,
                        plaintext,
                        "S3 PutObject"
                    );
                }
                Err(e) => {
                    inner.lines_dropped.fetch_add(lines, Ordering::Relaxed);
                    let msg = format!("{e}");
                    if !crate::s3::is_recoverable_s3_error(&msg) {
                        error!(
                            task = task_id,
                            bucket = %inner.bucket,
                            key = %key,
                            lines,
                            error = %msg,
                            "Fatal S3 PutObject error, stopping pipeline"
                        );
                        inner.fatal.store(true, Ordering::Relaxed);
                        break;
                    }
                    error!(
                        task = task_id,
                        bucket = %inner.bucket,
                        key = %key,
                        lines,
                        error = %msg,
                        "S3 PutObject failed"
                    );
                }
            }
        }
        debug!(task = task_id, "S3 uploader task finished");
    }

    /// Finalize the encoder for a prefix and enqueue an upload job.
    fn flush_batch(inner: &Inner, prefix: &str, batch: &mut PrefixBatch) -> Result<()> {
        let encoder = std::mem::replace(
            &mut batch.encoder,
            zstd::Encoder::new(Vec::new(), inner.compression_level)
                .context("create replacement zstd encoder")?,
        );
        let body = encoder.finish().context("finalize zstd batch")?;
        let lines = std::mem::replace(&mut batch.lines, 0);
        let plaintext = std::mem::replace(&mut batch.plaintext, 0);

        if body.is_empty() || lines == 0 {
            return Ok(());
        }

        let key = render_key(&inner.key_template, prefix, batch.seq, &inner.run_id);
        batch.seq += 1;

        let tx_guard = inner.upload_tx.lock().unwrap_or_else(|e| e.into_inner());
        match tx_guard.as_ref() {
            Some(tx) => {
                tx.send(UploadJob {
                    key,
                    body,
                    lines,
                    plaintext,
                })
                .map_err(|_| anyhow!("S3 uploader pool gone, channel closed"))?;
            }
            None => return Err(anyhow!("S3 sink already finished")),
        }
        Ok(())
    }
}

impl OutputSink for S3OutputSink {
    fn ingest(&self, prefix: &str, line: &[u8]) -> Result<()> {
        let entry = {
            let mut map = self
                .inner
                .prefixes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.entry(prefix.to_string())
                .or_insert_with(|| {
                    let encoder = zstd::Encoder::new(Vec::new(), self.inner.compression_level)
                        .expect("zstd encoder creation must succeed");
                    Arc::new(Mutex::new(PrefixBatch {
                        encoder,
                        lines: 0,
                        plaintext: 0,
                        seq: 0,
                    }))
                })
                .clone()
        };

        let mut batch = entry.lock().unwrap_or_else(|e| e.into_inner());
        batch.encoder.write_all(line).context("zstd write")?;
        batch.lines += 1;
        batch.plaintext += line.len() as u64;

        self.inner.matched_lines.fetch_add(1, Ordering::Relaxed);
        self.inner
            .plaintext_bytes
            .fetch_add(line.len() as u64, Ordering::Relaxed);

        if let Some(threshold) = self.inner.batch_max_bytes {
            let approx_compressed = batch.encoder.get_ref().len() as u64;
            if approx_compressed >= threshold {
                Self::flush_batch(&self.inner, prefix, &mut batch)?;
            }
        }
        Ok(())
    }

    fn finish<'a>(&'a self) -> BoxFinishFuture<'a> {
        Box::pin(async move {
            // Drain remaining batches.
            let prefixes: Vec<(String, Arc<Mutex<PrefixBatch>>)> = {
                let mut map = self
                    .inner
                    .prefixes
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                map.drain().collect()
            };
            for (prefix, entry) in prefixes {
                let mut batch = entry.lock().unwrap_or_else(|e| e.into_inner());
                if let Err(e) = Self::flush_batch(&self.inner, &prefix, &mut batch) {
                    warn!(prefix = %prefix, error = %e, "S3 sink: failed to flush final batch");
                }
            }

            // Close upload channel so uploader tasks drain and exit.
            {
                let mut tx_guard = self
                    .inner
                    .upload_tx
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                tx_guard.take();
            }

            let handles = {
                let mut h = self
                    .upload_handles
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                h.take()
            };
            if let Some(handles) = handles {
                for handle in handles {
                    if let Err(e) = handle.await {
                        warn!(error = %e, "S3 uploader task panicked");
                    }
                }
            }

            let stats = OutputStats {
                matched_lines: self.inner.matched_lines.load(Ordering::Relaxed),
                plaintext_bytes: self.inner.plaintext_bytes.load(Ordering::Relaxed),
                compressed_bytes: self.inner.compressed_bytes.load(Ordering::Relaxed),
                lines_dropped: self.inner.lines_dropped.load(Ordering::Relaxed),
                extras: serde_json::Map::from_iter([
                    ("bucket".to_string(), json!(self.inner.bucket)),
                    (
                        "objects_written".to_string(),
                        json!(self.inner.objects_written.load(Ordering::Relaxed)),
                    ),
                    ("run_id".to_string(), json!(self.inner.run_id)),
                ]),
            };

            if self.inner.fatal.load(Ordering::Relaxed) {
                return Err(anyhow!(
                    "S3 sink aborted due to fatal upload error \
                     (objects_written={}, lines_dropped={})",
                    stats
                        .extras
                        .get("objects_written")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    stats.lines_dropped,
                ));
            }
            Ok(stats)
        })
    }

    fn fatal_error_flag(&self) -> Option<Arc<AtomicBool>> {
        Some(self.inner.fatal.clone())
    }

    fn type_name(&self) -> &'static str {
        "s3"
    }
}

fn render_key(template: &str, prefix: &str, seq: u64, run_id: &str) -> String {
    let prefix_hash = short_hash(prefix);
    template
        .replace("{prefix}", prefix)
        .replace("{prefix_hash}", &prefix_hash)
        .replace("{seq}", &format!("{seq:05}"))
        .replace("{run_id}", run_id)
}

fn short_hash(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

fn make_run_id() -> String {
    let mut hasher = DefaultHasher::new();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    nanos.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_key_substitutes_all_placeholders() {
        let key = render_key(
            "out/{prefix}/{prefix_hash}-{seq}-{run_id}.zst",
            "logs/dt=20240101",
            7,
            "abcd1234",
        );
        assert!(key.starts_with("out/logs/dt=20240101/"));
        assert!(key.contains("00007"));
        assert!(key.ends_with("-abcd1234.zst"));
    }

    #[test]
    fn short_hash_is_stable() {
        assert_eq!(short_hash("foo"), short_hash("foo"));
        assert_ne!(short_hash("foo"), short_hash("bar"));
    }
}
