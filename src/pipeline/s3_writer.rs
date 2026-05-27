//! S3 output sink: per-prefix codec-encoded streaming uploads.
//!
//! ## Streaming model
//!
//! Each per-prefix batch is uploaded as a single S3 multipart upload,
//! streamed directly from the codec encoder. The compressed body never
//! materializes as a `Vec<u8>` — bytes flow from the encoder through a
//! bounded mpsc channel into TM's `PartStream` interface, which uploads
//! parts to S3 concurrently. Peak resident memory per active prefix is
//! ~`channel_cap × codec_block_size + multipart_part_mb`, independent of
//! total batch size. See [`super::s3_streaming`] for the channel plumbing.
//!
//! Per-prefix encoder lifecycle:
//!
//! 1. The first matched line for a prefix lazily opens an `ActiveUpload`:
//!    we render the destination key, build a `CodecEncoder<ChannelWriter>`,
//!    construct an `InputStream::from_part_stream(EncoderPartStream{..})`,
//!    and call `tm.upload()…initiate()` which spawns TM's internal upload
//!    tasks. Different prefixes have independent uploads and never block
//!    each other.
//! 2. Subsequent matched lines write through the codec encoder; bytes
//!    leave RAM as fast as TM can ship them to S3.
//! 3. The upload is closed when either `bytes_sent >= batch_max_mb`
//!    triggers a rollover or `finish()` runs at end-of-run. Closing means
//!    finalizing the framing + codec encoders (emitting trailers), then
//!    dropping the writer so the mpsc channel closes and TM's `PartStream`
//!    sees EOF.
//! 4. The TM driver task (spawned at open) joins the upload, then folds
//!    per-batch counts into the sink's global counters
//!    (`objects_written`, `compressed_bytes`) on success or
//!    `lines_dropped` + `fatal` on failure.
//! 5. After a rollover, `{seq}` is incremented and the next matched line
//!    opens a fresh upload — prefixes that match nothing never create an
//!    upload at all.
//!
//! Default mode (no `batch_max_mb`): each prefix produces exactly one
//! object covering the entire run; `{seq}` is always `00000`.
//!
//! Batched mode (`batch_max_mb` set): a prefix that crosses the threshold
//! N times produces N+1 objects (`00000`..`N`), the last one emitted by
//! the end-of-run close. Threshold is checked on `bytes_sent` (cumulative
//! compressed bytes shipped through the channel), so the actual rollover
//! lands a little above the configured size.
//!
//! Trade-offs of the streaming path versus the previous buffered model:
//!
//! - **Always-MPU.** `InputStream::from_part_stream` is MPU-only — even
//!   sub-`multipart_threshold_mb` batches go through CreateMultipartUpload
//!   → UploadPart → CompleteMultipartUpload (3 API calls) instead of a
//!   single PutObject.
//! - **No full-batch retry.** Once compressed bytes leave the encoder
//!   they're gone; TM's per-part retries are the only retry. The sink
//!   already treated a failed upload as a dropped batch
//!   (`lines_dropped += batch.lines`), so this is a non-regression.
//! - **zstd block buffering still hides bytes.** A trickle of small,
//!   highly-compressible lines can keep zstd's *output* near zero for a
//!   long time, so the `bytes_sent` threshold may not fire when expected.
//!   Use `compression.format: none` for size-driven batching with
//!   predictable thresholds, or feed enough volume per prefix to force
//!   block flushes.
//! - **Out-of-order upload completion.** TM uploads parts concurrently,
//!   so `{seq}` reflects sink open order, not upload-completion order.
//! - **`{seq}` is per-process.** Two runs hitting the same prefix produce
//!   overlapping `{seq}` values; `{run_id}` is the disambiguator.
//!
//! ## Key template placeholders
//!
//! - `{prefix}` — the source S3 prefix (e.g. `logs/dt=20240315/hour=09`).
//! - `{prefix_hash}` — 8-char hex hash of the prefix. Useful when the
//!   source prefix contains characters you don't want in the destination
//!   key.
//! - `{seq}` — zero-padded 5-digit per-prefix sequence number.
//! - `{run_id}` — 8-char hex hash unique to this process invocation.
//! - `{ext}` — codec-derived file extension (`zst` / `gz` / empty).
//!
//! ## Multipart configuration
//!
//! Each upload runs through TM with the configured `multipart_part_mb`
//! (default 5 MiB). For the streaming path TM also needs an *upper-bound*
//! content-length hint to plan part sizes; we advertise
//! `batch_max_bytes + part_bytes` when batching is on, or
//! `part_bytes × 10_000` (~50 GiB at default settings) when unbounded.
//! `multipart_threshold_mb` is retained for config compatibility but has
//! no effect on the streaming path (always-MPU).
//!
//! AWS enforces 5 MiB minimum part size and 10,000-part maximum; our
//! config validation rejects sub-5 MiB values at startup.
//!
//! The sink shares its `aws_sdk_s3::Client` with the transfer manager via
//! `tm::Config::Builder::client(...)`, so credentials, endpoint URL, and
//! the cached DNS resolver carry through unchanged. Concurrency for parts
//! in flight is controlled by `multipart_concurrency`: omit the field
//! for TM's auto-tuning, or set a positive integer for an explicit cap.
//! Per-prefix upload concurrency is implicit — bounded by the number of
//! distinct prefixes ingested in parallel by the filter workers.
//!
//! [`aws-sdk-s3-transfer-manager`]: https://crates.io/crates/aws-sdk-s3-transfer-manager

use super::codec::{Codec, CodecEncoder};
use super::framing::{FramedEncoder, OutputFormat};
use super::output::{BoxFinishFuture, OutputSink, OutputStats};
use super::path_template::{
    make_run_id, render_template, CollisionResult, CollisionTracker, TemplateValues,
};
use super::s3_streaming::{ChannelWriter, EncoderPartStream, CHANNEL_CAPACITY};
use crate::config::output::S3OutputConfig;
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3_transfer_manager as tm;
use bytes::Bytes;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

/// AWS S3 multipart upload limit.
const MAX_MPU_PARTS: u64 = 10_000;

/// In-flight per-prefix batch state.
///
/// `upload` is `None` until the first matched line for the prefix actually
/// arrives — opening the TM upload eagerly would create empty multipart
/// uploads for prefixes that never match. On rollover (`batch_max_mb`
/// crossed) the upload is closed and the slot returns to `None`, ready for
/// the next match's open. `seq` is the monotonic part identifier preserved
/// across rollovers.
struct PrefixBatch {
    upload: Option<ActiveUpload>,
    seq: u64,
}

/// One running TM multipart upload. Its encoder pushes compressed bytes
/// through `ChannelWriter` → mpsc channel → `EncoderPartStream` → TM, which
/// uploads parts to S3 concurrently. Streaming means the entire batch
/// never sits in RAM: at most ~`channel_cap × write-size` + `part_size` is
/// resident per active upload.
struct ActiveUpload {
    encoder: FramedEncoder<CodecEncoder<ChannelWriter>>,
    /// Background task driving the TM upload and accounting result stats.
    /// Pushed onto `Inner.pending_uploads` on close — `finish()` awaits all
    /// of them before returning stats.
    handle: JoinHandle<()>,
    /// Cumulative compressed bytes shipped through `ChannelWriter`. Shared
    /// with the writer so this struct can sample it for the per-batch
    /// `batch_max_bytes` rollover check without locking.
    bytes_sent: Arc<AtomicU64>,
    /// Lines + plaintext counts for the currently-open batch. Shared with
    /// the upload's background task so it can fold into global counters
    /// (or `lines_dropped` on failure) after the upload completes.
    batch_stats: Arc<BatchStats>,
    /// Rendered destination key — kept for logging.
    key: String,
}

#[derive(Default)]
struct BatchStats {
    lines: AtomicU64,
    plaintext: AtomicU64,
}

/// Owned state shared between ingest path and the per-prefix upload tasks.
struct Inner {
    /// AWS transfer manager client. Wraps our existing `aws_sdk_s3::Client`
    /// (we pass it via `Config::Builder::client(...)`) and runs each
    /// per-prefix upload as a multipart transfer (the streaming
    /// `PartStream` source is MPU-only — see
    /// `aws-sdk-s3-transfer-manager` `is_mpu_only`).
    tm: tm::Client,
    /// Tokio runtime handle captured at construction. Used to spawn
    /// per-upload tasks from the synchronous `ingest` path (which runs
    /// inside `spawn_blocking`).
    runtime: Handle,
    bucket: String,
    key_template: String,
    /// `Some(n)` enables size-based mid-run rollover at `n` compressed bytes
    /// shipped per prefix. `None` disables rollover — each prefix produces
    /// one object covering the whole run.
    batch_max_bytes: Option<u64>,
    /// Configured `multipart_part_mb` in bytes. Determines the part size we
    /// hand to TM and the upper-bound content-length hint we advertise to
    /// TM's part-size planner.
    part_bytes: u64,
    codec: Codec,
    format: OutputFormat,
    run_id: String,
    /// Per-prefix mutable state. Outer Mutex protects map insertion only;
    /// per-prefix entries are `Mutex<PrefixBatch>` so different prefixes
    /// never block each other.
    prefixes: Mutex<HashMap<String, Arc<Mutex<PrefixBatch>>>>,
    /// `JoinHandle`s for in-flight upload tasks. Each one decrements
    /// `inflight_bytes` as parts ship to TM and updates global counters
    /// (`objects_written`, `compressed_bytes`, `lines_dropped`, `fatal`)
    /// on completion. `finish()` joins all of these before returning.
    pending_uploads: Mutex<Vec<JoinHandle<()>>>,
    /// `JoinHandle`s for the blocking close work spawned by
    /// `close_prefix`. Each of these eventually pushes its
    /// `ActiveUpload.handle` into `pending_uploads`, so `finish()` must
    /// await `close_tasks` first, then `pending_uploads`.
    close_tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Defence-in-depth: warn (don't error) when two distinct source
    /// prefixes render to the same destination key. Static validation
    /// catches the common cases at config-resolve time; this catches the
    /// residual case (e.g. `{prefix_hash}` collisions on real inputs).
    /// Per upload, since each PutObject silently overwrites the previous,
    /// the run still completes — we just want the operator to know.
    collisions: Mutex<CollisionTracker>,
    /// Counters for end-of-run stats.
    matched_lines: AtomicU64,
    plaintext_bytes: AtomicU64,
    compressed_bytes: AtomicU64,
    lines_dropped: AtomicU64,
    objects_written: AtomicU64,
    /// Sink-global sum of bytes currently resident in our streaming
    /// pipeline (channel queues + reader pending buffers across all
    /// active uploads). Incremented by `ChannelWriter::write` and
    /// decremented by `EncoderPartStream::poll_part` when parts are
    /// handed to TM.
    inflight_bytes: Arc<AtomicU64>,
    /// Max observed value of `inflight_bytes` over the run. Surfaced in
    /// `OutputStats.extras` so the e2e suite can assert the sink doesn't
    /// buffer the whole run in memory.
    peak_inflight_bytes: Arc<AtomicU64>,
    /// Count of currently-open TM upload contexts. Incremented when
    /// `open_upload` spawns a driver task; decremented at the end of
    /// that driver task (i.e. once TM finalizes the multipart). Sampled
    /// by the progress reporter via `SinkObservability` to scale the
    /// per-upload "channel backed up" threshold.
    active_uploads: Arc<AtomicUsize>,
    fatal: Arc<AtomicBool>,
    /// Once `true`, the sink is finalized and `ingest` returns an error
    /// instead of opening a fresh upload. Prevents `flush_batch`-style
    /// races where late-arriving lines would otherwise reopen a closed
    /// batch.
    finished: AtomicBool,
}

pub struct S3OutputSink {
    inner: Arc<Inner>,
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

        // Build the transfer-manager client around our existing aws-sdk-s3
        // client. The TM crate's Config::Builder::client(...) accepts an
        // already-configured `aws_sdk_s3::Client`, so credentials,
        // endpoint URL, and the cached DNS resolver flow through unchanged.
        let part_bytes = cfg.multipart_part_mb * 1024 * 1024;
        let threshold_bytes = cfg.multipart_threshold_mb * 1024 * 1024;
        let concurrency = match cfg.multipart_concurrency {
            None => tm::types::ConcurrencyMode::Auto,
            Some(n) => tm::types::ConcurrencyMode::Explicit(n),
        };
        let tm_config = tm::Config::builder()
            .client(client)
            .multipart_threshold(tm::types::PartSize::Target(threshold_bytes))
            .part_size(tm::types::PartSize::Target(part_bytes))
            .concurrency(concurrency)
            .build();
        let tm_client = tm::Client::new(tm_config);

        let run_id = make_run_id();
        let codec = Codec::from_config(&cfg.compression)?;

        // `S3OutputSink::new` runs inside the tokio runtime (called from
        // async `main`). Capture the handle so the synchronous `ingest`
        // path can spawn upload tasks via `runtime.spawn(...)`.
        let runtime = Handle::current();

        let inner = Arc::new(Inner {
            tm: tm_client,
            runtime,
            bucket: cfg.bucket.clone(),
            key_template: cfg.key_template.clone(),
            batch_max_bytes: cfg.batch_max_mb.map(|mb| (mb * 1_000_000.0) as u64),
            part_bytes,
            codec,
            format: cfg.format.clone(),
            run_id,
            prefixes: Mutex::new(HashMap::new()),
            pending_uploads: Mutex::new(Vec::new()),
            close_tasks: Mutex::new(Vec::new()),
            collisions: Mutex::new(CollisionTracker::new()),
            matched_lines: AtomicU64::new(0),
            plaintext_bytes: AtomicU64::new(0),
            compressed_bytes: AtomicU64::new(0),
            lines_dropped: AtomicU64::new(0),
            objects_written: AtomicU64::new(0),
            inflight_bytes: Arc::new(AtomicU64::new(0)),
            peak_inflight_bytes: Arc::new(AtomicU64::new(0)),
            active_uploads: Arc::new(AtomicUsize::new(0)),
            fatal: Arc::new(AtomicBool::new(false)),
            finished: AtomicBool::new(false),
        });

        Ok(Self { inner })
    }

    /// Build a fresh active upload for `prefix`. Renders the destination
    /// key, opens an mpsc channel between the encoder side and TM's
    /// `PartStream`, kicks off the TM multipart upload as a background
    /// task, and returns the writer-side state for the per-prefix batch.
    ///
    /// `seq` is the value to substitute into `{seq}` for this batch's key —
    /// caller increments their own `PrefixBatch.seq` afterwards.
    fn open_upload(inner: &Arc<Inner>, prefix: &str, seq: u64) -> Result<ActiveUpload> {
        let key = render_template(
            &inner.key_template,
            &TemplateValues {
                prefix,
                run_id: &inner.run_id,
                seq,
                ext: inner.codec.extension(),
            },
        );

        // Defence-in-depth collision check. Static validation forbids
        // templates without `{prefix}`/`{prefix_hash}`, so this only
        // fires on residual cases (hash collisions, weird literals).
        // Warn-only — the upload still goes through.
        {
            let mut tracker = inner.collisions.lock().unwrap_or_else(|e| e.into_inner());
            if let CollisionResult::Collision { existing_prefix } = tracker.record(prefix, &key) {
                warn!(
                    bucket = %inner.bucket,
                    key = %key,
                    existing_prefix = %existing_prefix,
                    new_prefix = %prefix,
                    "S3 output: distinct source prefixes render to the same destination key — \
                     the second upload will overwrite the first"
                );
            }
        }

        // Channel capacity is small on purpose — see `CHANNEL_CAPACITY` doc.
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_CAPACITY);

        let bytes_sent = Arc::new(AtomicU64::new(0));
        let writer = ChannelWriter::new(
            tx,
            inner.inflight_bytes.clone(),
            inner.peak_inflight_bytes.clone(),
            bytes_sent.clone(),
        );
        let codec_enc = inner
            .codec
            .encoder(writer)
            .context("create streaming codec encoder")?;
        let framed = FramedEncoder::new(codec_enc, inner.format.clone());

        // Upper bound on content length: TM uses it to pick `part_size` so
        // total parts stay ≤ MAX_MPU_PARTS. For the bounded-batch case the
        // upper is `batch_max_bytes + part_bytes` (the rollover check fires
        // *after* the threshold cross, so we may overshoot by up to one
        // codec block). For the unbounded case we advertise
        // `part_bytes * MAX_MPU_PARTS` — TM will leave `part_size` as
        // configured, allowing up to ~50 GB per object at default
        // settings before it would auto-bump part_size.
        let upper_hint = match inner.batch_max_bytes {
            Some(cap) => cap + inner.part_bytes,
            None => inner.part_bytes.saturating_mul(MAX_MPU_PARTS),
        };
        let part_stream =
            EncoderPartStream::new(rx, inner.inflight_bytes.clone()).with_upper_size(upper_hint);
        let input_stream = tm::io::InputStream::from_part_stream(part_stream);

        let mut req = inner
            .tm
            .upload()
            .bucket(&inner.bucket)
            .key(&key)
            .body(input_stream)
            .content_type(if inner.format.is_json_array() {
                "application/json"
            } else {
                "application/x-ndjson"
            });
        if let Some(enc) = inner.codec.content_encoding() {
            req = req.content_encoding(enc);
        }

        let upload_handle = req
            .initiate()
            .map_err(|e| anyhow!("S3 sink: failed to initiate upload for `{key}`: {e}"))?;

        // From this moment until the TM driver task exits, this upload
        // contributes to `inflight_bytes` (channel + reader pending) and
        // counts as an "active upload" for the progress reporter's
        // backpressure heuristic. Decrement in the driver task's
        // closure below.
        inner.active_uploads.fetch_add(1, Ordering::Relaxed);

        let batch_stats = Arc::new(BatchStats::default());

        // Spawn a tiny driver task that awaits TM completion, then folds
        // per-batch counts into the sink's global counters (or
        // `lines_dropped` + `fatal` on failure).
        let driver = {
            let inner = inner.clone();
            let key_for_task = key.clone();
            let stats = batch_stats.clone();
            let bytes_sent_for_task = bytes_sent.clone();
            async move {
                let result = upload_handle.join().await;
                // Whatever TM's outcome, this upload is no longer active —
                // its mpsc is fully drained and its driver is exiting.
                inner.active_uploads.fetch_sub(1, Ordering::Relaxed);
                let lines = stats.lines.load(Ordering::Relaxed);
                let plaintext = stats.plaintext.load(Ordering::Relaxed);
                let bytes = bytes_sent_for_task.load(Ordering::Relaxed);
                match result {
                    Ok(_) => {
                        inner.compressed_bytes.fetch_add(bytes, Ordering::Relaxed);
                        inner.objects_written.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            bucket = %inner.bucket,
                            key = %key_for_task,
                            bytes,
                            lines,
                            plaintext,
                            "S3 streaming upload"
                        );
                    }
                    Err(e) => {
                        inner.lines_dropped.fetch_add(lines, Ordering::Relaxed);
                        let chain = error_chain_str(&e);
                        if !crate::s3::is_recoverable_s3_error(&chain) {
                            error!(
                                bucket = %inner.bucket,
                                key = %key_for_task,
                                lines,
                                error = %chain,
                                "Fatal S3 upload error, stopping pipeline"
                            );
                            inner.fatal.store(true, Ordering::Relaxed);
                        } else {
                            error!(
                                bucket = %inner.bucket,
                                key = %key_for_task,
                                lines,
                                error = %chain,
                                "S3 upload failed"
                            );
                        }
                    }
                }
            }
        };
        let handle = inner.runtime.spawn(driver);

        Ok(ActiveUpload {
            encoder: framed,
            handle,
            bytes_sent,
            batch_stats,
            key,
        })
    }

    /// Close an active upload — finalize the framing + codec layers (which
    /// emits any trailing JSON-array `]` and codec frame tail), drop the
    /// `ChannelWriter` so the channel closes and TM's `PartStream` sees
    /// EOF, then push the driver task's join handle into `pending_uploads`
    /// so `finish()` can await it.
    fn close_upload(inner: &Arc<Inner>, active: ActiveUpload) -> Result<()> {
        let ActiveUpload {
            encoder,
            handle,
            bytes_sent: _,
            batch_stats: _,
            key,
        } = active;

        // Finalize JSON-array framing. May write `]` through the codec.
        let codec_enc = encoder
            .finish()
            .with_context(|| format!("close batch framing for `{key}`"))?;
        // Finalize codec frame (zstd/gzip trailer). Consumes the
        // `ChannelWriter`; dropping it closes the mpsc channel.
        let _writer = codec_enc
            .finish()
            .with_context(|| format!("finalize codec encoder for `{key}`"))?;
        // _writer drops here, channel closes, TM's PartStream returns
        // Poll::Ready(None) on its next poll, TM completes the multipart.

        inner
            .pending_uploads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
        Ok(())
    }
}

/// Render an error and its source chain into a single string. The TM
/// crate wraps the underlying aws-sdk-s3 errors, so the recoverable /
/// fatal classifier needs to see the full chain to find substrings like
/// `dispatch failure` or `403 Forbidden`.
fn error_chain_str(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut src = err.source();
    while let Some(s) = src {
        out.push_str(": ");
        out.push_str(&s.to_string());
        src = s.source();
    }
    out
}

impl OutputSink for S3OutputSink {
    fn ingest(&self, prefix: &str, line: &[u8]) -> Result<()> {
        if self.inner.finished.load(Ordering::Relaxed) {
            return Err(anyhow!("S3 sink already finished"));
        }

        let entry = {
            let mut map = self
                .inner
                .prefixes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            map.entry(prefix.to_string())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(PrefixBatch {
                        upload: None,
                        seq: 0,
                    }))
                })
                .clone()
        };

        let mut batch = entry.lock().unwrap_or_else(|e| e.into_inner());

        // Lazily open an upload on the first matched line for this prefix
        // (or after a rollover closed the previous batch). Deferring the
        // open until first byte means prefixes that match nothing never
        // create empty multipart uploads.
        if batch.upload.is_none() {
            let seq = batch.seq;
            let active = Self::open_upload(&self.inner, prefix, seq)?;
            batch.upload = Some(active);
            batch.seq += 1;
        }

        let active = batch.upload.as_mut().expect("just opened above");
        active
            .encoder
            .write_item(line)
            .context("streaming encoder write")?;
        active.batch_stats.lines.fetch_add(1, Ordering::Relaxed);
        active
            .batch_stats
            .plaintext
            .fetch_add(line.len() as u64, Ordering::Relaxed);

        self.inner.matched_lines.fetch_add(1, Ordering::Relaxed);
        self.inner
            .plaintext_bytes
            .fetch_add(line.len() as u64, Ordering::Relaxed);

        // Rollover check: have we shipped enough compressed bytes through
        // this writer to cross `batch_max_bytes`? `bytes_sent` is
        // monotonic, so we just sample it.
        if let Some(threshold) = self.inner.batch_max_bytes {
            let shipped = active.bytes_sent.load(Ordering::Relaxed);
            if shipped >= threshold {
                let active = batch.upload.take().expect("just confirmed Some");
                Self::close_upload(&self.inner, active)?;
                // The next ingest for this prefix will open a fresh upload
                // with `seq` incremented above. No need to do it here.
            }
        }
        Ok(())
    }

    fn close_prefix(&self, prefix: &str) {
        // If `finish()` has already started, the trailing close path
        // there will handle whatever's left — bail to avoid races with
        // the prefix map drain.
        if self.inner.finished.load(Ordering::Relaxed) {
            return;
        }

        // Take the ActiveUpload out under the per-prefix lock (cheap)
        // without holding the outer prefix-map lock during the heavy
        // finalize work.
        let active_opt = {
            let map = self
                .inner
                .prefixes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let entry = map.get(prefix).cloned();
            drop(map);
            entry.and_then(|arc| arc.lock().unwrap_or_else(|e| e.into_inner()).upload.take())
        };
        let Some(active) = active_opt else {
            // No active upload — either close_prefix was called more than
            // once (CAS should have prevented this) or this prefix never
            // got a matched line. Either way, nothing to do.
            return;
        };

        // Finalizing the encoder may write a frame trailer through
        // ChannelWriter::write -> mpsc::blocking_send, which would panic
        // if invoked from a runtime worker. spawn_blocking moves the work
        // onto a blocking thread regardless of caller context.
        let inner = self.inner.clone();
        let prefix_for_task = prefix.to_string();
        let handle = self.inner.runtime.spawn_blocking(move || {
            if let Err(e) = Self::close_upload(&inner, active) {
                warn!(
                    prefix = %prefix_for_task,
                    error = %e,
                    "S3 sink: failed to close prefix upload",
                );
            }
        });
        self.inner
            .close_tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    fn finish<'a>(&'a self) -> BoxFinishFuture<'a> {
        Box::pin(async move {
            self.inner.finished.store(true, Ordering::Relaxed);

            // Close every active per-prefix upload. Each close drops the
            // writer → channel EOF → TM's PartStream returns
            // Poll::Ready(None) → TM completes the multipart upload.
            //
            // close_upload finalizes the codec encoder, which can write a
            // frame trailer through `ChannelWriter::write` →
            // `blocking_send`. Calling that from this async context
            // (a runtime worker thread) panics with "Cannot block the
            // current thread from within a runtime", so we hop to a
            // blocking thread for each close.
            let prefixes: Vec<(String, Arc<Mutex<PrefixBatch>>)> = {
                let mut map = self
                    .inner
                    .prefixes
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                map.drain().collect()
            };
            for (prefix, entry) in prefixes {
                let active_opt = entry
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .upload
                    .take();
                if let Some(active) = active_opt {
                    let inner_for_close = self.inner.clone();
                    let close_result = tokio::task::spawn_blocking(move || {
                        Self::close_upload(&inner_for_close, active)
                    })
                    .await;
                    match close_result {
                        Err(e) => warn!(
                            prefix = %prefix,
                            error = %e,
                            "S3 sink: close_upload task panicked"
                        ),
                        Ok(Err(e)) => warn!(
                            prefix = %prefix,
                            error = %e,
                            "S3 sink: failed to close trailing upload"
                        ),
                        Ok(Ok(())) => {}
                    }
                }
            }

            // First await every close_tasks handle — those are the
            // spawn_blocking finalizers triggered by `close_prefix`
            // mid-run. Each of them pushes its upload's driver
            // `JoinHandle` into `pending_uploads` as the last thing it
            // does, so this drain must happen before we read
            // `pending_uploads` below.
            let close_handles = std::mem::take(
                &mut *self
                    .inner
                    .close_tasks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
            );
            for handle in close_handles {
                if let Err(e) = handle.await {
                    warn!(error = %e, "S3 sink: close_prefix task panicked");
                }
            }

            // Await every driver task — they've already started uploading
            // (some may have finished mid-run), so this is just the join.
            let handles = std::mem::take(
                &mut *self
                    .inner
                    .pending_uploads
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
            );
            for handle in handles {
                if let Err(e) = handle.await {
                    warn!(error = %e, "S3 upload driver task panicked");
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
                    (
                        "peak_inflight_bytes".to_string(),
                        json!(self.inner.peak_inflight_bytes.load(Ordering::Relaxed)),
                    ),
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

    fn sink_observability(&self) -> crate::pipeline::SinkObservability {
        crate::pipeline::SinkObservability {
            inflight_bytes: Some(self.inner.inflight_bytes.clone()),
            active_uploads: Some(self.inner.active_uploads.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::path_template::short_hash;

    #[test]
    fn render_key_substitutes_all_placeholders() {
        let key = render_template(
            "out/{prefix}/{prefix_hash}-{seq}-{run_id}.{ext}",
            &TemplateValues {
                prefix: "logs/dt=20240101",
                run_id: "abcd1234",
                seq: 7,
                ext: "zst",
            },
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
