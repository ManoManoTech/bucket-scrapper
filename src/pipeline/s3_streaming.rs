//! Streaming bridge between the per-prefix codec encoder (sync `io::Write`)
//! and the AWS transfer manager's async `PartStream` interface.
//!
//! The s3 sink used to materialize the entire per-prefix compressed body in a
//! single `Vec<u8>` before handing it to TM. On long runs that buffer grew
//! unbounded — see the OOM regression test in `tests/e2e_pipeline.rs`. This
//! module replaces that buffer with a bounded tokio mpsc channel:
//!
//! ```text
//! filter worker  →  CodecEncoder<ChannelWriter>  →  mpsc (Bytes, cap=2)  →  EncoderPartStream  →  TM
//! ```
//!
//! `ChannelWriter` lives behind the codec encoder and ships each `write` as a
//! `Bytes` chunk through the channel. `EncoderPartStream` is what TM polls;
//! it aggregates up to `part_size` (TM-supplied, ≥ 5 MiB except on the last
//! part) before yielding a `PartData`. EOF (channel close) flushes whatever
//! is pending as the final part and then signals end-of-stream.
//!
//! The shared `inflight` / `peak` counters drive the `peak_inflight_bytes`
//! stat surfaced to the completion log — bytes are counted as "ours" while
//! they sit in the channel + `pending` buffer, and removed once a `PartData`
//! is yielded to TM.

use aws_sdk_s3_transfer_manager::io::{PartData, PartStream, SizeHint, StreamContext};
use bytes::{Bytes, BytesMut};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

/// Capacity of the writer→reader mpsc channel, in chunks. Two chunks keeps
/// one in-flight and one buffered without letting the producer race far
/// ahead. Each chunk is one `Write::write` call's worth of bytes — typically
/// a codec block (~128 KB for zstd at default level, smaller for gzip), so
/// steady-state channel residency is ~2 × block size per active prefix.
pub(super) const CHANNEL_CAPACITY: usize = 2;

/// Sync end of the streaming pipeline. Wraps an `mpsc::Sender<Bytes>` in an
/// `io::Write` so the codec encoder can push compressed bytes through it.
///
/// Lives inside the filter worker's `spawn_blocking` thread, which is why we
/// can call `Sender::blocking_send` (would deadlock on a runtime worker).
pub(super) struct ChannelWriter {
    tx: tokio::sync::mpsc::Sender<Bytes>,
    /// Sink-global sum of "ours" bytes across every active writer: those
    /// currently resident in either an mpsc channel or a reader's
    /// `pending` buffer. Decremented by `EncoderPartStream` when bytes are
    /// handed to TM.
    inflight: Arc<AtomicU64>,
    /// Sink-global high-water mark of `inflight`. Surfaced in
    /// `OutputStats.extras` as `peak_inflight_bytes`.
    peak: Arc<AtomicU64>,
    /// Per-writer monotonic byte counter — total bytes ever shipped through
    /// this writer's channel. Used by the s3 sink to decide whether a
    /// per-prefix batch has crossed `batch_max_mb` and should roll over.
    bytes_sent: Arc<AtomicU64>,
}

impl ChannelWriter {
    pub(super) fn new(
        tx: tokio::sync::mpsc::Sender<Bytes>,
        inflight: Arc<AtomicU64>,
        peak: Arc<AtomicU64>,
        bytes_sent: Arc<AtomicU64>,
    ) -> Self {
        Self {
            tx,
            inflight,
            peak,
            bytes_sent,
        }
    }
}

impl io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = buf.len();
        let chunk = Bytes::copy_from_slice(buf);
        // Update inflight + peak before sending so the peak observation
        // includes this chunk even if the consumer drains immediately.
        let new_total = self.inflight.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
        self.peak.fetch_max(new_total, Ordering::Relaxed);
        self.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
        match self.tx.blocking_send(chunk) {
            Ok(()) => Ok(n),
            Err(_) => {
                // Reader dropped (TM upload failed/cancelled). Roll back the
                // inflight bump so the metric stays accurate. `bytes_sent`
                // is monotonic — we count it as "shipped" since the writer
                // attempted to push it; the threshold check is a heuristic
                // either way.
                self.inflight.fetch_sub(n as u64, Ordering::Relaxed);
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "S3 upload stream closed before encoder finished",
                ))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        // Nothing to flush — `write` ships each chunk synchronously.
        Ok(())
    }
}

/// Async end: yields part-size chunks to TM. Aggregates incoming `Bytes`
/// chunks into `pending` until at least `part_size` are available, then
/// emits a full part. On channel close, flushes whatever remains as the
/// final part (marked `is_last(true)`) and returns `None` on the next poll.
pub(super) struct EncoderPartStream {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    pending: BytesMut,
    part_number: u64,
    eof_seen: bool,
    /// Cleared (signals "we already yielded the trailing partial part") so
    /// the next `poll_part` correctly returns `Poll::Ready(None)`.
    finished: bool,
    inflight: Arc<AtomicU64>,
    /// Optional upper bound advertised via `size_hint().upper()`. TM uses
    /// this to plan `part_size` (so total parts stay ≤ 10_000); the
    /// streaming encoder genuinely does not know the final compressed
    /// size, so callers must supply a conservative ceiling.
    upper_size_hint: Option<u64>,
}

impl EncoderPartStream {
    pub(super) fn new(rx: tokio::sync::mpsc::Receiver<Bytes>, inflight: Arc<AtomicU64>) -> Self {
        Self {
            rx,
            pending: BytesMut::new(),
            part_number: 1, // S3 multipart part numbers are 1-based
            eof_seen: false,
            finished: false,
            inflight,
            upper_size_hint: None,
        }
    }

    /// Set the conservative upper bound advertised to TM. Without this,
    /// `try_start_upload` rejects the stream with
    /// `upper_bound_size_hint_required`. The value need not match the
    /// final transferred size — TM only uses it to pick a `part_size` so
    /// total parts stay under the AWS 10_000-part limit.
    pub(super) fn with_upper_size(mut self, upper: u64) -> Self {
        self.upper_size_hint = Some(upper);
        self
    }
}

impl PartStream for EncoderPartStream {
    fn poll_part(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<io::Result<PartData>>> {
        let me = self.get_mut();
        if me.finished {
            return Poll::Ready(None);
        }

        let part_size = stream_cx.part_size();

        // Drain anything ready on the channel into `pending` until we have
        // enough for a full part — or we hit Pending / EOF.
        loop {
            if me.pending.len() >= part_size {
                break;
            }
            match me.rx.poll_recv(cx) {
                Poll::Ready(Some(chunk)) => {
                    me.pending.extend_from_slice(&chunk);
                }
                Poll::Ready(None) => {
                    me.eof_seen = true;
                    break;
                }
                Poll::Pending => {
                    // If we already have at least one byte, the caller can
                    // start uploading a short non-final part — but TM
                    // enforces exact part_size on non-last parts, so we
                    // *must* wait here for either a full part or EOF.
                    return Poll::Pending;
                }
            }
        }

        if me.pending.len() >= part_size {
            // Yield exactly one full part; remainder stays in `pending`.
            let part_bytes = me.pending.split_to(part_size).freeze();
            let n = part_bytes.len() as u64;
            me.inflight.fetch_sub(n, Ordering::Relaxed);
            let pn = me.part_number;
            me.part_number += 1;
            // Explicitly mark non-last so TM's part-size validation is
            // active (errors on size mismatch instead of silently uploading).
            return Poll::Ready(Some(Ok(PartData::new(pn, part_bytes).mark_last(false))));
        }

        // At this point we've seen EOF and have < part_size pending.
        debug_assert!(me.eof_seen);

        if me.pending.is_empty() {
            // No data at all (or we already emitted the trailing piece).
            // First branch: stream was never written to before close — TM
            // requires ≥1 part, so callers must defer the upload until at
            // least one byte ships; we still need to surface end-of-stream
            // gracefully if it happens.
            me.finished = true;
            return Poll::Ready(None);
        }

        // Yield the trailing partial part, mark as last, then finish on the
        // next poll.
        let part_bytes = std::mem::take(&mut me.pending).freeze();
        let n = part_bytes.len() as u64;
        me.inflight.fetch_sub(n, Ordering::Relaxed);
        let pn = me.part_number;
        me.part_number += 1;
        me.finished = true;
        Poll::Ready(Some(Ok(PartData::new(pn, part_bytes).mark_last(true))))
    }

    fn size_hint(&self) -> SizeHint {
        // We don't know the final compressed size, but TM requires
        // `upper()` to be `Some` (it uses the value to pick `part_size`
        // so total parts stay ≤ 10_000). Callers set the upper hint via
        // `with_upper_size`; if they didn't, we fall back to `None` and
        // TM will reject the upload at start — a louder failure than
        // silently corrupting accounting.
        SizeHint::default().with_upper(self.upper_size_hint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // PartStream's behavior under load (part-size aggregation, last-part
    // marking, EOF after empty stream) is exercised end-to-end via the
    // s3_output_* tests in tests/e2e_pipeline.rs — `StreamContext` has a
    // crate-private constructor so a direct unit test of `poll_part` would
    // need to mock through TM's harness.

    #[tokio::test]
    async fn writer_increments_inflight_and_peak() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(2);
        let inflight = Arc::new(AtomicU64::new(0));
        let peak = Arc::new(AtomicU64::new(0));
        let bytes_sent = Arc::new(AtomicU64::new(0));
        let mut w = ChannelWriter::new(tx, inflight.clone(), peak.clone(), bytes_sent.clone());

        // Send three small chunks; the channel cap is 2 so the third send
        // will block until the receiver drains, which we do concurrently.
        let drainer = tokio::spawn(async move {
            let mut total = 0u64;
            while let Some(b) = rx.recv().await {
                total += b.len() as u64;
            }
            total
        });

        // Move the writer onto a blocking thread (the production path).
        let inflight_for_w = inflight.clone();
        let peak_for_w = peak.clone();
        let drain_total = tokio::task::spawn_blocking(move || {
            w.write_all(b"hello ").unwrap();
            // Peak should already reflect the first chunk.
            assert!(peak_for_w.load(Ordering::Relaxed) >= 6);
            w.write_all(b"world!").unwrap();
            w.write_all(b"!").unwrap();
            drop(w); // closes the channel
            inflight_for_w.load(Ordering::Relaxed)
        });

        let _ = drain_total.await.unwrap();
        let total_drained = drainer.await.unwrap();
        assert_eq!(total_drained, 13);
        // After draining, *we* haven't decremented inflight (only the
        // PartStream side does). So inflight still reads the writer's total.
        assert_eq!(inflight.load(Ordering::Relaxed), 13);
        assert_eq!(peak.load(Ordering::Relaxed), 13);
        // bytes_sent tracks cumulative writes — monotonic, never decremented.
        assert_eq!(bytes_sent.load(Ordering::Relaxed), 13);
    }

    #[tokio::test]
    async fn writer_blocking_send_errors_when_reader_dropped() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
        let inflight = Arc::new(AtomicU64::new(0));
        let peak = Arc::new(AtomicU64::new(0));
        let bytes_sent = Arc::new(AtomicU64::new(0));
        let mut w = ChannelWriter::new(tx, inflight.clone(), peak, bytes_sent);
        drop(rx);
        let err = tokio::task::spawn_blocking(move || w.write_all(b"x").unwrap_err())
            .await
            .unwrap();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        // Inflight should have been rolled back after the failed send.
        assert_eq!(inflight.load(Ordering::Relaxed), 0);
    }
}
