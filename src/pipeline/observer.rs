use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Tracks raw bytes downloaded from S3 (before decompression).
///
/// Incremented in `download_and_decompress_inner` right after `body.collect()`,
/// so it measures true S3 download throughput independent of search/upload speed.
#[derive(Clone)]
pub struct DownloadObserver {
    bytes: Arc<AtomicUsize>,
}

impl Default for DownloadObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadObserver {
    pub fn new() -> Self {
        Self {
            bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn add_bytes(&self, n: usize) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Type-erased channel fill-level observer.
///
/// Captures the `len()` and `capacity()` of a `flume` channel at construction
/// time without retaining the concrete item type `T`.  This lets
/// [`crate::progress::PipelineProgress`] observe decompressed-channel fill levels without
/// depending on the private `DownloadedObject` type.
pub struct ChannelObserver {
    len: Box<dyn Fn() -> usize + Send + Sync>,
    cap: usize,
}

impl ChannelObserver {
    /// Create an observer from any `flume::Receiver<T>`.
    ///
    /// **Warning:** clones the receiver, which keeps the channel alive even if
    /// all "real" receivers are dropped.  Prefer [`from_sender`](Self::from_sender)
    /// when possible — it observes the same `len()` without affecting channel
    /// lifetime.
    #[deprecated(
        note = "use from_sender; from_receiver clones the receiver and keeps the channel alive"
    )]
    pub fn from_receiver<T: Send + 'static>(rx: &flume::Receiver<T>) -> Self {
        let rx = rx.clone();
        let cap = rx.capacity().unwrap_or(0);
        Self {
            len: Box::new(move || rx.len()),
            cap,
        }
    }

    /// Create an observer from any `flume::Sender<T>`.
    ///
    /// Observes the same `len()` / `capacity()` as the receiver side but does
    /// **not** keep the channel alive — when all real receivers drop, senders
    /// get `SendError` as expected.
    pub fn from_sender<T: Send + 'static>(tx: &flume::Sender<T>) -> Self {
        let tx = tx.clone();
        let cap = tx.capacity().unwrap_or(0);
        Self {
            len: Box::new(move || tx.len()),
            cap,
        }
    }

    pub fn len(&self) -> usize {
        (self.len)()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }
}

/// Read-only view of pipeline channel fill levels.
///
/// Used by [`crate::progress::PipelineProgress`] and the download coordinator
/// to observe HTTP pipeline health without coupling to the concrete channel
/// item types (`CompressedBatch`, etc.) inside `http_writer`.
pub struct PipelineObserver {
    line_len: Box<dyn Fn() -> usize + Send + Sync>,
    line_cap: usize,
    batch_len: Box<dyn Fn() -> usize + Send + Sync>,
    batch_cap: usize,
    batches_uploaded: Arc<AtomicUsize>,
    upload_time_us: Arc<AtomicUsize>,
    compressed_bytes_sent: Arc<AtomicUsize>,
    /// Throttle rate bits (f64 as u64). `None` = throttle disabled.
    throttle_rate_bits: Option<Arc<AtomicU64>>,
}

impl PipelineObserver {
    /// Build from two flume senders of any item type, plus shared counters.
    pub fn new<L: Send + 'static, B: Send + 'static>(
        line_tx: &flume::Sender<L>,
        batch_tx: &flume::Sender<B>,
        batches_uploaded: Arc<AtomicUsize>,
        upload_time_us: Arc<AtomicUsize>,
        compressed_bytes_sent: Arc<AtomicUsize>,
        throttle_rate_bits: Option<Arc<AtomicU64>>,
    ) -> Self {
        let line_tx = line_tx.clone();
        let line_cap = line_tx.capacity().unwrap_or(0);
        let batch_tx = batch_tx.clone();
        let batch_cap = batch_tx.capacity().unwrap_or(0);
        Self {
            line_len: Box::new(move || line_tx.len()),
            line_cap,
            batch_len: Box::new(move || batch_tx.len()),
            batch_cap,
            batches_uploaded,
            upload_time_us,
            compressed_bytes_sent,
            throttle_rate_bits,
        }
    }

    pub fn line_len(&self) -> usize {
        (self.line_len)()
    }

    pub fn line_capacity(&self) -> usize {
        self.line_cap
    }

    pub fn batch_len(&self) -> usize {
        (self.batch_len)()
    }

    pub fn batch_capacity(&self) -> usize {
        self.batch_cap
    }

    pub fn batches_uploaded(&self) -> usize {
        self.batches_uploaded.load(Ordering::Relaxed)
    }

    pub fn compressed_bytes_sent(&self) -> usize {
        self.compressed_bytes_sent.load(Ordering::Relaxed)
    }

    /// Average batch upload time in milliseconds, or 0.0 if no batches yet.
    pub fn avg_upload_ms(&self) -> f64 {
        let count = self.batches_uploaded();
        if count == 0 {
            return 0.0;
        }
        let total_us = self.upload_time_us.load(Ordering::Relaxed) as f64;
        total_us / count as f64 / 1000.0
    }

    /// Current throttle rate in MB/s, or `None` if disabled or unlimited.
    pub fn throttle_rate_mbps(&self) -> Option<f64> {
        let bits_arc = self.throttle_rate_bits.as_ref()?;
        let rate = f64::from_bits(bits_arc.load(Ordering::Relaxed));
        if rate.is_infinite() {
            None
        } else {
            Some(rate / 1_000_000.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_sender_allows_channel_close_on_receiver_drop() {
        let (tx, rx) = flume::bounded::<u8>(4);
        let _observer = ChannelObserver::from_sender(&tx);

        // Drop the only real receiver.
        drop(rx);

        // The channel should be closed — send must fail immediately.
        assert!(
            tx.send(1).is_err(),
            "from_sender observer must not keep the channel alive"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn from_receiver_keeps_channel_alive_after_receiver_drop() {
        let (tx, rx) = flume::bounded::<u8>(4);
        let _observer = ChannelObserver::from_receiver(&rx);

        // Drop the "real" receiver.
        drop(rx);

        // The observer still holds a cloned receiver, so the channel stays
        // open and send succeeds (this is the bug that from_sender fixes).
        assert!(
            tx.send(1).is_ok(),
            "from_receiver observer should keep the channel alive (demonstrating the bug)"
        );
    }

    #[test]
    fn from_sender_observes_len_and_capacity() {
        let (tx, rx) = flume::bounded::<u8>(8);
        let observer = ChannelObserver::from_sender(&tx);

        assert_eq!(observer.capacity(), 8);
        assert_eq!(observer.len(), 0);
        assert!(observer.is_empty());

        tx.send(42).unwrap();
        tx.send(43).unwrap();
        assert_eq!(observer.len(), 2);

        let _ = rx.recv().unwrap();
        assert_eq!(observer.len(), 1);
    }
}
