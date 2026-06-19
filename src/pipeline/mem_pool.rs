//! Input-buffer memory pool: a byte-bounded admission gate for download chunks.
//!
//! Mirrors the semantics of the `MemoryLimitedAllocator` in the sibling
//! `log-consolidator-checker-rs` project (reserve N bytes before fetching, free
//! on Drop, park the caller when the pool is full) but is implemented on a
//! `tokio::sync::Semaphore` whose permits *are* bytes — no extra dependency, and
//! the permit's RAII `Drop` is the release.
//!
//! It is a pure **admission gate**, not a recycling allocator: it bounds how
//! many chunk bytes may be resident at once (preventing OOM and providing
//! backpressure — when the decoder lags, reservations pile up and new range
//! GETs park here), but the bytes themselves are ordinary SDK `Bytes`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A snapshot of pool occupancy for the progress metrics.
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    /// Bytes currently reserved (in-flight GETs + out-of-order held chunks).
    pub used: usize,
    /// Total pool capacity in bytes.
    pub total: usize,
    /// High-water mark of `used` over the run.
    pub peak: usize,
    /// Tasks currently parked waiting for capacity.
    pub waiters: usize,
}

/// Byte-bounded admission gate. Construct once per run, share via `Arc`.
pub struct InputBufferPool {
    sem: Arc<Semaphore>,
    total: usize,
    peak: AtomicUsize,
    waiters: AtomicUsize,
}

impl InputBufferPool {
    /// Create a pool that admits at most `total_bytes` of resident chunk data.
    pub fn new(total_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(total_bytes)),
            total: total_bytes,
            peak: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
        })
    }

    /// Reserve `bytes` from the pool, awaiting (parking) until capacity frees up.
    /// The returned [`Loan`] releases the reservation on `Drop`.
    ///
    /// Fails fast if `bytes` exceeds total capacity — such a request could never
    /// be satisfied and would otherwise hang forever.
    pub async fn reserve(self: &Arc<Self>, bytes: usize) -> anyhow::Result<Loan> {
        if bytes > self.total {
            anyhow::bail!(
                "input-buffer reservation of {bytes} B exceeds pool capacity of {} B; \
                 raise --max-input-buffer-memory-mb or lower --download-chunk-size-mb",
                self.total
            );
        }
        // `acquire_many` takes a u32; chunk sizes are far below 4 GiB, but guard
        // anyway so a pathological config errors instead of panicking.
        let n: u32 = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("reservation {bytes} B too large for a single chunk"))?;

        self.waiters.fetch_add(1, Ordering::Relaxed);
        let permit = self
            .sem
            .clone()
            .acquire_many_owned(n)
            .await
            .map_err(|_| anyhow::anyhow!("input-buffer pool closed"));
        self.waiters.fetch_sub(1, Ordering::Relaxed);
        let permit = permit?;

        // Update the high-water mark with the new occupancy.
        let used = self.total - self.sem.available_permits();
        self.peak.fetch_max(used, Ordering::Relaxed);

        Ok(Loan {
            _permit: permit,
            bytes,
        })
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            used: self.total - self.sem.available_permits(),
            total: self.total,
            peak: self.peak.load(Ordering::Relaxed),
            waiters: self.waiters.load(Ordering::Relaxed),
        }
    }
}

/// RAII guard for a pool reservation. Dropping it returns the bytes to the pool
/// (and wakes a parked waiter, via the underlying semaphore).
#[derive(Debug)]
pub struct Loan {
    _permit: OwnedSemaphorePermit,
    /// Reserved size, exposed for the held-bytes gauge.
    pub bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn reserve_and_release_track_usage() {
        let pool = InputBufferPool::new(100);
        let a = pool.reserve(60).await.unwrap();
        assert_eq!(pool.stats().used, 60);
        let b = pool.reserve(40).await.unwrap();
        assert_eq!(pool.stats().used, 100);
        drop(a);
        assert_eq!(pool.stats().used, 40);
        drop(b);
        assert_eq!(pool.stats().used, 0);
    }

    #[tokio::test]
    async fn peak_is_high_water_mark() {
        let pool = InputBufferPool::new(100);
        let a = pool.reserve(70).await.unwrap();
        let b = pool.reserve(30).await.unwrap();
        drop(a);
        drop(b);
        assert_eq!(pool.stats().used, 0);
        assert_eq!(pool.stats().peak, 100, "peak should remember the max");
    }

    #[tokio::test]
    async fn reserve_parks_until_capacity_frees() {
        let pool = InputBufferPool::new(100);
        let big = pool.reserve(80).await.unwrap();

        // This 50 B reservation cannot be satisfied (only 20 B free) — it parks.
        let pool2 = pool.clone();
        let waiter = tokio::spawn(async move { pool2.reserve(50).await.map(|l| l.bytes) });

        // Give the waiter a moment to register, then confirm it's parked.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "reservation should be parked");
        assert_eq!(pool.stats().waiters, 1);

        drop(big); // frees 80 B → the parked 50 B reservation can proceed.
        let got = waiter.await.unwrap().unwrap();
        assert_eq!(got, 50);
        assert_eq!(pool.stats().waiters, 0);
    }

    #[tokio::test]
    async fn oversized_reservation_fails_fast() {
        let pool = InputBufferPool::new(100);
        let err = pool.reserve(101).await.unwrap_err();
        assert!(format!("{err}").contains("exceeds pool capacity"));
    }
}
