//! Per-prefix progress tracking for close-on-completion of S3 uploads.
//!
//! Each prefix in the run gets one [`PrefixProgress`] instance, built once
//! at the top of `StreamingDownloader::search_objects` and threaded through
//! the pipeline via `DecompressedLine.progress`. The orchestrator hands each
//! download task its prefix's `Arc<PrefixProgress>`; the download task
//! increments `sent` for every emitted line and decrements
//! `downloads_pending` on exit; the filter worker increments `processed`
//! for every line pulled off the channel.
//!
//! The "close ready" condition for prefix `p` is:
//!
//! ```text
//! downloads_pending[p] == 0  AND  sent[p] == processed[p]
//! ```
//!
//! Once observed by either the last filter worker to bump `processed` or
//! the last download task to bump `downloads_pending` to zero, a
//! single-winner CAS on `closed` fires `OutputSink::close_prefix(name)`.
//! `OutputSink::finish` is the safety net — anything that misses the
//! early-close path still gets closed at end-of-run, so a counter bug
//! degrades to "same RAM as before the fix", never to "hang forever".
//!
//! See [`crate::pipeline::orchestrator`] for the call sites and
//! [`crate::pipeline::s3_writer`] for the close handler.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free per-prefix progress counters + close guard.
///
/// All increments use `Relaxed` ordering — happens-before across stages
/// is established by the bounded `flume` channels carrying
/// `DecompressedLine` (which itself carries an `Arc<PrefixProgress>`).
#[derive(Debug)]
pub struct PrefixProgress {
    /// Lines emitted into the line channel for this prefix. Monotonic.
    /// Bumped by the download task in `emit_lines` before each
    /// `line_tx.send`.
    pub sent: AtomicU64,
    /// Lines pulled off the line channel for this prefix. Monotonic.
    /// Bumped by the filter worker after each `rx.recv()`, before the
    /// regex match (so non-matches still count — `sent` and `processed`
    /// both reflect the *channel* traffic, not what reached the sink).
    pub processed: AtomicU64,
    /// Open downloads for this prefix. Incremented at dispatch in
    /// `download_coordinator`, decremented when the task exits.
    pub downloads_pending: AtomicU64,
    /// CAS guard: only the first writer to flip this `true` actually
    /// invokes `OutputSink::close_prefix`.
    pub closed: AtomicBool,
    /// The prefix string itself. Stored once here so close handlers and
    /// end-of-run assertions don't need a separate map lookup. The
    /// `Arc<PrefixProgress>` is the canonical handle.
    pub name: String,
}

impl PrefixProgress {
    pub fn new(name: String) -> Arc<Self> {
        Arc::new(Self {
            sent: AtomicU64::new(0),
            processed: AtomicU64::new(0),
            downloads_pending: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            name,
        })
    }

    /// `true` iff `downloads_pending == 0 && sent == processed`.
    ///
    /// Caller must perform the `closed` CAS separately to decide whether
    /// to actually invoke the close path. This split lets the CAS happen
    /// only when the condition is genuinely met, avoiding a wasted CAS
    /// on the hot path of every line.
    pub fn is_drained(&self) -> bool {
        if self.downloads_pending.load(Ordering::Relaxed) != 0 {
            return false;
        }
        // Load sent before processed: if they're equal we conclude drained.
        // Since both counters are monotonic and download tasks have
        // already exited by the time downloads_pending==0, sent is frozen
        // when we observe downloads_pending==0, so this ordering is safe.
        let sent = self.sent.load(Ordering::Relaxed);
        let processed = self.processed.load(Ordering::Relaxed);
        sent == processed
    }

    /// Attempt to claim the close. Returns `true` exactly once across all
    /// callers; subsequent calls return `false`. The winning caller is
    /// expected to invoke `OutputSink::close_prefix(&self.name)`.
    pub fn try_claim_close(&self) -> bool {
        self.closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_drained_false_while_downloads_pending() {
        let p = PrefixProgress::new("x".into());
        p.downloads_pending.fetch_add(1, Ordering::Relaxed);
        p.sent.fetch_add(5, Ordering::Relaxed);
        p.processed.fetch_add(5, Ordering::Relaxed);
        assert!(!p.is_drained(), "downloads_pending > 0 must block close");
    }

    #[test]
    fn is_drained_false_while_sent_gt_processed() {
        let p = PrefixProgress::new("x".into());
        p.sent.fetch_add(10, Ordering::Relaxed);
        p.processed.fetch_add(7, Ordering::Relaxed);
        assert!(!p.is_drained(), "sent > processed must block close");
    }

    #[test]
    fn is_drained_true_when_all_zero() {
        let p = PrefixProgress::new("x".into());
        assert!(
            p.is_drained(),
            "freshly-built progress is drained (no work ever queued)"
        );
    }

    #[test]
    fn is_drained_true_after_balanced_counters() {
        let p = PrefixProgress::new("x".into());
        p.sent.fetch_add(42, Ordering::Relaxed);
        p.processed.fetch_add(42, Ordering::Relaxed);
        assert!(p.is_drained());
    }

    #[test]
    fn try_claim_close_wins_exactly_once() {
        let p = PrefixProgress::new("x".into());
        assert!(p.try_claim_close(), "first caller wins");
        assert!(!p.try_claim_close(), "second caller loses");
        assert!(!p.try_claim_close(), "third caller loses");
    }

    #[test]
    fn try_claim_close_is_thread_safe() {
        // Hammer 8 threads, each calling try_claim_close 1000 times against
        // the same instance. Exactly one of the 8000 calls must win.
        let p = PrefixProgress::new("x".into());
        let total_wins = std::sync::Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let p = p.clone();
            let wins = total_wins.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    if p.try_claim_close() {
                        wins.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(total_wins.load(Ordering::Relaxed), 1);
    }
}
