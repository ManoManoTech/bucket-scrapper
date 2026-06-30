//! Runtime control plane: a Unix-domain-socket server that lets an operator
//! retune a *running* pipeline (and read its live state) without restarting a
//! long S3 sweep. The companion client is the `bsctl` binary.
//!
//! Wire protocol is deliberately trivial: one newline-delimited JSON
//! [`ControlRequest`] per line in, one newline-delimited [`ControlResponse`]
//! per line out, then the connection may be reused or closed. No framing
//! library, no RPC crate — just `serde_json` over [`tokio::net::UnixStream`].
//!
//! The knobs map onto the live primitives the pipeline already shares via
//! `Arc`: the two download `tokio::sync::Semaphore`s (resized with
//! `add_permits` / acquire-then-`forget`), the filter-worker pool (grown by
//! spawning into the orchestrator's `JoinSet`, shrunk via a shared
//! retire-counter the workers honor), and the chunk-size atomic read at
//! download-dispatch time.

pub mod server;

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Default control-socket path, shared by the daemon (where it listens) and
/// `bsctl` (where it connects). Relative, so it resolves to the process's
/// working directory — for the container image that's the `/app` WORKDIR, so
/// `docker exec <container> ./bsctl status` reaches the running scrapper with
/// no flags. Override on either side with `--control-socket` / `--socket`;
/// disable the daemon's plane entirely with `--no-socket`.
pub const DEFAULT_SOCKET_PATH: &str = "bs.sock";

/// Live, shared tuning state for one pipeline run. Built once in
/// `StreamingDownloader::new`, held behind an `Arc`, and mutated by the
/// control server while the pipeline reads from it.
///
/// The two semaphores are the *same* `Arc`s the download coordinator acquires
/// from, so resizing them here is immediately visible to the pipeline. Filter
/// workers are not a semaphore — growth is signalled out-of-band to the
/// orchestrator's join loop (which owns the `JoinSet`), and shrink is the
/// [`filter_retire`](Self::filter_retire) counter workers honor.
pub struct RuntimeControls {
    /// Bounds concurrent download+decompress tasks ("downloaders").
    pub file_semaphore: Arc<Semaphore>,
    /// Bounds concurrent range GETs within a chunked download.
    pub download_semaphore: Arc<Semaphore>,
    /// Volunteer-retire counter: filter workers claim one (CAS) at their next
    /// line boundary and exit. Cloned into each worker.
    pub filter_retire: Arc<AtomicUsize>,
    /// Active part size in bytes; `0` disables chunking. Read at dispatch.
    pub chunk_size: Arc<AtomicUsize>,
    /// Current file-semaphore target (permits added minus forgotten).
    file_limit: AtomicUsize,
    /// Current download-semaphore target.
    range_limit: AtomicUsize,
}

impl RuntimeControls {
    /// `file_slots` seeds the file semaphore, `range_concurrency` the download
    /// semaphore, `chunk_size_bytes` the part size (`0` = disabled).
    pub fn new(file_slots: usize, range_concurrency: usize, chunk_size_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            file_semaphore: Arc::new(Semaphore::new(file_slots.max(1))),
            download_semaphore: Arc::new(Semaphore::new(range_concurrency.max(1))),
            filter_retire: Arc::new(AtomicUsize::new(0)),
            chunk_size: Arc::new(AtomicUsize::new(chunk_size_bytes)),
            file_limit: AtomicUsize::new(file_slots.max(1)),
            range_limit: AtomicUsize::new(range_concurrency.max(1)),
        })
    }

    pub fn file_limit(&self) -> usize {
        self.file_limit.load(Ordering::Relaxed)
    }

    pub fn range_limit(&self) -> usize {
        self.range_limit.load(Ordering::Relaxed)
    }

    pub fn part_size_mb(&self) -> u64 {
        (self.chunk_size.load(Ordering::Relaxed) / 1_000_000) as u64
    }

    pub fn filter_retire_pending(&self) -> usize {
        self.filter_retire.load(Ordering::Relaxed)
    }

    /// Grow download-task concurrency by `n` (adds file-semaphore permits).
    /// Returns the new target.
    pub fn grow_download_tasks(&self, n: usize) -> usize {
        grow(&self.file_semaphore, &self.file_limit, n)
    }

    /// Shrink download-task concurrency by up to `n`, never below 1. Spawns a
    /// detached task to forget permits as in-flight downloaders release them,
    /// so this returns immediately. Returns the new target.
    pub fn shrink_download_tasks(&self, n: usize) -> usize {
        shrink(&self.file_semaphore, &self.file_limit, n, 1)
    }

    /// Grow range-GET concurrency by `n`. Returns the new target.
    pub fn grow_range_concurrency(&self, n: usize) -> usize {
        grow(&self.download_semaphore, &self.range_limit, n)
    }

    /// Shrink range-GET concurrency by up to `n`, never below 1. Returns the
    /// new target.
    pub fn shrink_range_concurrency(&self, n: usize) -> usize {
        shrink(&self.download_semaphore, &self.range_limit, n, 1)
    }

    /// Set the part size in MB (`0` disables chunking). Returns the prior MB.
    pub fn set_part_size_mb(&self, mb: u64) -> u64 {
        let prev = self
            .chunk_size
            .swap((mb as usize) * 1_000_000, Ordering::Relaxed);
        (prev / 1_000_000) as u64
    }
}

/// Add `n` permits to `sem` and bump `limit`. Returns the new limit.
fn grow(sem: &Arc<Semaphore>, limit: &AtomicUsize, n: usize) -> usize {
    if n == 0 {
        return limit.load(Ordering::Relaxed);
    }
    sem.add_permits(n);
    limit.fetch_add(n, Ordering::Relaxed) + n
}

/// Forget up to `n` permits from `sem` (clamped so the limit stays ≥ `floor`),
/// reducing concurrency as in-flight holders release. The forgetting runs in a
/// detached task because `acquire_many_owned` blocks until enough permits are
/// free. Returns the new (post-shrink) limit immediately.
fn shrink(sem: &Arc<Semaphore>, limit: &AtomicUsize, n: usize, floor: usize) -> usize {
    let current = limit.load(Ordering::Relaxed);
    let actual = n.min(current.saturating_sub(floor));
    if actual == 0 {
        return current;
    }
    let new_limit = current - actual;
    limit.store(new_limit, Ordering::Relaxed);
    let sem = sem.clone();
    tokio::spawn(async move {
        // u32 cast is safe: concurrency knobs are tiny. Acquire-then-forget
        // permanently removes the permits once they free up.
        if let Ok(permit) = sem.acquire_many_owned(actual as u32).await {
            permit.forget();
        }
    });
    new_limit
}

/// A single operator command. Adjust-* deltas are signed: positive grows,
/// negative shrinks. Set-* commands take an absolute value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ControlRequest {
    /// Read-only snapshot of effective params + live gauges.
    Status,
    /// ± filter worker tasks. Grow spawns into the orchestrator's JoinSet;
    /// shrink asks N workers to retire at their next line boundary.
    AdjustFilterWorkers { delta: i64 },
    /// ± concurrent download+decompress tasks ("downloaders"). Backed by the
    /// file semaphore — grow lets the coordinator spawn more downloaders.
    AdjustDownloadTasks { delta: i64 },
    /// ± range-GET concurrency *within* a chunked download. Backed by the
    /// download semaphore.
    AdjustRangeConcurrency { delta: i64 },
    /// Set the parallel-chunked-download part size in MB. `0` disables
    /// chunking. Applies to objects dispatched after the change.
    SetPartSizeMb { mb: u64 },
    /// Set the line-channel capacity. **Unsupported in v1** — flume bounded
    /// channels cannot be resized in place; the daemon replies
    /// [`ControlResponse::Unsupported`].
    SetLineBufferSize { size: usize },
}

/// The daemon's reply to a [`ControlRequest`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", content = "data", rename_all = "snake_case")]
pub enum ControlResponse {
    /// Reply to [`ControlRequest::Status`].
    Status(StatusSnapshot),
    /// An adjust/set command was applied. `before`/`after` are the knob's
    /// effective value either side of the change (already clamped).
    Applied {
        knob: String,
        before: i64,
        after: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    /// The command is recognized but not implemented in this build.
    Unsupported(String),
    /// The command could not be applied (parse/validation/runtime error).
    Error(String),
}

/// Live view of the pipeline, sampled from the same atomics + channel
/// observers the periodic progress reporter uses. All values are an
/// instantaneous read; rates are cumulative counters the caller can diff
/// across two `status` calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    // ── tunable knobs (current effective values) ──
    /// Filter worker tasks currently alive.
    pub filter_workers_alive: usize,
    /// Pending filter-worker retirements not yet claimed.
    pub filter_retire_pending: usize,
    /// Download-task concurrency target (file-semaphore permits).
    pub download_tasks_limit: usize,
    /// Range-GET concurrency target (download-semaphore permits).
    pub range_concurrency_limit: usize,
    /// Current part size in MB (`0` = chunking disabled).
    pub part_size_mb: u64,
    /// Line-channel capacity (fixed for the run in v1).
    pub line_buffer_size: usize,

    // ── live gauges ──
    /// In-flight range GETs right now.
    pub dl_active: usize,
    /// Live download+decompress tasks right now.
    pub files_in_flight: usize,
    /// Lines queued in the download→filter channel.
    pub line_channel_len: usize,
    /// Capacity of that channel.
    pub line_channel_cap: usize,
    /// Cumulative raw bytes downloaded from S3 (pre-decompress).
    pub downloaded_bytes: u64,
    /// Cumulative matched lines emitted to the sink.
    pub match_count: usize,
}

/// Parse one request line. Trims the trailing newline for the caller.
pub fn parse_request(line: &str) -> Result<ControlRequest, serde_json::Error> {
    serde_json::from_str(line.trim())
}

/// Render a request as a single line (no trailing newline added).
pub fn encode_request(req: &ControlRequest) -> String {
    serde_json::to_string(req).expect("ControlRequest serializes")
}

/// Render a response as a single line (no trailing newline added).
pub fn encode_response(resp: &ControlResponse) -> String {
    serde_json::to_string(resp).expect("ControlResponse serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips() {
        let cases = [
            ControlRequest::Status,
            ControlRequest::AdjustFilterWorkers { delta: -2 },
            ControlRequest::AdjustDownloadTasks { delta: 4 },
            ControlRequest::AdjustRangeConcurrency { delta: -1 },
            ControlRequest::SetPartSizeMb { mb: 16 },
            ControlRequest::SetLineBufferSize { size: 2000 },
        ];
        for req in cases {
            let line = encode_request(&req);
            assert!(!line.contains('\n'), "encoded request must be one line");
            assert_eq!(parse_request(&line).unwrap(), req);
        }
    }

    #[test]
    fn response_round_trips() {
        let snap = StatusSnapshot {
            filter_workers_alive: 8,
            filter_retire_pending: 0,
            download_tasks_limit: 32,
            range_concurrency_limit: 16,
            part_size_mb: 0,
            line_buffer_size: 1000,
            dl_active: 3,
            files_in_flight: 5,
            line_channel_len: 12,
            line_channel_cap: 1000,
            downloaded_bytes: 123_456,
            match_count: 42,
        };
        let cases = [
            ControlResponse::Status(snap),
            ControlResponse::Applied {
                knob: "filter_workers".into(),
                before: 8,
                after: 10,
                note: None,
            },
            ControlResponse::Unsupported("nope".into()),
            ControlResponse::Error("bad".into()),
        ];
        for resp in cases {
            let line = encode_response(&resp);
            let back: ControlResponse = serde_json::from_str(&line).unwrap();
            assert_eq!(back, resp);
        }
    }

    #[test]
    fn parse_tolerates_trailing_newline_and_whitespace() {
        let line = format!("{}\n", encode_request(&ControlRequest::Status));
        assert_eq!(parse_request(&line).unwrap(), ControlRequest::Status);
    }

    #[tokio::test]
    async fn grow_download_tasks_adds_permits() {
        let c = RuntimeControls::new(4, 16, 0);
        assert_eq!(c.file_limit(), 4);
        assert_eq!(c.file_semaphore.available_permits(), 4);
        assert_eq!(c.grow_download_tasks(3), 7);
        assert_eq!(c.file_limit(), 7);
        assert_eq!(c.file_semaphore.available_permits(), 7);
    }

    #[tokio::test]
    async fn shrink_download_tasks_forgets_permits_and_honors_floor() {
        let c = RuntimeControls::new(8, 16, 0);
        // Shrink by 3 → target 5; the detached forget task removes permits.
        assert_eq!(c.shrink_download_tasks(3), 5);
        assert_eq!(c.file_limit(), 5);
        tokio::task::yield_now().await;
        // Idle semaphore: all 8 free, 3 get forgotten → 5 remain.
        assert_eq!(c.file_semaphore.available_permits(), 5);

        // Cannot go below the floor of 1.
        assert_eq!(c.shrink_download_tasks(100), 1);
        assert_eq!(c.file_limit(), 1);
    }

    #[tokio::test]
    async fn set_part_size_round_trips_mb() {
        let c = RuntimeControls::new(4, 16, 0);
        assert_eq!(c.part_size_mb(), 0);
        assert_eq!(c.set_part_size_mb(16), 0);
        assert_eq!(c.part_size_mb(), 16);
        assert_eq!(c.chunk_size.load(Ordering::Relaxed), 16_000_000);
        assert_eq!(c.set_part_size_mb(0), 16);
        assert_eq!(c.part_size_mb(), 0);
    }

    #[test]
    fn retire_cas_lets_exactly_one_claimant_win_per_unit() {
        // Model the worker-side claim: N units posted, each claimed once.
        let retire = Arc::new(AtomicUsize::new(3));
        let mut claims = 0;
        // Drain as a single worker would across many boundary checks.
        loop {
            let mut pending = retire.load(Ordering::Relaxed);
            if pending == 0 {
                break;
            }
            while pending > 0 {
                match retire.compare_exchange_weak(
                    pending,
                    pending - 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        claims += 1;
                        break;
                    }
                    Err(observed) => pending = observed,
                }
            }
        }
        assert_eq!(claims, 3);
        assert_eq!(retire.load(Ordering::Relaxed), 0);
    }
}
