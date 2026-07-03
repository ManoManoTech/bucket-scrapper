//! Unix-domain-socket control server. One task per run; accepts newline-
//! delimited [`ControlRequest`] lines and replies with [`ControlResponse`]
//! lines. Spawned from `StreamingDownloader::search_objects` only when a
//! socket path was configured, and aborted when the run ends.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

use super::{
    encode_response, parse_request, ControlRequest, ControlResponse, RuntimeControls,
    StatusSnapshot,
};
use crate::pipeline::observer::{ChannelObserver, DownloadObserver, ReadPathMetrics};
use crate::pipeline::SinkObservability;
use crate::progress::classify_bottleneck_non_http;
use crate::sysmetrics::{self, CpuMeter};

/// How long a history we keep for the trailing-window rates, and how often the
/// background sampler records a point.
const WINDOW_RETAIN: Duration = Duration::from_secs(62);
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Read-only handles the server samples to answer `status`. These mirror the
/// gauges the periodic progress reporter already reads, so `status` and the
/// progress log can't disagree.
pub struct StatusHandles {
    /// Live filter workers.
    pub workers_alive: Arc<AtomicUsize>,
    /// Read-path gauges (`dl_active`, `files_in_flight`, `decoders_input_wait`).
    pub metrics: Arc<ReadPathMetrics>,
    /// Cumulative raw S3 bytes downloaded.
    pub download_observer: DownloadObserver,
    /// Cumulative decompressed bytes fed to the filter (for filter throughput).
    pub filter_bytes_in: Arc<AtomicUsize>,
    /// Cumulative matched lines.
    pub match_count: Arc<AtomicUsize>,
    /// Filter workers currently inside `sink.ingest` (classifier input).
    pub workers_in_ingest: Arc<AtomicUsize>,
    /// Sink-side gauges (classifier input; all `None` for the void sink).
    pub sink_obs: SinkObservability,
    /// Sink kind label (classifier input).
    pub sink_kind: &'static str,
    /// Fill level of the download→filter line channel.
    pub line_channel: ChannelObserver,
    /// Configured line-channel capacity (fixed for the run in v1).
    pub line_buffer_size: usize,
}

/// One throughput sample: elapsed-since-start plus the two cumulative byte
/// counters. Storing elapsed millis (not `Instant`) keeps the rate math a pure,
/// testable function.
#[derive(Clone, Copy)]
struct Sample {
    at_ms: u64,
    dl_bytes: u64,
    filter_bytes: u64,
}

/// Trailing-window throughput tracker. A background task records a [`Sample`]
/// each second; `status` computes MB/s over 10/30/60s windows from the ring.
pub struct ThroughputWindows {
    base: Instant,
    samples: Mutex<VecDeque<Sample>>,
}

impl ThroughputWindows {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            samples: Mutex::new(VecDeque::new()),
        })
    }

    fn record(&self, dl_bytes: u64, filter_bytes: u64) {
        let at_ms = self.base.elapsed().as_millis() as u64;
        let mut q = self.samples.lock().unwrap();
        q.push_back(Sample {
            at_ms,
            dl_bytes,
            filter_bytes,
        });
        let cutoff = at_ms.saturating_sub(WINDOW_RETAIN.as_millis() as u64);
        while q.front().is_some_and(|s| s.at_ms < cutoff) {
            q.pop_front();
        }
    }

    /// (download, filter) MB/s over the trailing `window`.
    fn rates(&self, window: Duration) -> (f64, f64) {
        let now_ms = self.base.elapsed().as_millis() as u64;
        let q = self.samples.lock().unwrap();
        let dl: Vec<(u64, u64)> = q.iter().map(|s| (s.at_ms, s.dl_bytes)).collect();
        let filt: Vec<(u64, u64)> = q.iter().map(|s| (s.at_ms, s.filter_bytes)).collect();
        let w = window.as_millis() as u64;
        (
            windowed_mbps(&dl, w, now_ms),
            windowed_mbps(&filt, w, now_ms),
        )
    }
}

/// Pure MB/s over a trailing `window_ms`: (bytes now − bytes at window start) /
/// elapsed. Uses the oldest sample within the window as the baseline; returns
/// 0.0 until there are two usable points spanning real time.
fn windowed_mbps(samples: &[(u64, u64)], window_ms: u64, now_ms: u64) -> f64 {
    let Some(&(last_ms, last_bytes)) = samples.last() else {
        return 0.0;
    };
    let cutoff = now_ms.saturating_sub(window_ms);
    // Oldest sample at or after the cutoff is the window baseline.
    let Some(&(first_ms, first_bytes)) = samples.iter().find(|&&(t, _)| t >= cutoff) else {
        return 0.0;
    };
    let dt_ms = last_ms.saturating_sub(first_ms);
    if dt_ms == 0 {
        return 0.0;
    }
    let dbytes = last_bytes.saturating_sub(first_bytes);
    // MB/s = (bytes / seconds) / 1e6 = (bytes / (ms/1000)) / 1e6 = bytes/ms / 1000.
    dbytes as f64 / dt_ms as f64 / 1000.0
}

/// Everything the server needs to apply commands and answer `status`.
pub struct ControlContext {
    pub controls: Arc<RuntimeControls>,
    /// Sends "grow filter workers by N" to the orchestrator's join loop, which
    /// owns the `JoinSet`. Shrink uses `controls.filter_retire` directly.
    pub grow_workers: flume::Sender<usize>,
    pub status: StatusHandles,
    throughput: Arc<ThroughputWindows>,
    /// Latest busy %CPU (f64 bits; NaN = no reading), published by the sampler.
    cpu_percent: Arc<AtomicU64>,
}

impl ControlContext {
    /// Build a context, allocating its throughput tracker.
    pub fn new(
        controls: Arc<RuntimeControls>,
        grow_workers: flume::Sender<usize>,
        status: StatusHandles,
    ) -> Arc<Self> {
        Arc::new(Self {
            controls,
            grow_workers,
            status,
            throughput: ThroughputWindows::new(),
            cpu_percent: Arc::new(AtomicU64::new(f64::NAN.to_bits())),
        })
    }

    fn snapshot(&self) -> StatusSnapshot {
        let s = &self.status;
        let dc_len = s.line_channel.len();
        let dc_cap = s.line_channel.capacity();
        let dc_pct = (dc_len * 100).checked_div(dc_cap).unwrap_or(0);
        let workers_alive = s.workers_alive.load(Ordering::Relaxed);
        let decoders_input_wait = s.metrics.decoders_input_wait.load(Ordering::Relaxed);
        let files_in_flight = s.metrics.files_in_flight.load(Ordering::Relaxed);
        let reassembly_blocked = s.metrics.reassembly_blocked.load(Ordering::Relaxed);
        let bottleneck = classify_bottleneck_non_http(
            dc_pct,
            s.workers_in_ingest.load(Ordering::Relaxed),
            workers_alive.max(1),
            s.sink_obs
                .inflight_bytes
                .as_ref()
                .map(|a| a.load(Ordering::Relaxed)),
            s.sink_obs
                .active_uploads
                .as_ref()
                .map(|a| a.load(Ordering::Relaxed)),
            s.sink_kind,
            decoders_input_wait,
            files_in_flight,
            reassembly_blocked,
        );
        let (dl10, f10) = self.throughput.rates(Duration::from_secs(10));
        let (dl30, f30) = self.throughput.rates(Duration::from_secs(30));
        let (dl60, f60) = self.throughput.rates(Duration::from_secs(60));
        let cpu_psi = sysmetrics::cpu_pressure();

        StatusSnapshot {
            filter_workers_alive: workers_alive,
            filter_retire_pending: self.controls.filter_retire_pending(),
            download_tasks_limit: self.controls.file_limit(),
            range_concurrency_limit: self.controls.range_limit(),
            part_size_mb: self.controls.part_size_mb(),
            line_buffer_size: s.line_buffer_size,
            dl_active: s.metrics.dl_active.load(Ordering::Relaxed),
            files_in_flight,
            decoders_input_wait,
            line_channel_len: dc_len,
            line_channel_cap: dc_cap,
            downloaded_bytes: s.download_observer.bytes() as u64,
            match_count: s.match_count.load(Ordering::Relaxed),
            download_mbps_10s: dl10,
            download_mbps_30s: dl30,
            download_mbps_60s: dl60,
            filter_mbps_10s: f10,
            filter_mbps_30s: f30,
            filter_mbps_60s: f60,
            bottleneck: bottleneck.to_string(),
            cpu_percent: sysmetrics::load_f64(&self.cpu_percent),
            cpu_pressure_avg10: cpu_psi.map(|p| p.some_avg10),
            cpu_pressure_avg60: cpu_psi.map(|p| p.some_avg60),
            mem_pressure_avg10: sysmetrics::memory_pressure().map(|p| p.some_avg10),
        }
    }

    /// Apply one request and produce the reply.
    fn handle(&self, req: ControlRequest) -> ControlResponse {
        match req {
            ControlRequest::Status => ControlResponse::Status(self.snapshot()),

            ControlRequest::AdjustFilterWorkers { delta } => self.adjust_filter_workers(delta),

            ControlRequest::AdjustDownloadTasks { delta } => {
                let before = self.controls.file_limit() as i64;
                let after = if delta >= 0 {
                    self.controls.grow_download_tasks(delta as usize)
                } else {
                    self.controls.shrink_download_tasks((-delta) as usize)
                } as i64;
                applied("download_tasks", before, after, delta)
            }

            ControlRequest::AdjustRangeConcurrency { delta } => {
                let before = self.controls.range_limit() as i64;
                let after = if delta >= 0 {
                    self.controls.grow_range_concurrency(delta as usize)
                } else {
                    self.controls.shrink_range_concurrency((-delta) as usize)
                } as i64;
                applied("range_concurrency", before, after, delta)
            }

            ControlRequest::SetPartSizeMb { mb } => {
                let before = self.controls.set_part_size_mb(mb) as i64;
                ControlResponse::Applied {
                    knob: "part_size_mb".into(),
                    before,
                    after: mb as i64,
                    note: Some(if mb == 0 {
                        "chunking disabled; applies to objects dispatched after now".into()
                    } else {
                        "applies to objects dispatched after now".into()
                    }),
                }
            }

            ControlRequest::SetLineBufferSize { .. } => ControlResponse::Unsupported(
                "line-buffer resize is not supported in v1 (flume bounded channels \
                 have a fixed capacity); restart with --line-buffer-size instead"
                    .into(),
            ),
        }
    }

    fn adjust_filter_workers(&self, delta: i64) -> ControlResponse {
        // The *effective target* is the live count minus retirements already
        // queued but not yet claimed. Reporting against this (rather than raw
        // live count) keeps stacked shrinks consistent: each reply's `after`
        // is the next reply's `before`.
        let alive = self.status.workers_alive.load(Ordering::Relaxed);
        let pending = self.controls.filter_retire_pending();
        let target_before = alive.saturating_sub(pending) as i64;
        match delta.cmp(&0) {
            std::cmp::Ordering::Equal => applied("filter_workers", target_before, target_before, 0),
            std::cmp::Ordering::Greater => {
                let n = delta as usize;
                match self.grow_workers.send(n) {
                    Ok(()) => ControlResponse::Applied {
                        knob: "filter_workers".into(),
                        before: target_before,
                        after: target_before + n as i64,
                        note: Some("workers spawning asynchronously".into()),
                    },
                    // Channel closed ⇒ the run's join loop has finished/stopped
                    // accepting new workers (download complete).
                    Err(_) => {
                        ControlResponse::Error("cannot add workers: pipeline is finishing".into())
                    }
                }
            }
            std::cmp::Ordering::Less => {
                // Clamp so at least one worker survives once all queued
                // retirements (including this batch) are claimed.
                let claimable = (target_before - 1).max(0) as usize;
                let actual = ((-delta) as usize).min(claimable);
                if actual > 0 {
                    self.controls
                        .filter_retire
                        .fetch_add(actual, Ordering::Relaxed);
                }
                ControlResponse::Applied {
                    knob: "filter_workers".into(),
                    before: target_before,
                    after: target_before - actual as i64,
                    note: Some("workers retire at their next line boundary".into()),
                }
            }
        }
    }
}

fn applied(knob: &str, before: i64, after: i64, requested: i64) -> ControlResponse {
    let note =
        (after - before != requested).then(|| format!("clamped (requested delta {requested})"));
    ControlResponse::Applied {
        knob: knob.into(),
        before,
        after,
        note,
    }
}

/// Bind the socket and serve until aborted. Removes a stale socket file first
/// and on return. Errors that bubble up here are fatal to the control plane
/// only — the caller spawns this detached so the pipeline is unaffected.
pub async fn serve(socket_path: PathBuf, ctx: Arc<ControlContext>) -> Result<()> {
    // Best-effort removal of a stale socket from a prior crashed run.
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("binding control socket {}", socket_path.display()))?;
    restrict_permissions(&socket_path);
    info!(socket = %socket_path.display(), "Control socket listening");

    // Background sampler feeding the trailing-window rates and %CPU.
    let sampler = {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
            let mut cpu = CpuMeter::new();
            loop {
                tick.tick().await;
                ctx.throughput.record(
                    ctx.status.download_observer.bytes() as u64,
                    ctx.status.filter_bytes_in.load(Ordering::Relaxed) as u64,
                );
                sysmetrics::store_f64(&ctx.cpu_percent, cpu.sample());
            }
        })
    };

    let result = accept_loop(&listener, ctx).await;
    sampler.abort();
    let _ = std::fs::remove_file(&socket_path);
    result
}

async fn accept_loop(listener: &UnixListener, ctx: Arc<ControlContext>) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(stream, ctx).await {
                        warn!(error = %e, "Control connection ended with error");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "Control socket accept failed");
            }
        }
    }
}

async fn handle_conn(stream: UnixStream, ctx: Arc<ControlContext>) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let resp = match parse_request(&line) {
            Ok(req) => ctx.handle(req),
            Err(e) => ControlResponse::Error(format!("bad request: {e}")),
        };
        let mut out = encode_response(&resp);
        out.push('\n');
        write_half.write_all(out.as_bytes()).await?;
        write_half.flush().await?;
    }
    Ok(())
}

/// Tighten the socket to owner-only (0600). Best-effort: a failure here is
/// logged by the caller's flow only if bind already succeeded, and a
/// world-accessible socket on a single-operator host is not a regression.
fn restrict_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            warn!(error = %e, "Could not restrict control socket permissions");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::windowed_mbps;

    #[test]
    fn windowed_mbps_bytes_per_ms_equals_mb_per_s() {
        // 100 MB over 1000 ms = 100 MB/s. Samples: (t_ms, cumulative_bytes).
        let s = [(0u64, 0u64), (1000, 100_000_000)];
        assert!((windowed_mbps(&s, 10_000, 1000) - 100.0).abs() < 1e-6);
    }

    #[test]
    fn windowed_mbps_uses_only_samples_within_window() {
        // Old fast burst then a slow second; a 10s window baseline is the
        // oldest point ≥ (now-10s), so only recent throughput counts.
        let s = [
            (0u64, 0u64),            // outside a 10s window ending at 40_000
            (30_000, 3_000_000_000), // baseline within window
            (40_000, 3_050_000_000), // +50 MB over 10s = 5 MB/s
        ];
        assert!((windowed_mbps(&s, 10_000, 40_000) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn windowed_mbps_zero_without_span() {
        assert_eq!(windowed_mbps(&[], 10_000, 0), 0.0);
        assert_eq!(windowed_mbps(&[(500, 42)], 10_000, 500), 0.0); // single point
    }
}
