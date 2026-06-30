//! Unix-domain-socket control server. One task per run; accepts newline-
//! delimited [`ControlRequest`] lines and replies with [`ControlResponse`]
//! lines. Spawned from `StreamingDownloader::search_objects` only when a
//! socket path was configured, and aborted when the run ends.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

use super::{
    encode_response, parse_request, ControlRequest, ControlResponse, RuntimeControls,
    StatusSnapshot,
};
use crate::pipeline::observer::{ChannelObserver, DownloadObserver, ReadPathMetrics};

/// Read-only handles the server samples to answer `status`. These mirror the
/// gauges the periodic progress reporter already reads, so `status` and the
/// progress log can't disagree.
pub struct StatusHandles {
    /// Live filter workers.
    pub workers_alive: Arc<AtomicUsize>,
    /// Read-path gauges (`dl_active`, `files_in_flight`).
    pub metrics: Arc<ReadPathMetrics>,
    /// Cumulative raw S3 bytes downloaded.
    pub download_observer: DownloadObserver,
    /// Cumulative matched lines.
    pub match_count: Arc<AtomicUsize>,
    /// Fill level of the download→filter line channel.
    pub line_channel: ChannelObserver,
    /// Configured line-channel capacity (fixed for the run in v1).
    pub line_buffer_size: usize,
}

/// Everything the server needs to apply commands and answer `status`.
pub struct ControlContext {
    pub controls: Arc<RuntimeControls>,
    /// Sends "grow filter workers by N" to the orchestrator's join loop, which
    /// owns the `JoinSet`. Shrink uses `controls.filter_retire` directly.
    pub grow_workers: flume::Sender<usize>,
    pub status: StatusHandles,
}

impl ControlContext {
    fn snapshot(&self) -> StatusSnapshot {
        let s = &self.status;
        StatusSnapshot {
            filter_workers_alive: s.workers_alive.load(Ordering::Relaxed),
            filter_retire_pending: self.controls.filter_retire_pending(),
            download_tasks_limit: self.controls.file_limit(),
            range_concurrency_limit: self.controls.range_limit(),
            part_size_mb: self.controls.part_size_mb(),
            line_buffer_size: s.line_buffer_size,
            dl_active: s.metrics.dl_active.load(Ordering::Relaxed),
            files_in_flight: s.metrics.files_in_flight.load(Ordering::Relaxed),
            line_channel_len: s.line_channel.len(),
            line_channel_cap: s.line_channel.capacity(),
            downloaded_bytes: s.download_observer.bytes() as u64,
            match_count: s.match_count.load(Ordering::Relaxed),
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

    let result = accept_loop(&listener, ctx).await;
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
