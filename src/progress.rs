//! Cross-cutting progress tracking for the download → search → export pipeline.

use crate::pipeline::{
    ChannelObserver, DownloadObserver, PipelineObserver, ReadPathMetrics, SinkObservability,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Sink-agnostic gauge: how many filter workers are *currently* executing
/// inside `sink.ingest`. Sampled instantaneously by the progress reporter
/// at each tick. High value relative to the worker count means the sink
/// is the bottleneck (codec + framing + per-sink-specific I/O); low value
/// means filter workers are spending their time on regex or upstream
/// channel receives.
///
/// Use [`IngestGuard`] in the filter worker to bump the counter
/// automatically:
///
/// ```ignore
/// let _g = IngestGuard::new(&gauge);
/// sink.ingest(prefix, line)?;
/// ```
pub type IngestGauge = Arc<AtomicUsize>;

/// RAII helper that increments an [`IngestGauge`] on construction and
/// decrements it on drop. Cheap (two `Relaxed` atomic ops); safe across
/// panics because the decrement runs on unwind.
pub struct IngestGuard<'a> {
    gauge: &'a AtomicUsize,
}

impl<'a> IngestGuard<'a> {
    pub fn new(gauge: &'a AtomicUsize) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self { gauge }
    }
}

impl Drop for IngestGuard<'_> {
    fn drop(&mut self) {
        self.gauge.fetch_sub(1, Ordering::Relaxed);
    }
}

fn rss_mb() -> usize {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok())
        .map(|pages| pages * page_size / 1_000_000)
        .unwrap_or(0)
}

// Linux-only: reads /proc/self/fd. Returns 0 on non-Linux (diagnostic only).
fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").map_or(0, |d| d.count())
}

/// Progress tracking for search operations.
///
/// Observes all pipeline stages (download, decompress channel, compress, upload)
/// and emits periodic structured-log reports with throughput and bottleneck info.
pub struct PipelineProgress {
    pub total_files: usize,
    pub files_processed: usize,
    pub total_bytes: usize,
    pub bytes_processed: usize,
    /// Shared atomic counter incremented by filter workers.
    pub match_count: Arc<AtomicUsize>,
    /// Total bytes of matching lines (for byte-level selectivity).
    pub match_bytes: Arc<AtomicUsize>,
    /// Total lines that entered the filter stage (regardless of pattern).
    pub filter_lines_in: Arc<AtomicUsize>,
    /// Total bytes that entered the filter stage (regardless of pattern).
    pub filter_bytes_in: Arc<AtomicUsize>,
    pub start_time: std::time::Instant,
    pub last_report_time: std::time::Instant,
    pub report_interval: Duration,
    pub pipeline: Option<PipelineObserver>,
    pub decompressed_ch: ChannelObserver,
    pub download_observer: DownloadObserver,
    /// Snapshot of downloaded bytes at last report (for download_mbps)
    pub prev_downloaded_bytes: usize,
    /// Snapshot of compressed_bytes_sent at last report (for upload_mbps)
    pub prev_uploaded_bytes: usize,
    /// Snapshot of filter_bytes_in at last report (for filter_in_mbps)
    pub prev_filter_bytes_in: usize,
    /// Number of filter workers still alive (decremented on worker exit).
    pub workers_alive: Arc<AtomicUsize>,
    /// Total filter workers spawned at run start. Used as the denominator
    /// for the "is the sink busy?" heuristic.
    pub total_workers: usize,
    /// Gauge of workers currently inside `sink.ingest`. Sampled
    /// instantaneously at report time. See [`IngestGauge`].
    pub workers_in_ingest: IngestGauge,
    /// Per-sink internal-state observability. `default()` for sinks
    /// without internal channels (file, void).
    pub sink_obs: SinkObservability,
    /// `sink.type_name()` snapshot, used by the classifier to pick the
    /// right per-sink drill-down label (`sink_s3_*`, `sink_file`, ...).
    pub sink_kind: &'static str,
    /// Read-path gauges (chunked download + B1/B2 buffering). Read each tick;
    /// drives the `download`/`chunk_reassembly`/`decompress` split.
    pub read_metrics: Arc<ReadPathMetrics>,
}

impl PipelineProgress {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        total_files: usize,
        total_bytes: usize,
        report_interval: Duration,
        pipeline: Option<PipelineObserver>,
        decompressed_ch: ChannelObserver,
        download_observer: DownloadObserver,
        match_count: Arc<AtomicUsize>,
        match_bytes: Arc<AtomicUsize>,
        filter_lines_in: Arc<AtomicUsize>,
        filter_bytes_in: Arc<AtomicUsize>,
        workers_alive: Arc<AtomicUsize>,
        total_workers: usize,
        workers_in_ingest: IngestGauge,
        sink_obs: SinkObservability,
        sink_kind: &'static str,
        read_metrics: Arc<ReadPathMetrics>,
    ) -> Self {
        let now = std::time::Instant::now();
        Self {
            total_files,
            files_processed: 0,
            total_bytes,
            bytes_processed: 0,
            match_count,
            match_bytes,
            filter_lines_in,
            filter_bytes_in,
            start_time: now,
            last_report_time: now,
            report_interval,
            pipeline,
            decompressed_ch,
            download_observer,
            prev_downloaded_bytes: 0,
            prev_uploaded_bytes: 0,
            prev_filter_bytes_in: 0,
            workers_alive,
            total_workers,
            workers_in_ingest,
            sink_obs,
            sink_kind,
            read_metrics,
        }
    }

    pub fn update(&mut self, bytes: usize) {
        self.files_processed += 1;
        self.bytes_processed += bytes;
    }

    pub fn should_report(&self) -> bool {
        self.last_report_time.elapsed() > self.report_interval
    }

    pub fn report(&mut self) {
        let dl_now = self.download_observer.bytes();
        let pct = ((dl_now * 100) / self.total_bytes.max(1)).min(100);
        let interval_s = self.last_report_time.elapsed().as_secs_f64();
        let download_delta = dl_now - self.prev_downloaded_bytes;
        let download_mbps = if interval_s > 0.0 {
            download_delta as f64 / 1_000_000.0 / interval_s
        } else {
            0.0
        };

        let matches_now = self.match_count.load(Ordering::Relaxed);
        let match_bytes_now = self.match_bytes.load(Ordering::Relaxed);
        let filter_lines_now = self.filter_lines_in.load(Ordering::Relaxed);
        let filter_bytes_now = self.filter_bytes_in.load(Ordering::Relaxed);
        let filter_in_delta = filter_bytes_now.saturating_sub(self.prev_filter_bytes_in);
        let filter_in_mbps = if interval_s > 0.0 {
            filter_in_delta as f64 / 1_000_000.0 / interval_s
        } else {
            0.0
        };
        let matched_ratio_lines = matches_now as f64 / filter_lines_now.max(1) as f64;
        let matched_ratio_bytes = match_bytes_now as f64 / filter_bytes_now.max(1) as f64;

        let dc_cap = self.decompressed_ch.capacity().max(1);
        let dc_len = self.decompressed_ch.len();
        let dc_pct = dc_len * 100 / dc_cap;

        let in_ingest = self.workers_in_ingest.load(Ordering::Relaxed);
        let sink_inflight_bytes = self
            .sink_obs
            .inflight_bytes
            .as_ref()
            .map(|a| a.load(Ordering::Relaxed));
        let sink_active_uploads = self
            .sink_obs
            .active_uploads
            .as_ref()
            .map(|a| a.load(Ordering::Relaxed));

        // Read-path gauges: B1 (pool) → B2 (decode-input) → B3 (line channel).
        let rm = &self.read_metrics;
        let dl_active = rm.dl_active.load(Ordering::Relaxed);
        let files_in_flight = rm.files_in_flight.load(Ordering::Relaxed);
        let chunks_remaining = rm.chunks_remaining.load(Ordering::Relaxed);
        let b1_held = rm.b1_held_bytes.load(Ordering::Relaxed);
        let b2_used = rm.b2_used_bytes.load(Ordering::Relaxed);
        let b2_pct = (b2_used * 100)
            .checked_div(rm.b2_capacity)
            .unwrap_or(0)
            .min(100);
        let reassembly_blocked = rm.reassembly_blocked.load(Ordering::Relaxed);
        let decoders_input_wait = rm.decoders_input_wait.load(Ordering::Relaxed);
        let (pool_used_mb, pool_total_mb, pool_peak_mb, pool_waiters) = match &rm.pool {
            Some(p) => {
                let s = p.stats();
                (
                    s.used / 1_000_000,
                    s.total / 1_000_000,
                    s.peak / 1_000_000,
                    s.waiters,
                )
            }
            None => (0, 0, 0, 0),
        };

        if let Some(ref pipe) = self.pipeline {
            let uploaded_now = pipe.compressed_bytes_sent();
            let upload_delta = uploaded_now - self.prev_uploaded_bytes;
            let upload_mbps = if interval_s > 0.0 {
                upload_delta as f64 / 1_000_000.0 / interval_s
            } else {
                0.0
            };

            let batch_cap = pipe.batch_capacity().max(1);
            let line_cap = pipe.line_capacity().max(1);
            let batch_pct = pipe.batch_len() * 100 / batch_cap;
            let line_pct = pipe.line_len() * 100 / line_cap;

            let bottleneck = classify_bottleneck_http(
                batch_pct,
                line_pct,
                dc_pct,
                in_ingest,
                self.total_workers,
            );

            let throttle_mbps = pipe.throttle_rate_mbps();

            info!(
                files_done = self.files_processed,
                files_total = self.total_files,
                pct = pct,
                input_mb_done = dl_now / 1_000_000,
                input_mb_total = self.total_bytes / 1_000_000,
                download_mbps = format_args!("{download_mbps:.1}"),
                matches = matches_now,
                match_mb = match_bytes_now / 1_000_000,
                filter_lines_in = filter_lines_now,
                filter_in_mb = filter_bytes_now / 1_000_000,
                filter_in_mbps = format_args!("{filter_in_mbps:.1}"),
                matched_ratio_lines = format_args!("{matched_ratio_lines:.4}"),
                matched_ratio_bytes = format_args!("{matched_ratio_bytes:.4}"),
                dc_ch = format_args!("{dc_len}/{dc_cap}"),
                line_ch_len = pipe.line_len(),
                line_ch_cap = pipe.line_capacity(),
                batch_ch_len = pipe.batch_len(),
                batch_ch_cap = pipe.batch_capacity(),
                uploaded_mb = uploaded_now / 1_000_000,
                upload_mbps = format_args!("{upload_mbps:.1}"),
                throttle_mbps = throttle_mbps.map(|r| format!("{r:.1}")),
                batches = pipe.batches_uploaded(),
                avg_upload_ms = format_args!("{:.1}", pipe.avg_upload_ms()),
                workers_alive = self.workers_alive.load(Ordering::Relaxed),
                workers_in_ingest = in_ingest,
                dl_active = dl_active,
                files_in_flight = files_in_flight,
                chunks_remaining = chunks_remaining,
                b1_held_mb = b1_held / 1_000_000,
                b2_used_mb = b2_used / 1_000_000,
                b2_pct = b2_pct,
                decoders_input_wait = decoders_input_wait,
                reassembly_blocked = reassembly_blocked,
                pool_used_mb = pool_used_mb,
                pool_total_mb = pool_total_mb,
                pool_peak_mb = pool_peak_mb,
                pool_waiters = pool_waiters,
                open_fds = open_fds(),
                rss_mb = rss_mb(),
                bottleneck = bottleneck,
                elapsed_s = self.start_time.elapsed().as_secs_f32(),
                "Search progress"
            );

            self.prev_uploaded_bytes = uploaded_now;
        } else {
            let bottleneck = classify_bottleneck_non_http(
                dc_pct,
                in_ingest,
                self.total_workers,
                sink_inflight_bytes,
                sink_active_uploads,
                self.sink_kind,
                decoders_input_wait,
                files_in_flight,
                reassembly_blocked,
            );

            info!(
                files_done = self.files_processed,
                files_total = self.total_files,
                pct = pct,
                input_mb_done = dl_now / 1_000_000,
                input_mb_total = self.total_bytes / 1_000_000,
                download_mbps = format_args!("{download_mbps:.1}"),
                matches = matches_now,
                match_mb = match_bytes_now / 1_000_000,
                filter_lines_in = filter_lines_now,
                filter_in_mb = filter_bytes_now / 1_000_000,
                filter_in_mbps = format_args!("{filter_in_mbps:.1}"),
                matched_ratio_lines = format_args!("{matched_ratio_lines:.4}"),
                matched_ratio_bytes = format_args!("{matched_ratio_bytes:.4}"),
                dc_ch = format_args!("{dc_len}/{dc_cap}"),
                workers_alive = self.workers_alive.load(Ordering::Relaxed),
                workers_in_ingest = in_ingest,
                dl_active = dl_active,
                files_in_flight = files_in_flight,
                chunks_remaining = chunks_remaining,
                b1_held_mb = b1_held / 1_000_000,
                b2_used_mb = b2_used / 1_000_000,
                b2_pct = b2_pct,
                decoders_input_wait = decoders_input_wait,
                reassembly_blocked = reassembly_blocked,
                pool_used_mb = pool_used_mb,
                pool_total_mb = pool_total_mb,
                pool_peak_mb = pool_peak_mb,
                pool_waiters = pool_waiters,
                sink_inflight_bytes = sink_inflight_bytes,
                sink_active_uploads = sink_active_uploads,
                open_fds = open_fds(),
                rss_mb = rss_mb(),
                bottleneck = bottleneck,
                elapsed_s = self.start_time.elapsed().as_secs_f32(),
                "Search progress"
            );
        }

        self.prev_downloaded_bytes = dl_now;
        self.prev_filter_bytes_in = filter_bytes_now;
        self.last_report_time = std::time::Instant::now();
    }
}

/// Classify the dominant pipeline bottleneck for the HTTP-output path.
///
/// Inputs are channel fill percentages (0–100) for the three HTTP-mode
/// channels plus the cross-sink workers-in-ingest gauge. The priority
/// order matches production wiring: a full *downstream* channel means
/// upstream stages can't drain, so we report the most-downstream-saturated
/// stage first.
///
/// `in_ingest` and `total_workers` disambiguate the historical `"filter"`
/// label into `filter` vs `sink_http` — see [`classify_bottleneck_non_http`]
/// for the same logic on file/s3 sinks.
fn classify_bottleneck_http(
    batch_pct: usize,
    line_pct: usize,
    dc_pct: usize,
    in_ingest: usize,
    total_workers: usize,
) -> &'static str {
    if batch_pct > 80 {
        "upload"
    } else if line_pct > 80 {
        "compress"
    } else if dc_pct > 80 {
        if sink_is_busy(in_ingest, total_workers) {
            "sink_http"
        } else {
            "filter"
        }
    } else {
        "download"
    }
}

/// Classify the dominant bottleneck for sinks that don't have the HTTP
/// writer's compress + upload channel decomposition (file, s3, void).
///
/// Reading key:
/// - `download` — decompressed channel is mostly empty; the download or
///   decompress stage isn't keeping up.
/// - `filter` — decompressed channel is full **and** few workers are
///   inside `sink.ingest`. Workers are spending their time on regex /
///   channel receive — filter-stage CPU is the lid.
/// - `sink_s3_codec` — sink is the lid (most workers inside `sink.ingest`),
///   and the S3 sink's internal mpsc is **not** backed up — the cost is
///   on the producer side (codec compression).
/// - `sink_s3_network` — same as above but the S3 mpsc **is** backed up:
///   workers are blocked in `ChannelWriter::blocking_send` because TM /
///   S3 isn't draining parts fast enough.
/// - `sink_file` — sink is the lid for the file sink. No deeper drill-down
///   at this level; reach for `iostat`/`vmstat` to see whether codec or
///   the OS write path is at fault.
/// - `sink_void` — should never realistically fire (void's `ingest` is a
///   counter bump); included for completeness so the classifier returns
///   a meaningful label rather than falling through.
///
/// When the decompressed-line channel (B3) is **not** full, upstream can't fill
/// it. The B2/reassembly signals split that formerly-catch-all `download` case:
/// - `decompress` — decoders have input (few are waiting on B2) yet B3 still
///   isn't full: the decoder CPU is the lid.
/// - `chunk_reassembly` — decoders are starved and a file is head-of-line
///   blocked (a later chunk is buffered but the next in-order one isn't).
/// - `download` — decoders are starved and nothing's reassembly-blocked: the
///   network isn't delivering.
#[allow(clippy::too_many_arguments)]
fn classify_bottleneck_non_http(
    dc_pct: usize,
    in_ingest: usize,
    total_workers: usize,
    sink_inflight_bytes: Option<u64>,
    sink_active_uploads: Option<usize>,
    sink_kind: &str,
    decoders_input_wait: usize,
    files_in_flight: usize,
    reassembly_blocked: usize,
) -> &'static str {
    if dc_pct <= 80 {
        // B3 has room — the lid is upstream of the filter stage.
        if decoders_decompress_bound(decoders_input_wait, files_in_flight) {
            return "decompress";
        }
        if reassembly_blocked > 0 {
            return "chunk_reassembly";
        }
        return "download";
    }
    if !sink_is_busy(in_ingest, total_workers) {
        return "filter";
    }
    match sink_kind {
        "s3" => {
            if s3_mpsc_is_backed_up(sink_inflight_bytes, sink_active_uploads) {
                "sink_s3_network"
            } else {
                "sink_s3_codec"
            }
        }
        "file" => "sink_file",
        "void" => "sink_void",
        // Unknown sink kind — surface as a generic sink_busy label so the
        // operator notices and we don't lie about which sink it is.
        _ => "sink_busy",
    }
}

/// `true` when at least half the filter workers are inside `sink.ingest`.
/// That's the threshold below which the sink can't realistically be the
/// dominant cost — over half the workers are doing other work.
fn sink_is_busy(in_ingest: usize, total_workers: usize) -> bool {
    in_ingest * 2 >= total_workers.max(1)
}

/// `true` when decoders have input to chew on (fewer than half are blocked
/// waiting for B2) — so if the line channel still isn't full, the decoder CPU
/// is the lid. Requires at least one live decoder. Mirrors [`sink_is_busy`]'s
/// majority threshold on the input-wait gauge.
fn decoders_decompress_bound(decoders_input_wait: usize, files_in_flight: usize) -> bool {
    files_in_flight > 0 && decoders_input_wait * 2 < files_in_flight
}

/// Heuristic: the S3 sink's mpsc channels are considered "backed up" when
/// resident inflight bytes exceed an envelope sized by the per-upload
/// channel capacity (≈ 256 KB, matching `CHANNEL_CAPACITY=2` × ~128 KB
/// codec blocks) across the currently-open uploads. Empty / unknown
/// inflight reads conservatively to `false` — better to say "codec" than
/// to falsely accuse the network.
fn s3_mpsc_is_backed_up(inflight: Option<u64>, active: Option<usize>) -> bool {
    let (Some(inflight), Some(active)) = (inflight, active) else {
        return false;
    };
    if active == 0 {
        return false;
    }
    // Per-upload threshold sized to half the channel envelope. We're
    // intentionally permissive: the goal is to flip the label when
    // backpressure is *sustained*, not on a single transient chunk.
    const PER_UPLOAD_BACKED_UP_BYTES: u64 = 128 * 1024;
    inflight >= (active as u64) * PER_UPLOAD_BACKED_UP_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    // HTTP path: most-downstream-saturated channel wins, but the
    // historical `"filter"` label is now split via the workers-in-ingest
    // gauge into `filter` vs `sink_http`.

    #[test]
    fn bottleneck_http_priority_order_when_sink_idle() {
        // 0 workers in ingest → the sink is not the lid. The dc-saturated
        // case falls through to `filter`.
        assert_eq!(classify_bottleneck_http(90, 90, 90, 0, 8), "upload");
        assert_eq!(classify_bottleneck_http(0, 90, 90, 0, 8), "compress");
        assert_eq!(classify_bottleneck_http(0, 0, 90, 0, 8), "filter");
        assert_eq!(classify_bottleneck_http(0, 0, 0, 0, 8), "download");
    }

    #[test]
    fn bottleneck_http_dc_full_with_busy_sink_reports_sink_http() {
        // dc full + half the workers stuck in sink.ingest → sink is the
        // lid, even on the HTTP path (the line/batch channels happen to
        // be drained right now but the sink is still where time goes).
        assert_eq!(classify_bottleneck_http(0, 0, 90, 4, 8), "sink_http");
    }

    #[test]
    fn bottleneck_http_threshold_is_strict_gt_80() {
        // Exactly 80 should NOT trigger; 81 should.
        assert_eq!(classify_bottleneck_http(80, 80, 80, 0, 8), "download");
        assert_eq!(classify_bottleneck_http(81, 0, 0, 0, 8), "upload");
        assert_eq!(classify_bottleneck_http(0, 81, 0, 0, 8), "compress");
        assert_eq!(classify_bottleneck_http(0, 0, 81, 0, 8), "filter");
    }

    // Non-HTTP path (file, s3, void): single dc channel; differentiation
    // comes from the workers-in-ingest gauge + per-sink internal signals.

    #[test]
    fn non_http_dc_empty_reports_download() {
        for kind in ["file", "s3", "void"] {
            assert_eq!(
                classify_bottleneck_non_http(0, 0, 8, None, None, kind, 0, 0, 0),
                "download",
                "kind={kind}"
            );
            assert_eq!(
                classify_bottleneck_non_http(80, 8, 8, None, None, kind, 0, 0, 0),
                "download",
                "kind={kind}"
            );
        }
    }

    #[test]
    fn non_http_dc_full_idle_sink_reports_filter() {
        // dc saturated but no workers in ingest → filter-side CPU is the
        // lid (regex, channel-recv, work that doesn't touch the sink).
        for kind in ["file", "s3", "void"] {
            assert_eq!(
                classify_bottleneck_non_http(90, 0, 8, None, None, kind, 0, 0, 0),
                "filter",
                "kind={kind}"
            );
            assert_eq!(
                classify_bottleneck_non_http(90, 3, 8, None, None, kind, 0, 0, 0),
                "filter",
                "kind={kind} below-half threshold"
            );
        }
    }

    #[test]
    fn non_http_dc_full_busy_sink_picks_per_sink_label() {
        // half-or-more workers stuck in sink.ingest → the sink is the lid.
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, None, None, "file", 0, 0, 0),
            "sink_file"
        );
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, None, None, "void", 0, 0, 0),
            "sink_void"
        );
        // Unknown sink — fall back to a generic, honest label.
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, None, None, "exotic", 0, 0, 0),
            "sink_busy"
        );
    }

    #[test]
    fn non_http_s3_distinguishes_codec_vs_network() {
        // sink busy + S3 mpsc nearly empty → producer (codec) is the lid.
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, Some(0), Some(4), "s3", 0, 0, 0),
            "sink_s3_codec"
        );
        // sink busy + S3 mpsc backed up → consumer (TM / network) is the lid.
        // 4 active × 128 KB threshold = 512 KB, so 1 MB inflight crosses it.
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, Some(1024 * 1024), Some(4), "s3", 0, 0, 0),
            "sink_s3_network"
        );
    }

    #[test]
    fn non_http_s3_with_missing_inflight_signal_falls_back_to_codec() {
        // If the S3 sink's observability isn't wired (shouldn't happen in
        // production but worth being explicit), we prefer to say `codec`
        // rather than falsely blame the network.
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, None, Some(4), "s3", 0, 0, 0),
            "sink_s3_codec"
        );
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, Some(1024 * 1024), None, "s3", 0, 0, 0),
            "sink_s3_codec"
        );
        // Zero active uploads with high inflight is nonsensical, treat as codec.
        assert_eq!(
            classify_bottleneck_non_http(90, 4, 8, Some(999_999), Some(0), "s3", 0, 0, 0),
            "sink_s3_codec"
        );
    }

    // B3-has-room split: decompress vs chunk_reassembly vs download.

    #[test]
    fn non_http_decompress_when_decoders_busy_and_b3_has_room() {
        // dc_pct ≤ 80 (B3 has room) + decoders mostly NOT waiting for input
        // (1 of 8) → the decoder CPU is the lid.
        assert_eq!(
            classify_bottleneck_non_http(50, 0, 8, None, None, "file", 1, 8, 0),
            "decompress"
        );
    }

    #[test]
    fn non_http_chunk_reassembly_when_starved_and_blocked() {
        // Decoders starved (all 8 waiting) AND a file is head-of-line blocked.
        assert_eq!(
            classify_bottleneck_non_http(10, 0, 8, None, None, "s3", 8, 8, 1),
            "chunk_reassembly"
        );
    }

    #[test]
    fn non_http_download_when_starved_not_blocked() {
        // Starved, nothing reassembly-blocked → network is the lid.
        assert_eq!(
            classify_bottleneck_non_http(10, 0, 8, None, None, "s3", 8, 8, 0),
            "download"
        );
        // Startup: no live decoders yet → download, not decompress.
        assert_eq!(
            classify_bottleneck_non_http(0, 0, 8, None, None, "file", 0, 0, 0),
            "download"
        );
    }

    #[test]
    fn decoders_decompress_bound_threshold() {
        assert!(!decoders_decompress_bound(0, 0)); // no decoders
        assert!(decoders_decompress_bound(0, 4)); // none waiting → busy
        assert!(decoders_decompress_bound(1, 4)); // 1 of 4 waiting → busy
        assert!(!decoders_decompress_bound(2, 4)); // half waiting → not bound
        assert!(!decoders_decompress_bound(4, 4)); // all waiting → starved
    }

    #[test]
    fn ingest_guard_increments_then_decrements() {
        let g = Arc::new(AtomicUsize::new(0));
        {
            let _guard = IngestGuard::new(&g);
            assert_eq!(g.load(Ordering::Relaxed), 1);
            {
                let _nested = IngestGuard::new(&g);
                assert_eq!(g.load(Ordering::Relaxed), 2);
            }
            assert_eq!(g.load(Ordering::Relaxed), 1);
        }
        assert_eq!(g.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn sink_is_busy_threshold_is_half_or_more() {
        assert!(!sink_is_busy(0, 8));
        assert!(!sink_is_busy(3, 8));
        assert!(sink_is_busy(4, 8));
        assert!(sink_is_busy(8, 8));
        // Edge: zero workers (e.g. all already exited) — sink "busyness"
        // is undefined; treat conservatively as not-busy.
        assert!(!sink_is_busy(0, 0));
    }
}
