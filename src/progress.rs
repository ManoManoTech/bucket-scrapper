//! Cross-cutting progress tracking for the download → search → export pipeline.

use crate::pipeline::{ChannelObserver, DownloadObserver, PipelineObserver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

fn rss_mb() -> usize {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok())
        .map(|pages| pages * page_size / 1_000_000)
        .unwrap_or(0)
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
}

impl PipelineProgress {
    pub fn new(
        total_files: usize,
        total_bytes: usize,
        report_interval: Duration,
        pipeline: Option<PipelineObserver>,
        decompressed_ch: ChannelObserver,
        download_observer: DownloadObserver,
        match_count: Arc<AtomicUsize>,
    ) -> Self {
        let now = std::time::Instant::now();
        Self {
            total_files,
            files_processed: 0,
            total_bytes,
            bytes_processed: 0,
            match_count,
            start_time: now,
            last_report_time: now,
            report_interval,
            pipeline,
            decompressed_ch,
            download_observer,
            prev_downloaded_bytes: 0,
            prev_uploaded_bytes: 0,
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
        let pct = (self.bytes_processed * 100) / self.total_bytes.max(1);
        let interval_s = self.last_report_time.elapsed().as_secs_f64();

        let dl_now = self.download_observer.bytes();
        let download_delta = dl_now - self.prev_downloaded_bytes;
        let download_mbps = if interval_s > 0.0 {
            download_delta as f64 / 1_000_000.0 / interval_s
        } else {
            0.0
        };

        let dc_cap = self.decompressed_ch.capacity().max(1);
        let dc_len = self.decompressed_ch.len();
        let dc_pct = dc_len * 100 / dc_cap;

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

            let bottleneck = if batch_pct > 80 {
                "upload"
            } else if line_pct > 80 {
                "compress"
            } else if dc_pct > 80 {
                "filter"
            } else {
                "download"
            };

            let throttle_mbps = pipe.throttle_rate_mbps();

            info!(
                files_done = self.files_processed,
                files_total = self.total_files,
                pct = pct,
                input_mb_done = self.bytes_processed / 1_000_000,
                input_mb_total = self.total_bytes / 1_000_000,
                download_mbps = format_args!("{download_mbps:.1}"),
                matches = self.match_count.load(Ordering::Relaxed),
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
                rss_mb = rss_mb(),
                bottleneck = bottleneck,
                elapsed_s = self.start_time.elapsed().as_secs_f32(),
                "Search progress"
            );

            self.prev_uploaded_bytes = uploaded_now;
        } else {
            let bottleneck = if dc_pct > 80 { "filter" } else { "download" };

            info!(
                files_done = self.files_processed,
                files_total = self.total_files,
                pct = pct,
                input_mb_done = self.bytes_processed / 1_000_000,
                input_mb_total = self.total_bytes / 1_000_000,
                download_mbps = format_args!("{download_mbps:.1}"),
                matches = self.match_count.load(Ordering::Relaxed),
                dc_ch = format_args!("{dc_len}/{dc_cap}"),
                rss_mb = rss_mb(),
                bottleneck = bottleneck,
                elapsed_s = self.start_time.elapsed().as_secs_f32(),
                "Search progress"
            );
        }

        self.prev_downloaded_bytes = dl_now;
        self.last_report_time = std::time::Instant::now();
    }
}
