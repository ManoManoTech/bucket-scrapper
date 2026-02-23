// src/progress.rs
//! Cross-cutting progress tracking for the download → search → export pipeline.

use crate::pipeline::PipelineObserver;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

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
/// [`PipelineProgress`] observe decompressed-channel fill levels without
/// depending on the private `DownloadedObject` type.
pub struct ChannelObserver {
    len: Box<dyn Fn() -> usize + Send + Sync>,
    cap: usize,
}

impl ChannelObserver {
    /// Create an observer from any `flume::Receiver<T>`.
    ///
    /// Uses the receiver side so that holding this observer does not prevent
    /// channel closure (which requires all *senders* to be dropped).
    pub fn from_receiver<T: Send + 'static>(rx: &flume::Receiver<T>) -> Self {
        let rx = rx.clone();
        let cap = rx.capacity().unwrap_or(0);
        Self {
            len: Box::new(move || rx.len()),
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

fn rss_mb() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok())
        .map(|pages| pages * 4096 / 1_000_000)
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
        let download_mbps = if interval_s > 0.0 { download_delta as f64 / 1_000_000.0 / interval_s } else { 0.0 };

        let dc_cap = self.decompressed_ch.capacity().max(1);
        let dc_len = self.decompressed_ch.len();
        let dc_pct = dc_len * 100 / dc_cap;

        if let Some(ref pipe) = self.pipeline {
            let uploaded_now = pipe.compressed_bytes_sent();
            let upload_delta = uploaded_now - self.prev_uploaded_bytes;
            let upload_mbps = if interval_s > 0.0 { upload_delta as f64 / 1_000_000.0 / interval_s } else { 0.0 };

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

            if let Some(throttle_mbps) = pipe.throttle_rate_mbps() {
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
                    throttle_mbps = format_args!("{throttle_mbps:.1}"),
                    batches = pipe.batches_uploaded(),
                    avg_upload_ms = format_args!("{:.1}", pipe.avg_upload_ms()),
                    rss_mb = rss_mb(),
                    bottleneck = bottleneck,
                    elapsed_s = self.start_time.elapsed().as_secs_f32(),
                    "Search progress"
                );
            } else {
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
                    batches = pipe.batches_uploaded(),
                    avg_upload_ms = format_args!("{:.1}", pipe.avg_upload_ms()),
                    rss_mb = rss_mb(),
                    bottleneck = bottleneck,
                    elapsed_s = self.start_time.elapsed().as_secs_f32(),
                    "Search progress"
                );
            }

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
