// src/utils/signal_handler.rs
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::structured_log::{LogEntry, MemoryStats, ProgressStats, TimeStats};
use human_bytes::human_bytes;
use signal_hook::{consts::SIGUSR2, iterator::Signals};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use tracing::info;

// Shared reference to indicate if the signal handler is already running
static SIGNAL_HANDLER_RUNNING: AtomicBool = AtomicBool::new(false);

// Structure to track progress information
pub struct ProgressTracker {
    pub total_files: AtomicUsize,
    pub completed_files: AtomicUsize,
    pub total_bytes: AtomicUsize,
    pub processed_bytes: AtomicUsize,
    pub start_time: Instant,
}

impl ProgressTracker {
    pub fn new(total_files: usize, total_bytes: usize) -> Self {
        Self {
            total_files: AtomicUsize::new(total_files),
            completed_files: AtomicUsize::new(0),
            total_bytes: AtomicUsize::new(total_bytes),
            processed_bytes: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn update_total(&self, total_files: usize, total_bytes: usize) {
        self.total_files.store(total_files, Ordering::SeqCst);
        self.total_bytes.store(total_bytes, Ordering::SeqCst);
    }

    pub fn increment_processed(&self, bytes: usize) {
        self.processed_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn increment_completed(&self) {
        self.completed_files.fetch_add(1, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        self.completed_files.store(0, Ordering::SeqCst);
        self.processed_bytes.store(0, Ordering::SeqCst);
    }

    // Calculate percentage complete (files)
    pub fn percent_complete_files(&self) -> f64 {
        let total = self.total_files.load(Ordering::SeqCst);
        let completed = self.completed_files.load(Ordering::SeqCst);

        if total == 0 {
            return 0.0;
        }

        (completed as f64 / total as f64) * 100.0
    }

    // Calculate percentage complete (bytes)
    pub fn percent_complete_bytes(&self) -> f64 {
        let total = self.total_bytes.load(Ordering::SeqCst);
        let processed = self.processed_bytes.load(Ordering::SeqCst);

        if total == 0 {
            return 0.0;
        }

        (processed as f64 / total as f64) * 100.0
    }

    // Calculate processing speed (MB/s)
    pub fn processing_speed_mb_per_sec(&self) -> f64 {
        let processed = self.processed_bytes.load(Ordering::SeqCst) as f64;
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();

        if elapsed_secs < 0.001 {
            return 0.0;
        }

        // Convert bytes to megabytes
        (processed / (1024.0 * 1024.0)) / elapsed_secs
    }

    // Calculate estimated time remaining
    pub fn estimated_time_remaining(&self) -> Option<Duration> {
        let total = self.total_bytes.load(Ordering::SeqCst) as f64;
        let processed = self.processed_bytes.load(Ordering::SeqCst) as f64;
        let elapsed = self.start_time.elapsed();

        if processed < 1.0 || total == 0.0 {
            return None;
        }

        let percent_done = processed / total;
        if percent_done < 0.001 {
            return None;
        }

        let total_time_estimate = elapsed.as_secs_f64() / percent_done;
        let remaining_secs = total_time_estimate - elapsed.as_secs_f64();

        if remaining_secs <= 0.0 {
            return Some(Duration::from_secs(0));
        }

        Some(Duration::from_secs_f64(remaining_secs))
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

pub struct MemoryMonitor {
    memory_allocator: Arc<MemoryLimitedAllocator>,
    progress_tracker: Option<Arc<ProgressTracker>>,
    target_date: String,
    target_hour: String,
}

impl MemoryMonitor {
    pub fn new(
        memory_allocator: Arc<MemoryLimitedAllocator>,
        target_date: &str,
        target_hour: &str,
    ) -> Self {
        Self {
            memory_allocator,
            progress_tracker: None,
            target_date: target_date.to_string(),
            target_hour: target_hour.to_string(),
        }
    }

    pub fn with_progress(
        memory_allocator: Arc<MemoryLimitedAllocator>,
        progress_tracker: Arc<ProgressTracker>,
        target_date: &str,
        target_hour: &str,
    ) -> Self {
        Self {
            memory_allocator,
            progress_tracker: Some(progress_tracker),
            target_date: target_date.to_string(),
            target_hour: target_hour.to_string(),
        }
    }

    // Format duration to a human-readable string
    fn format_duration(duration: Duration) -> String {
        let total_secs = duration.as_secs();
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;

        if hours > 0 {
            format!("{}h {}m {}s", hours, minutes, seconds)
        } else if minutes > 0 {
            format!("{}m {}s", minutes, seconds)
        } else {
            format!("{}s", seconds)
        }
    }

    // Log memory usage statistics as a single structured JSON object
    pub fn log_memory_stats(&self) {
        let (memory_used_raw, memory_total_raw) = self.memory_allocator.stats();
        let (memory_waiters, memory_waiters_size, loan_count) =
            self.memory_allocator.waiters_count_and_size();

        let memory_percent = if memory_total_raw > 0 {
            ((memory_used_raw as f64 / memory_total_raw as f64) * 1000.0).round() / 10.0
        } else {
            0.0
        };

        let memory_stats = MemoryStats {
            loan_count,
            used: human_bytes(memory_used_raw as f64),
            used_bytes: memory_used_raw,
            total: human_bytes(memory_total_raw as f64),
            total_bytes: memory_total_raw,
            percent: memory_percent,
            waiters: memory_waiters,
            waiters_size: human_bytes(memory_waiters_size as f64),
        };

        let (progress_stats, time_stats, message) = if let Some(progress) = &self.progress_tracker {
            let completed_files = progress.completed_files.load(Ordering::SeqCst);
            let total_files = progress.total_files.load(Ordering::SeqCst);
            let processed_bytes_raw = progress.processed_bytes.load(Ordering::SeqCst);
            let total_bytes_raw = progress.total_bytes.load(Ordering::SeqCst);
            let percent_files = (progress.percent_complete_files() * 10.0).round() / 10.0;
            let percent_bytes = (progress.percent_complete_bytes() * 10.0).round() / 10.0;
            let speed = (progress.processing_speed_mb_per_sec() * 100.0).round() / 100.0;

            let progress_stats = ProgressStats {
                completed_files,
                total_files,
                percent_files,
                processed_bytes: human_bytes(processed_bytes_raw as f64),
                processed_bytes_raw,
                total_bytes: human_bytes(total_bytes_raw as f64),
                total_bytes_raw,
                percent_bytes,
                speed_mb_per_sec: speed,
            };

            let elapsed = progress.start_time.elapsed();
            let (time_stats, eta_str) = if let Some(remaining) = progress.estimated_time_remaining()
            {
                let completion_str = Self::calculate_completion_time(remaining);
                let eta = Self::format_duration(remaining);
                (
                    TimeStats {
                        elapsed: Self::format_duration(elapsed),
                        elapsed_secs: elapsed.as_secs(),
                        remaining: Some(eta.clone()),
                        remaining_secs: Some(remaining.as_secs()),
                        estimated_completion: completion_str,
                    },
                    eta,
                )
            } else {
                (
                    TimeStats {
                        elapsed: Self::format_duration(elapsed),
                        elapsed_secs: elapsed.as_secs(),
                        remaining: None,
                        remaining_secs: None,
                        estimated_completion: None,
                    },
                    "calculating...".to_string(),
                )
            };

            let message = format!(
                "Checking {}/{}: {}/{} files ({:.1}%), {}/{} processed, {:.2} MB/s, elapsed {}, ETA {}",
                self.target_date,
                self.target_hour,
                completed_files,
                total_files,
                percent_files,
                human_bytes(processed_bytes_raw as f64),
                human_bytes(total_bytes_raw as f64),
                speed,
                Self::format_duration(elapsed),
                eta_str
            );

            (Some(progress_stats), Some(time_stats), message)
        } else {
            let message = format!(
                "Checking {}/{}: memory {}/{} ({:.1}%)",
                self.target_date,
                self.target_hour,
                memory_stats.used,
                memory_stats.total,
                memory_stats.percent
            );
            (None, None, message)
        };

        let mut entry = LogEntry::info(message)
            .with_target("log_consolidator_checker_rust::utils::signal_handler")
            .with_date_hour(&self.target_date, &self.target_hour)
            .with_memory(memory_stats);

        if let Some(progress) = progress_stats {
            entry = entry.with_progress(progress);
        }
        if let Some(time) = time_stats {
            entry = entry.with_time(time);
        }

        entry.emit();
    }

    fn calculate_completion_time(remaining: Duration) -> Option<String> {
        let now = SystemTime::now();
        if let Some(completion_time) = now.checked_add(remaining) {
            match completion_time.duration_since(SystemTime::UNIX_EPOCH) {
                Ok(completion_datetime) => {
                    let datetime =
                        chrono::DateTime::from_timestamp(completion_datetime.as_secs() as i64, 0)
                            .unwrap_or_else(|| chrono::Utc::now());
                    Some(datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                }
                Err(_) => None,
            }
        } else {
            None
        }
    }

    // Setup signal handler for SIGUSR2
    pub fn setup_signal_handler(&self) -> Result<(), anyhow::Error> {
        // Only set up handler if it's not already running
        if SIGNAL_HANDLER_RUNNING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // Handler already running
            return Ok(());
        }

        let memory_allocator = Arc::clone(&self.memory_allocator);
        let progress_tracker = self.progress_tracker.clone();
        let target_date = self.target_date.clone();
        let target_hour = self.target_hour.clone();

        // Set up signals
        let mut signals = Signals::new(&[SIGUSR2])?;

        // Spawn thread to handle signals
        thread::spawn(move || {
            for sig in signals.forever() {
                match sig {
                    SIGUSR2 => {
                        let monitor = if let Some(tracker) = &progress_tracker {
                            MemoryMonitor::with_progress(
                                Arc::clone(&memory_allocator),
                                Arc::clone(tracker),
                                &target_date,
                                &target_hour,
                            )
                        } else {
                            MemoryMonitor::new(
                                Arc::clone(&memory_allocator),
                                &target_date,
                                &target_hour,
                            )
                        };
                        monitor.log_memory_stats();
                    }
                    _ => unreachable!(),
                }
            }
        });

        info!(target_date = %self.target_date, target_hour = %self.target_hour, "Signal handler set up for SIGUSR2");
        Ok(())
    }
}
