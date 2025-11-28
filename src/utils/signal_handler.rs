// src/utils/signal_handler.rs
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use human_bytes::human_bytes;
use signal_hook::{consts::SIGUSR2, iterator::Signals};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use tracing::{info, warn};

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
}

impl MemoryMonitor {
    pub fn new(memory_allocator: Arc<MemoryLimitedAllocator>) -> Self {
        Self {
            memory_allocator,
            progress_tracker: None,
        }
    }

    pub fn with_progress(
        memory_allocator: Arc<MemoryLimitedAllocator>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Self {
        Self {
            memory_allocator,
            progress_tracker: Some(progress_tracker),
        }
    }

    // Format memory usage as human-readable strings
    fn format_memory_stats(&self) -> (String, String) {
        let (memory_used, memory_total) = self.memory_allocator.stats();

        // Format with human-readable values
        (
            human_bytes(memory_used as f64),
            human_bytes(memory_total as f64),
        )
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

    // Log memory usage statistics in human-readable format
    pub fn log_memory_stats(&self) {
        let (memory_used, memory_total) = self.format_memory_stats();

        // Calculate percentages
        let (memory_used_raw, memory_total_raw) = self.memory_allocator.stats();

        let memory_percent = if memory_total_raw > 0 {
            (memory_used_raw as f64 / memory_total_raw as f64) * 100.0
        } else {
            0.0
        };

        // Get waiters count
        let (memory_waiters, memory_waiters_size, loan_count) =
            self.memory_allocator.waiters_count_and_size();
        let waiters_size_human = human_bytes(memory_waiters_size as f64);

        info!("======= MEMORY USAGE STATS =======");
        info!(
            "Memory pool: {} loans, {}/{} ({:.1}%) with {} waiters (waiting for {} of memory)",
            loan_count,
            memory_used,
            memory_total,
            memory_percent,
            memory_waiters,
            waiters_size_human
        );

        // Add progress information if available
        if let Some(progress) = &self.progress_tracker {
            let percent_files = progress.percent_complete_files();
            let percent_bytes = progress.percent_complete_bytes();
            let speed = progress.processing_speed_mb_per_sec();

            info!("======= PROGRESS STATS =======");
            info!(
                "Files processed: {}/{} ({:.1}%)",
                progress.completed_files.load(Ordering::SeqCst),
                progress.total_files.load(Ordering::SeqCst),
                percent_files
            );
            info!(
                "Bytes processed: {}/{} ({:.1}%)",
                human_bytes(progress.processed_bytes.load(Ordering::SeqCst) as f64),
                human_bytes(progress.total_bytes.load(Ordering::SeqCst) as f64),
                percent_bytes
            );
            info!("Processing speed: {:.2} MB/s", speed);

            // Show elapsed time
            let elapsed = progress.start_time.elapsed();
            info!("Elapsed time: {}", Self::format_duration(elapsed));

            // Show estimated time remaining if available
            if let Some(remaining) = progress.estimated_time_remaining() {
                info!(
                    "Estimated time remaining: {}",
                    Self::format_duration(remaining)
                );

                // Calculate estimated completion time
                let now = SystemTime::now();
                // Fixed: checked_add returns Option not Result
                if let Some(completion_time) = now.checked_add(remaining) {
                    match completion_time.duration_since(SystemTime::UNIX_EPOCH) {
                        Ok(completion_datetime) => {
                            let datetime = chrono::DateTime::from_timestamp(
                                completion_datetime.as_secs() as i64,
                                0,
                            )
                            .unwrap_or_else(|| chrono::Utc::now());

                            info!(
                                "Estimated completion time: {}",
                                datetime.format("%Y-%m-%d %H:%M:%S %Z")
                            );
                        }
                        Err(_) => {
                            // This shouldn't happen with reasonable timestamps
                            warn!("Could not calculate completion time");
                        }
                    }
                }
            }
        }

        info!("=================================");
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
                            )
                        } else {
                            MemoryMonitor::new(Arc::clone(&memory_allocator))
                        };
                        monitor.log_memory_stats();
                    }
                    _ => unreachable!(),
                }
            }
        });

        info!("Signal handler set up for SIGUSR2 to report memory allocator status and progress");
        Ok(())
    }
}
