// src/utils/structured_log.rs
// Central structured logging schema for consistent JSON output

use human_bytes::human_bytes;
use serde::Serialize;

/// Log level enum
#[derive(Serialize, Clone, Copy, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    #[default]
    Info,
    Warn,
    Error,
    Debug,
}

/// Check result enum
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum CheckResult {
    Passed,
    Failed,
}

/// Bucket role enum
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum BucketRole {
    Archived,
    Consolidated,
}

/// Match status enum for character comparison
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum MatchStatus {
    Match,
    Mismatch,
}

/// Main log entry - single schema for all structured logs
#[derive(Serialize, Default)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: LogLevel,
    pub message: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<&'static str>,

    // Target date/hour for the check
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_hour: Option<String>,

    // Check result (only present in final result log)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_result: Option<CheckResult>,

    // Single bucket info (for per-bucket operations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<BucketInfo>,

    // Multiple buckets info (for check start)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<BucketsConfig>,

    // File stats for a bucket
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_stats: Option<FileStats>,

    // Character count stats (for final comparison)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub character_stats: Option<CharacterStats>,

    // Memory stats (for progress logs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryStats>,

    // Progress stats (for progress logs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProgressStats>,

    // Time stats (for progress logs)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<TimeStats>,
}

/// Bucket information - consistent across all logs
#[derive(Serialize, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub role: BucketRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_patterns: Option<Vec<String>>,
}

/// Configuration of all buckets involved in a check
#[derive(Serialize)]
pub struct BucketsConfig {
    pub archived: Vec<BucketInfo>,
    pub consolidated: BucketInfo,
    pub archived_count: usize,
    pub total_count: usize,
}

/// File statistics for a bucket
#[derive(Serialize)]
pub struct FileStats {
    pub file_count: usize,
    pub total_size_bytes: usize,
    pub total_size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

/// Character count statistics for comparison
#[derive(Serialize)]
pub struct CharacterStats {
    pub archived_total: u64,
    pub archived_total_human: String,
    pub consolidated_total: u64,
    pub consolidated_total_human: String,
    pub match_status: MatchStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difference_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difference_total: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difference_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_bucket: Option<Vec<BucketCharacterStats>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<i64>,
}

/// Per-bucket character statistics
#[derive(Serialize)]
pub struct BucketCharacterStats {
    pub name: String,
    pub role: BucketRole,
    pub total_chars: u64,
    pub total_chars_human: String,
}

/// Memory pool statistics
#[derive(Serialize)]
pub struct MemoryStats {
    pub loan_count: usize,
    pub used: String,
    pub used_bytes: usize,
    pub total: String,
    pub total_bytes: usize,
    pub percent: f64,
    pub waiters: usize,
    pub waiters_size: String,
}

/// Processing progress statistics
#[derive(Serialize)]
pub struct ProgressStats {
    pub completed_files: usize,
    pub total_files: usize,
    pub percent_files: f64,
    pub processed_bytes: String,
    pub processed_bytes_raw: usize,
    pub total_bytes: String,
    pub total_bytes_raw: usize,
    pub percent_bytes: f64,
    pub speed_mb_per_sec: f64,
}

/// Time statistics
#[derive(Serialize)]
pub struct TimeStats {
    pub elapsed: String,
    pub elapsed_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_completion: Option<String>,
}

impl LogEntry {
    pub fn new(level: LogLevel, message: impl Into<String>) -> Self {
        Self {
            timestamp: chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string(),
            level,
            message: message.into(),
            ..Default::default()
        }
    }

    pub fn info(message: impl Into<String>) -> Self {
        Self::new(LogLevel::Info, message)
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::new(LogLevel::Error, message)
    }

    pub fn warn(message: impl Into<String>) -> Self {
        Self::new(LogLevel::Warn, message)
    }

    pub fn with_target(mut self, target: &'static str) -> Self {
        self.target = Some(target);
        self
    }

    pub fn with_date_hour(mut self, date: &str, hour: &str) -> Self {
        self.target_date = Some(date.to_string());
        self.target_hour = Some(hour.to_string());
        self
    }

    pub fn with_check_result(mut self, result: CheckResult) -> Self {
        self.check_result = Some(result);
        self
    }

    pub fn with_bucket(mut self, bucket: BucketInfo) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub fn with_buckets(mut self, buckets: BucketsConfig) -> Self {
        self.buckets = Some(buckets);
        self
    }

    pub fn with_file_stats(mut self, stats: FileStats) -> Self {
        self.file_stats = Some(stats);
        self
    }

    pub fn with_character_stats(mut self, stats: CharacterStats) -> Self {
        self.character_stats = Some(stats);
        self
    }

    pub fn with_memory(mut self, memory: MemoryStats) -> Self {
        self.memory = Some(memory);
        self
    }

    pub fn with_progress(mut self, progress: ProgressStats) -> Self {
        self.progress = Some(progress);
        self
    }

    pub fn with_time(mut self, time: TimeStats) -> Self {
        self.time = Some(time);
        self
    }

    /// Emit the log entry as JSON to stdout
    pub fn emit(&self) {
        if let Ok(json) = serde_json::to_string(self) {
            println!("{}", json);
        }
    }
}

impl BucketInfo {
    pub fn new(name: &str, role: BucketRole) -> Self {
        Self {
            name: name.to_string(),
            role,
            prefix_patterns: None,
        }
    }

    pub fn archived(name: &str) -> Self {
        Self::new(name, BucketRole::Archived)
    }

    pub fn consolidated(name: &str) -> Self {
        Self::new(name, BucketRole::Consolidated)
    }

    pub fn with_patterns(mut self, patterns: Option<Vec<String>>) -> Self {
        self.prefix_patterns = patterns;
        self
    }
}

impl FileStats {
    pub fn new(file_count: usize, total_size_bytes: usize) -> Self {
        Self {
            file_count,
            total_size_bytes,
            total_size: human_bytes(total_size_bytes as f64),
            checksum: None,
        }
    }

    pub fn with_checksum(mut self, checksum: String) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

impl CharacterStats {
    pub fn matched(archived_total: u64, consolidated_total: u64) -> Self {
        Self {
            archived_total,
            archived_total_human: human_bytes(archived_total as f64),
            consolidated_total,
            consolidated_total_human: human_bytes(consolidated_total as f64),
            match_status: MatchStatus::Match,
            difference_count: None,
            difference_total: None,
            difference_hash: None,
            per_bucket: None,
            duration_secs: None,
        }
    }

    pub fn mismatched(
        archived_total: u64,
        consolidated_total: u64,
        difference_count: usize,
        difference_total: i64,
        difference_hash: String,
    ) -> Self {
        Self {
            archived_total,
            archived_total_human: human_bytes(archived_total as f64),
            consolidated_total,
            consolidated_total_human: human_bytes(consolidated_total as f64),
            match_status: MatchStatus::Mismatch,
            difference_count: Some(difference_count),
            difference_total: Some(difference_total),
            difference_hash: Some(difference_hash),
            per_bucket: None,
            duration_secs: None,
        }
    }

    pub fn with_per_bucket(mut self, per_bucket: Vec<BucketCharacterStats>) -> Self {
        self.per_bucket = Some(per_bucket);
        self
    }

    pub fn with_duration(mut self, duration_secs: i64) -> Self {
        self.duration_secs = Some(duration_secs);
        self
    }
}

impl BucketCharacterStats {
    pub fn new(name: &str, role: BucketRole, total_chars: u64) -> Self {
        Self {
            name: name.to_string(),
            role,
            total_chars,
            total_chars_human: human_bytes(total_chars as f64),
        }
    }

    pub fn archived(name: &str, total_chars: u64) -> Self {
        Self::new(name, BucketRole::Archived, total_chars)
    }

    pub fn consolidated(name: &str, total_chars: u64) -> Self {
        Self::new(name, BucketRole::Consolidated, total_chars)
    }
}
