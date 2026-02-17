// src/config/types.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum PathSchema {
    Static { static_path: String },
    DateFormat { datefmt: String },
}

/// Bucket configuration with path components and patterns
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BucketConfig {
    pub bucket: String,
    #[serde(skip_serializing)]
    pub path: Vec<PathSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub only_prefix_patterns: Option<Vec<String>>,
    #[allow(dead_code)]
    #[serde(flatten, skip_serializing)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

impl BucketConfig {
    /// Validate that the bucket config has at least one DateFormat path component.
    /// Without it, the prefix never narrows by date/hour and we'd list the entire bucket.
    pub fn validate(&self) -> Result<(), String> {
        let has_datefmt = self.path.iter().any(|p| matches!(p, PathSchema::DateFormat { .. }));
        if !has_datefmt {
            return Err(format!(
                "Bucket '{}' has no datefmt in path — this would list the entire bucket prefix. \
                 Add a datefmt component like: datefmt: \"dt=20240101/hour=00\"",
                self.bucket
            ));
        }
        Ok(())
    }
}

/// HTTP output configuration for sending logs to a REST API (e.g., HTTP)
#[derive(Debug, Deserialize, Clone)]
pub struct HttpOutputConfig {
    /// The URL to send logs to (e.g., https://intake.handy-mango.http.com/api/v1/logs)
    pub url: String,
    /// API key for authentication (can also be set via HTTP_BEARER_AUTH env var)
    #[serde(default)]
    pub api_key: Option<String>,
    /// Timeout for HTTP requests in seconds (default: 30)
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_timeout_secs() -> u64 {
    30
}

/// Simplified config schema for bucket scrapper
#[derive(Debug, Deserialize, Clone)]
pub struct ConfigSchema {
    /// List of buckets to search
    #[serde(default)]
    pub buckets: Vec<BucketConfig>,

    /// Default AWS region (optional)
    #[serde(default)]
    pub region: Option<String>,

    /// Output directory for search results (file mode)
    #[serde(default)]
    pub output_dir: Option<String>,

    /// HTTP output configuration (for sending to REST API like REST API)
    #[serde(default)]
    pub http_output: Option<HttpOutputConfig>,

    #[allow(dead_code)]
    #[serde(flatten)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

impl Default for ConfigSchema {
    fn default() -> Self {
        Self {
            buckets: Vec::new(),
            region: None,
            output_dir: None,
            http_output: None,
            extra: HashMap::new(),
        }
    }
}

/// Information about an S3 object
#[derive(Debug, Clone, Serialize)]
pub struct S3ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    /// Date/hour prefix extracted from the key, used for output file grouping.
    pub prefix: String,
}

/// Extract date/hour prefix from an S3 key for output file grouping.
///
/// Looks for `dt=YYYYMMDD/hour=HH`; falls back to directory structure.
pub(crate) fn extract_prefix(key: &str) -> String {
    if let Some(dt_pos) = key.find("dt=") {
        if let Some(hour_pos) = key.find("hour=") {
            if dt_pos < hour_pos {
                let dt_start = dt_pos + 3;
                let dt_end = dt_start + 8; // YYYYMMDD
                let hour_start = hour_pos + 5;
                let hour_end = hour_start + 2; // HH

                if dt_end <= key.len() && hour_end <= key.len() {
                    let date = &key[dt_start..dt_end];
                    let hour = &key[hour_start..hour_end];
                    return format!("{}H{}", date, hour);
                }
            }
        }
    }

    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() >= 2 {
        parts[..parts.len() - 1].join("_")
    } else {
        "unknown".to_string()
    }
}

// Type aliases for date/hour strings
pub type DateString = String; // YYYYMMDD format
pub type HourString = String; // HH format 00-23

/// Collection of S3 files with metadata
#[derive(Debug, Clone)]
pub struct S3FileList {
    pub bucket: String,
    pub checksum: String,
    pub files: Vec<S3ObjectInfo>,
    pub total_archives_size: usize,
}
