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

/// Simplified config schema for bucket scrapper
#[derive(Debug, Deserialize, Clone)]
pub struct ConfigSchema {
    /// List of buckets to search
    #[serde(default)]
    pub buckets: Vec<BucketConfig>,

    /// Default AWS region (optional)
    #[serde(default)]
    pub region: Option<String>,

    /// Output directory for search results
    #[serde(default)]
    pub output_dir: Option<String>,

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
