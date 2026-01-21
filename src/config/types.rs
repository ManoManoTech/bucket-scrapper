// src/config/types.rs
// Location: src/config/types.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum PathSchema {
    Static { static_path: String },
    DateFormat { datefmt: String },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BucketConfig {
    pub bucket: String,
    #[serde(skip_serializing)]
    pub path: Vec<PathSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub only_prefix_patterns: Option<Vec<String>>,
    #[serde(default, skip_serializing)]
    pub proceed_without_matching_objects: bool,
    #[serde(flatten, skip_serializing)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

/// Configuration for continuous consolidation mode.
/// Allows the checker to automatically select a target hour to check.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContinuousConsolidationConfig {
    /// Minimum age before checking (e.g., "70m" for 70 minutes).
    /// Hours newer than (now - min_age) are not eligible.
    pub min_age: String,

    /// Maximum age to consider (e.g., "528h" for 22 days).
    /// Hours older than (now - max_age) are not considered.
    pub max_age: String,

    /// Step interval (always "1h" for hourly granularity).
    /// Currently unused but preserved for config compatibility.
    #[serde(default = "default_step")]
    pub step: String,
}

fn default_step() -> String {
    "1h".to_string()
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
pub struct ConfigSchema {
    pub bucketsToConsolidate: Vec<BucketConfig>,
    pub bucketsConsolidated: Vec<BucketConfig>,
    pub bucketsCheckerResults: Vec<BucketConfig>,
    /// Optional continuous consolidation config for auto-selecting target hours
    #[serde(default)]
    pub continuousConsolidation: Option<ContinuousConsolidationConfig>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

pub type DateString = String; // YYYYMMDD format
pub type HourString = String; // HH format 00-23

#[derive(Debug, Clone, Serialize)]
pub struct S3ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct S3FileList {
    pub bucket: String,
    pub key_prefix: String,
    pub checksum: String,
    pub files: Vec<S3ObjectInfo>,
    pub total_archives_size: usize,
}
