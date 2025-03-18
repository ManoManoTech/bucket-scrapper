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

#[derive(Debug, Deserialize, Clone)]
pub struct BucketConfig {
    pub bucket: String,
    pub path: Vec<PathSchema>,
    #[serde(default)]
    pub only_prefix_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub proceed_without_matching_objects: bool,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(non_snake_case)]
pub struct ConfigSchema {
    pub bucketsToConsolidate: Vec<BucketConfig>,
    pub bucketsConsolidated: Vec<BucketConfig>,
    pub bucketsCheckerResults: Vec<BucketConfig>,
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
