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

#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub kind: BucketKind,
    pub bucket: String,
    pub proceed_without_matching_objects: bool,
    pub only_prefix_patterns: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BucketKind {
    Archived,
    Consolidated,
    Results,
}

pub type DateString = String; // YYYYMMDD format
pub type HourString = String; // HH format 00-23

#[derive(Debug, Clone, Serialize)]
pub struct S3ObjectInfo {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct S3FileList {
    pub filenames: Vec<S3ObjectInfo>,
    pub total_archives_size: usize,
}
