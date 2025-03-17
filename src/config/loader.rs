// src/config/loader.rs
use crate::config::types::{BucketConfig, BucketInfo, BucketKind, ConfigSchema};
use anyhow::{Context, Result};
use log::{info};
use std::fs;
use std::path::Path;

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<ConfigSchema> {
    let config_str = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;

    let config: ConfigSchema =
        serde_yaml::from_str(&config_str).with_context(|| "Failed to parse YAML config")?;

    // Validate config
    if config.bucketsConsolidated.len() > 1 {
        return Err(anyhow::anyhow!("More than one consolidated bucket found. We only support one destination bucket for now."));
    }

    if config.bucketsToConsolidate.is_empty() {
        return Err(anyhow::anyhow!(
            "No archived bucket found. We need at least one archived (source) bucket."
        ));
    }

    if config.bucketsCheckerResults.is_empty() {
        return Err(anyhow::anyhow!("No results bucket found."));
    }

    info!("Config loaded successfully");
    Ok(config)
}

pub fn get_all_buckets_info(config: &ConfigSchema) -> Vec<BucketInfo> {
    let mut buckets = Vec::new();

    // Add archived buckets
    for bucket in &config.bucketsToConsolidate {
        buckets.push(BucketInfo {
            kind: BucketKind::Archived,
            bucket: bucket.bucket.clone(),
            proceed_without_matching_objects: bucket.proceed_without_matching_objects,
            only_prefix_patterns: bucket.only_prefix_patterns.clone(),
        });
    }

    // Add consolidated bucket
    for bucket in &config.bucketsConsolidated {
        buckets.push(BucketInfo {
            kind: BucketKind::Consolidated,
            bucket: bucket.bucket.clone(),
            proceed_without_matching_objects: bucket.proceed_without_matching_objects,
            only_prefix_patterns: bucket.only_prefix_patterns.clone(),
        });
    }

    // Add results bucket
    for bucket in &config.bucketsCheckerResults {
        buckets.push(BucketInfo {
            kind: BucketKind::Results,
            bucket: bucket.bucket.clone(),
            proceed_without_matching_objects: bucket.proceed_without_matching_objects,
            only_prefix_patterns: bucket.only_prefix_patterns.clone(),
        });
    }

    buckets
}

pub fn get_consolidated_buckets(config: &ConfigSchema) -> Vec<&BucketConfig> {
    config.bucketsConsolidated.iter().collect()
}

pub fn get_archived_buckets(config: &ConfigSchema) -> Vec<&BucketConfig> {
    config.bucketsToConsolidate.iter().collect()
}

pub fn get_results_bucket(config: &ConfigSchema) -> Option<&BucketConfig> {
    config.bucketsCheckerResults.first()
}
