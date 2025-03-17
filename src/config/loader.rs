// src/config/loader.rs
use crate::config::types::{BucketConfig, ConfigSchema};
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

pub fn get_consolidated_buckets(config: &ConfigSchema) -> Vec<&BucketConfig> {
    config.bucketsConsolidated.iter().collect()
}

pub fn get_archived_buckets(config: &ConfigSchema) -> Vec<&BucketConfig> {
    config.bucketsToConsolidate.iter().collect()
}

pub fn get_results_bucket(config: &ConfigSchema) -> Option<&BucketConfig> {
    config.bucketsCheckerResults.first()
}
