// src/config/loader.rs
use crate::config::types::{BucketConfig, ConfigSchema};
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use tracing::info;

/// Load configuration from a YAML file
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<ConfigSchema> {
    let config_str = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;

    let config: ConfigSchema =
        serde_yaml::from_str(&config_str).with_context(|| "Failed to parse YAML config")?;

    info!(
        "Config loaded successfully with {} buckets",
        config.buckets.len()
    );
    Ok(config)
}

/// Get configured buckets from the config
pub fn get_buckets(config: &ConfigSchema) -> Vec<&BucketConfig> {
    config.buckets.iter().collect()
}
