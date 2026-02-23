// src/config/loader.rs
use crate::config::types::ConfigSchema;
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
        buckets = config.buckets.len(),
        "Config loaded"
    );
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn load_config_missing_file() {
        let err = load_config("/nonexistent/path.yaml").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("Failed to read config file"), "got: {msg}");
    }

    #[test]
    fn load_config_invalid_yaml() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b":::bad").unwrap();
        let err = load_config(f.path()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("Failed to parse YAML"), "got: {msg}");
    }

    #[test]
    fn load_config_valid_yaml() {
        let yaml = r#"
buckets:
  - bucket: my-bucket
    path:
      - static_path: "logs/"
      - datefmt: "dt=%Y%m%d"
"#;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        let config = load_config(f.path()).unwrap();
        assert_eq!(config.buckets.len(), 1);
        assert_eq!(config.buckets[0].bucket, "my-bucket");
    }

}
