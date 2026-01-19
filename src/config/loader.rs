// src/config/loader.rs
use crate::config::types::{BucketConfig, ConfigSchema};
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use tracing::info;

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<ConfigSchema> {
    let config_str = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;

    let config: ConfigSchema =
        serde_yaml::from_str(&config_str).with_context(|| "Failed to parse YAML config")?;

    // Validate config
    if config.bucketsConsolidated.len() > 1 {
        return Err(anyhow::anyhow!(
            "More than one consolidated bucket found. We only support one destination bucket for now."
        ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_valid_config() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket"
    path:
      - static_path: "logs"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();

        let config = load_config(temp_file.path()).unwrap();

        assert_eq!(config.bucketsToConsolidate.len(), 1);
        assert_eq!(config.bucketsConsolidated.len(), 1);
        assert_eq!(config.bucketsCheckerResults.len(), 1);
        assert_eq!(config.bucketsToConsolidate[0].bucket, "source-bucket");
    }

    #[test]
    fn test_load_config_file_not_found() {
        let result = load_config("non_existent_file.yaml");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to read config file"));
    }

    #[test]
    fn test_load_config_invalid_yaml() {
        let invalid_yaml = "invalid: yaml: content: [";

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), invalid_yaml).unwrap();

        let result = load_config(temp_file.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to parse YAML config"));
    }

    #[test]
    fn test_load_config_too_many_consolidated_buckets() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket"
    path:
      - static_path: "logs"

bucketsConsolidated:
  - bucket: "consolidated-bucket-1"
    path:
      - static_path: "consolidated1"
  - bucket: "consolidated-bucket-2"
    path:
      - static_path: "consolidated2"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();

        let result = load_config(temp_file.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("More than one consolidated bucket found"));
    }

    #[test]
    fn test_load_config_no_archived_buckets() {
        let config_content = r#"
bucketsToConsolidate: []

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();

        let result = load_config(temp_file.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No archived bucket found"));
    }

    #[test]
    fn test_load_config_no_results_buckets() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket"
    path:
      - static_path: "logs"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults: []
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();

        let result = load_config(temp_file.path());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No results bucket found"));
    }

    #[test]
    fn test_get_consolidated_buckets() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket"
    path:
      - static_path: "logs"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();
        let config = load_config(temp_file.path()).unwrap();

        let consolidated = get_consolidated_buckets(&config);
        assert_eq!(consolidated.len(), 1);
        assert_eq!(consolidated[0].bucket, "consolidated-bucket");
    }

    #[test]
    fn test_get_archived_buckets() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket-1"
    path:
      - static_path: "logs1"
  - bucket: "source-bucket-2"
    path:
      - static_path: "logs2"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();
        let config = load_config(temp_file.path()).unwrap();

        let archived = get_archived_buckets(&config);
        assert_eq!(archived.len(), 2);
        assert_eq!(archived[0].bucket, "source-bucket-1");
        assert_eq!(archived[1].bucket, "source-bucket-2");
    }

    #[test]
    fn test_get_results_bucket() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket"
    path:
      - static_path: "logs"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();
        let config = load_config(temp_file.path()).unwrap();

        let results = get_results_bucket(&config);
        assert!(results.is_some());
        assert_eq!(results.unwrap().bucket, "results-bucket");
    }

    #[test]
    fn test_get_results_bucket_empty() {
        let config_content = r#"
bucketsToConsolidate:
  - bucket: "source-bucket"
    path:
      - static_path: "logs"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"

bucketsCheckerResults: []
"#;

        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), config_content).unwrap();

        // This should fail in load_config, but let's test the function directly
        let config: ConfigSchema = serde_yaml::from_str(config_content).unwrap();
        let results = get_results_bucket(&config);
        assert!(results.is_none());
    }
}
