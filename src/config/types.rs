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
    /// Captures unknown YAML keys for forward-compatibility.
    #[serde(flatten, skip_serializing)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

impl BucketConfig {
    /// Validate that the bucket config has at least one DateFormat path component.
    /// Without it, the prefix never narrows by date/hour and we'd list the entire bucket.
    pub fn validate(&self) -> Result<(), String> {
        let has_datefmt = self
            .path
            .iter()
            .any(|p| matches!(p, PathSchema::DateFormat { .. }));
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

/// HTTP output configuration for sending logs to a REST API
#[derive(Debug, Deserialize, Clone)]
pub struct HttpOutputConfig {
    /// The URL to send logs to (e.g., https://logs.example.com/api/v1/logs)
    pub url: String,
    /// Bearer token for authentication (can also be set via HTTP_BEARER_AUTH env var)
    #[serde(default)]
    pub bearer_auth: Option<String>,
    /// Timeout for HTTP requests in seconds (default: 30)
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_timeout_secs() -> u64 {
    30
}

/// Simplified config schema for bucket scrapper
#[derive(Debug, Deserialize, Clone, Default)]
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

    /// HTTP output configuration (for sending to REST API)
    #[serde(default)]
    pub http_output: Option<HttpOutputConfig>,

    /// Captures unknown YAML keys for forward-compatibility.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_config(path: Vec<PathSchema>) -> BucketConfig {
        BucketConfig {
            bucket: "test-bucket".to_string(),
            path,
            only_prefix_patterns: None,
            extra: HashMap::new(),
        }
    }

    #[test]
    fn validate_rejects_path_without_datefmt() {
        let cfg = bucket_config(vec![PathSchema::Static {
            static_path: "logs/".to_string(),
        }]);
        let err = cfg.validate().unwrap_err();
        assert!(
            err.contains("test-bucket"),
            "error should name the bucket: {err}"
        );
    }

    #[test]
    fn validate_accepts_path_with_datefmt() {
        let cfg = bucket_config(vec![
            PathSchema::Static {
                static_path: "logs/".to_string(),
            },
            PathSchema::DateFormat {
                datefmt: "dt=%Y%m%d".to_string(),
            },
        ]);
        cfg.validate().unwrap();
    }

    #[test]
    fn validate_rejects_empty_path() {
        let cfg = bucket_config(vec![]);
        assert!(cfg.validate().is_err());
    }
}
