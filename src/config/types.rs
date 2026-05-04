use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::output::OutputConfig;

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

/// Simplified config schema for bucket scrapper
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigSchema {
    /// List of buckets to search
    #[serde(default)]
    pub buckets: Vec<BucketConfig>,

    /// Default AWS region (optional)
    #[serde(default)]
    pub region: Option<String>,

    /// Output configuration: one (and, today, only one) entry describing
    /// where matched lines should be written. When this list is non-empty
    /// the CLI per-output flags must be unset (config-driven mode).
    /// When omitted or empty, the output is built entirely from CLI flags
    /// (CLI-driven mode).
    #[serde(default)]
    pub outputs: Vec<OutputConfig>,
}

impl ConfigSchema {
    /// Validate the `outputs:` list. Today the rule is exactly-one entry; when
    /// concurrent fan-out lands, this validator relaxes to "1 or more".
    ///
    /// Returns `Ok(())` when the list is *empty* — that just means the user is
    /// driving the output from the CLI; the resolver will catch missing flags.
    pub fn validate_outputs(&self) -> Result<(), String> {
        match self.outputs.len() {
            0 | 1 => Ok(()),
            n => Err(format!(
                "outputs: list has {n} entries but only 1 is supported today \
                 (multi-output fan-out is not yet implemented)",
            )),
        }
    }
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

    #[test]
    fn config_schema_accepts_empty_outputs() {
        let cfg = ConfigSchema::default();
        cfg.validate_outputs().unwrap();
    }

    #[test]
    fn config_schema_rejects_legacy_output_dir_key() {
        // Hard-break: legacy keys must error out
        let yaml = r#"
buckets: []
output_dir: /tmp/out
"#;
        let err = serde_yaml::from_str::<ConfigSchema>(yaml).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("output_dir") || msg.contains("unknown field"),
            "got: {msg}"
        );
    }

    #[test]
    fn config_schema_rejects_legacy_http_output_key() {
        let yaml = r#"
buckets: []
http_output:
  url: https://example.com
"#;
        let err = serde_yaml::from_str::<ConfigSchema>(yaml).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("http_output") || msg.contains("unknown field"),
            "got: {msg}"
        );
    }
}
