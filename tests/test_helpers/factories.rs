/// Test data factories for generating dynamic test data
/// These factories create test objects programmatically for various test scenarios
use serde_json::json;
use std::collections::HashMap;

/// Factory for creating JSON log entries
pub struct JsonLogFactory;

impl JsonLogFactory {
    /// Create a simple JSON log entry with env, service, and custom fields
    pub fn create_simple(
        env: &str,
        service: &str,
        fields: HashMap<&str, serde_json::Value>,
    ) -> String {
        let mut obj = json!({
            "env": env,
            "service": service
        });

        // Add custom fields
        for (key, value) in fields {
            obj[key] = value;
        }

        obj.to_string()
    }

    /// Create a realistic log entry with timestamp and standard fields
    pub fn create_realistic(env: &str, service: &str, level: &str, message: &str) -> String {
        json!({
            "env": env,
            "service": service,
            "level": level,
            "message": message,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "host": "test-host",
            "version": "1.0.0"
        })
        .to_string()
    }

    /// Create an error log entry
    pub fn create_error(env: &str, service: &str, error_msg: &str, severity: &str) -> String {
        json!({
            "env": env,
            "service": service,
            "level": "error",
            "error": error_msg,
            "severity": severity,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "stack_trace": "...",
            "retry_count": 3
        })
        .to_string()
    }

    /// Create a performance/metrics log entry
    pub fn create_metrics(env: &str, service: &str, endpoint: &str, duration_ms: u32) -> String {
        json!({
            "env": env,
            "service": service,
            "type": "metrics",
            "endpoint": endpoint,
            "method": "GET",
            "status": 200,
            "duration_ms": duration_ms,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "user_agent": "test-client/1.0"
        })
        .to_string()
    }

    /// Create a batch of JSON entries for the same env-service combination
    pub fn create_batch(env: &str, service: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| {
                Self::create_realistic(env, service, "info", &format!("Batch log entry #{}", i))
            })
            .collect()
    }

    /// Create entries for multiple env-service combinations
    pub fn create_multi_env_service(
        combinations: Vec<(&str, &str)>,
        entries_per_combo: usize,
    ) -> Vec<String> {
        let mut all_entries = Vec::new();

        for (env, service) in combinations {
            let entries = Self::create_batch(env, service, entries_per_combo);
            all_entries.extend(entries);
        }

        all_entries
    }
}

/// Factory for creating grouped and compressed test files
pub struct CompressedFileFactory;

impl CompressedFileFactory {
    /// Group JSON strings by env-service and return compressed data
    pub fn create_grouped_files(json_entries: Vec<String>) -> Vec<(String, String, Vec<u8>)> {
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();

        // Parse and group by env-service
        for json_str in json_entries {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json_str) {
                let env = parsed["env"].as_str().unwrap_or("unknown");
                let service = parsed["service"].as_str().unwrap_or("unknown");
                let key = format!("{}-{}", env, service);
                groups.entry(key).or_insert_with(Vec::new).push(json_str);
            }
        }

        // Create compressed files for each group
        let mut compressed_files = Vec::new();
        for (group_key, json_lines) in groups {
            let combined_json = json_lines.join("\n");
            let compressed_data = super::compression::compress_with_zstd(combined_json.as_bytes());
            let filename = format!("{}.json.zst", group_key);
            compressed_files.push((group_key, filename, compressed_data));
        }

        compressed_files
    }
}

/// Factory for creating file lists with various patterns
pub struct FileListFactory;

impl FileListFactory {
    /// Generate a list of log files with sequential naming
    pub fn create_sequential_files(
        base_path: &str,
        prefix: &str,
        count: usize,
        extension: &str,
    ) -> Vec<(String, String)> {
        (0..count)
            .map(|i| {
                let key = format!("{}/{}-{:03}.{}", base_path, prefix, i, extension);
                let content = format!("Log content for file {} #{}", prefix, i);
                (key, content)
            })
            .collect()
    }

    /// Generate files with different service prefixes
    pub fn create_prefixed_files(
        base_path: &str,
        prefixes: Vec<&str>,
        extension: &str,
    ) -> Vec<(String, String)> {
        prefixes
            .into_iter()
            .enumerate()
            .map(|(i, prefix)| {
                let key = format!("{}/{}-service.{}", base_path, prefix, extension);
                let content = format!("{} service logs #{}", prefix, i);
                (key, content)
            })
            .collect()
    }

    /// Generate a mix of compressed and uncompressed files
    pub fn create_mixed_files(base_path: &str) -> Vec<(String, String)> {
        vec![
            (
                format!("{}/app.log.gz", base_path),
                "Compressed log file".to_string(),
            ),
            (
                format!("{}/data.json.zst", base_path),
                "Compressed JSON file".to_string(),
            ),
            (
                format!("{}/metrics.json.gz", base_path),
                "Compressed metrics".to_string(),
            ),
            (
                format!("{}/config.txt", base_path),
                "Plain text file".to_string(),
            ),
            (
                format!("{}/readme.md", base_path),
                "Markdown file".to_string(),
            ),
        ]
    }
}

/// Factory for creating bucket configurations
pub struct BucketConfigFactory;

impl BucketConfigFactory {
    /// Create a quick test bucket config with minimal setup
    pub fn quick_config(
        bucket_name: &str,
    ) -> log_consolidator_checker_rust::config::types::BucketConfig {
        use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
        use std::collections::HashMap;

        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "2006/01/02/15".to_string(),
                },
            ],
            only_prefix_patterns: None,
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }

    /// Create config with custom prefix patterns
    pub fn with_patterns(
        bucket_name: &str,
        patterns: Vec<String>,
    ) -> log_consolidator_checker_rust::config::types::BucketConfig {
        use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
        use std::collections::HashMap;

        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "2006/01/02/15".to_string(),
                },
            ],
            only_prefix_patterns: Some(patterns),
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }

    /// Create complete environment configuration with all 6 bucket types
    pub fn create_full_environment_config(
    ) -> log_consolidator_checker_rust::config::types::ConfigSchema {
        use log_consolidator_checker_rust::config::types::ConfigSchema;
        use std::collections::HashMap;

        ConfigSchema {
            bucketsToConsolidate: vec![
                Self::input_bucket_config("input-bucket-1"),
                Self::input_bucket_config("input-bucket-2"),
            ],
            bucketsConsolidated: vec![
                Self::consolidated_bucket_config("consolidated-bucket-1"),
                Self::consolidated_bucket_config("consolidated-bucket-2"),
            ],
            bucketsCheckerResults: vec![
                Self::results_bucket_config("results-bucket-1"),
                Self::results_bucket_config("results-bucket-2"),
            ],
            extra: HashMap::new(),
        }
    }

    /// Create input bucket configuration with filtering patterns
    pub fn input_bucket_config(
        bucket_name: &str,
    ) -> log_consolidator_checker_rust::config::types::BucketConfig {
        use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
        use std::collections::HashMap;

        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "raw-logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "dt=placeholder/hour=99".to_string(),
                },
            ],
            only_prefix_patterns: Some(vec![
                "^app-.*".to_string(),
                "^web-.*".to_string(),
                "^api-.*".to_string(),
            ]),
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }

    /// Create consolidated bucket configuration
    pub fn consolidated_bucket_config(
        bucket_name: &str,
    ) -> log_consolidator_checker_rust::config::types::BucketConfig {
        use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
        use std::collections::HashMap;

        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "processed".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "2006/01/02/15".to_string(),
                },
            ],
            only_prefix_patterns: None,
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }

    /// Create results bucket configuration
    pub fn results_bucket_config(
        bucket_name: &str,
    ) -> log_consolidator_checker_rust::config::types::BucketConfig {
        use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
        use std::collections::HashMap;

        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![PathSchema::Static {
                static_path: "results".to_string(),
            }],
            only_prefix_patterns: None,
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }

    /// Create bucket group configurations
    pub fn create_bucket_group(
        group_type: &str,
        count: usize,
    ) -> Vec<log_consolidator_checker_rust::config::types::BucketConfig> {
        (0..count)
            .map(|i| {
                let bucket_name = format!("{}-bucket-{}", group_type, i + 1);
                match group_type {
                    "input" => Self::input_bucket_config(&bucket_name),
                    "consolidated" => Self::consolidated_bucket_config(&bucket_name),
                    "results" => Self::results_bucket_config(&bucket_name),
                    _ => Self::quick_config(&bucket_name),
                }
            })
            .collect()
    }

    /// Create standard test configuration with customizable bucket counts
    pub fn create_test_config(
        input_count: usize,
        consolidated_count: usize,
        results_count: usize,
    ) -> log_consolidator_checker_rust::config::types::ConfigSchema {
        use log_consolidator_checker_rust::config::types::ConfigSchema;
        use std::collections::HashMap;

        ConfigSchema {
            bucketsToConsolidate: Self::create_bucket_group("input", input_count),
            bucketsConsolidated: Self::create_bucket_group("consolidated", consolidated_count),
            bucketsCheckerResults: Self::create_bucket_group("results", results_count),
            extra: HashMap::new(),
        }
    }

    /// Create configuration for archive bucket pattern (legacy support)
    pub fn archive_bucket_config(
        bucket_name: &str,
    ) -> log_consolidator_checker_rust::config::types::BucketConfig {
        use log_consolidator_checker_rust::config::types::{BucketConfig, PathSchema};
        use std::collections::HashMap;

        BucketConfig {
            bucket: bucket_name.to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "archived".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "2006/01/02/15".to_string(),
                },
            ],
            only_prefix_patterns: None,
            proceed_without_matching_objects: false,
            extra: HashMap::new(),
        }
    }
}
