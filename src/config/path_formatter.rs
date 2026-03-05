use super::types::{BucketConfig, PathSchema};
use crate::utils::date::{common_date_format, empty_date_format, raw_logs_date_format};
use anyhow::Result;
use regex::Regex;
use tracing::warn;

type PathFormatter = Box<dyn Fn(&str, &str) -> Result<String> + Send + Sync>;

/// Extracts a formatter function for a date format string
fn extract_single_format_date_and_prefix(prefix: String) -> PathFormatter {
    if prefix.contains("dt=") && (prefix.contains("/hour=") || prefix.contains("/h=")) {
        let use_hour_format = prefix.contains("/hour=");
        let regex = if use_hour_format {
            Regex::new(r"dt=.*\/hour=\d\d").unwrap()
        } else {
            Regex::new(r"dt=.*\/h=\d\d").unwrap()
        };
        let key_prefix = regex.replace_all(&prefix, "").to_string();

        Box::new(move |date: &str, hour: &str| {
            if use_hour_format {
                Ok(format!("{}{}", key_prefix, common_date_format(date, hour)))
            } else {
                // Use h= format instead of hour=
                Ok(format!("{key_prefix}dt={date}/h={hour}"))
            }
        })
    } else if prefix.contains("2006/01/02/15") {
        // Go reference time: Mon Jan 2 15:04:05 MST 2006 (YYYY/MM/DD/HH)
        let regex = Regex::new(r"2006\/01\/02\/15").unwrap();
        let key_prefix = regex.replace_all(&prefix, "").to_string();

        Box::new(move |date: &str, hour: &str| {
            let formatted = raw_logs_date_format(date, hour)?;
            Ok(format!("{key_prefix}{formatted}"))
        })
    } else {
        warn!(prefix = %prefix, "No date formatter found for prefix");

        let prefix_clone = prefix.clone();
        Box::new(move |date: &str, hour: &str| {
            Ok(format!("{}{}", prefix_clone, empty_date_format(date, hour)))
        })
    }
}

/// Generates a path formatter function from a bucket config
///
/// # Examples
///
/// ```
/// use bucket_scrapper::config::types::{BucketConfig, PathSchema};
/// use bucket_scrapper::config::path_formatter::generate_path_formatter;
/// use std::collections::HashMap;
///
/// let bucket = BucketConfig {
///     bucket: "test-bucket".to_string(),
///     path: vec![
///         PathSchema::Static { static_path: "logs".to_string() },
///         PathSchema::DateFormat { datefmt: "dt=20231225/hour=14".to_string() }
///     ],
///     only_prefix_patterns: None,
///     extra: HashMap::new(),
/// };
///
/// let formatter = generate_path_formatter(&bucket);
/// let result = formatter("20231225", "14").unwrap();
/// assert_eq!(result, "logs/dt=20231225/hour=14");
/// ```
pub fn generate_path_formatter(bucket: &BucketConfig) -> PathFormatter {
    let path_components = bucket.path.clone();

    Box::new(move |date: &str, hour: &str| -> Result<String> {
        let mut parts = Vec::new();

        for component in &path_components {
            match component {
                PathSchema::Static { static_path } => {
                    parts.push(static_path.clone());
                }
                PathSchema::DateFormat { datefmt } => {
                    let formatter = extract_single_format_date_and_prefix(datefmt.clone());
                    parts.push(formatter(date, hour)?);
                }
            }
        }

        // Filter out empty parts and join with "/"
        let path = parts
            .iter()
            .filter(|part: &&String| !part.is_empty())
            .map(|s: &String| s.as_str())
            .collect::<Vec<_>>()
            .join("/");

        Ok(path)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_generate_path_formatter_static_only() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::Static {
                    static_path: "data".to_string(),
                },
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        assert_eq!(result, "logs/data");
    }

    #[test]
    fn test_generate_path_formatter_common_date_format() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "dt=20231225/hour=14".to_string(),
                },
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        assert_eq!(result, "logs/dt=20231225/hour=14");
    }

    #[test]
    fn test_generate_path_formatter_raw_logs_format() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "raw".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "2006/01/02/15".to_string(),
                },
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        assert_eq!(result, "raw/2023/12/25/14");
    }

    #[test]
    fn test_generate_path_formatter_with_prefix() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "app".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "prefix-dt=placeholder/hour=99-suffix".to_string(),
                },
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        // The regex removes "dt=placeholder/hour=99" and replaces with actual date
        assert_eq!(result, "app/prefix--suffixdt=20231225/hour=14");
    }

    #[test]
    fn test_generate_path_formatter_unknown_format() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "unknown-format".to_string(),
                },
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        // Should fall back to empty format
        assert_eq!(result, "logs/unknown-format");
    }

    #[test]
    fn test_generate_path_formatter_empty_parts_filtered() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "".to_string(),
                }, // Empty - should be filtered
                PathSchema::Static {
                    static_path: "logs".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "dt=20231225/hour=14".to_string(),
                },
                PathSchema::Static {
                    static_path: "".to_string(),
                }, // Empty - should be filtered
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        assert_eq!(result, "logs/dt=20231225/hour=14");
    }

    #[test]
    fn test_generate_path_formatter_invalid_raw_date() {
        let bucket = BucketConfig {
            bucket: "test-bucket".to_string(),
            path: vec![PathSchema::DateFormat {
                datefmt: "2006/01/02/15".to_string(),
            }],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        // Invalid date format should return error
        let result = formatter("invalid", "14");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_single_format_date_and_prefix_common() {
        let formatter = extract_single_format_date_and_prefix(
            "prefix-dt=placeholder/hour=99-suffix".to_string(),
        );
        let result = formatter("20231225", "14").unwrap();
        // Regex removes "dt=placeholder/hour=99", leaves "prefix--suffix", then adds actual date
        assert_eq!(result, "prefix--suffixdt=20231225/hour=14");
    }

    #[test]
    fn test_extract_single_format_date_and_prefix_raw() {
        let formatter =
            extract_single_format_date_and_prefix("prefix-2006/01/02/15-suffix".to_string());
        let result = formatter("20231225", "14").unwrap();
        // Regex removes "2006/01/02/15", leaves "prefix--suffix", then adds actual date
        assert_eq!(result, "prefix--suffix2023/12/25/14");
    }

    #[test]
    fn test_complex_path_structure() {
        let bucket = BucketConfig {
            bucket: "complex-bucket".to_string(),
            path: vec![
                PathSchema::Static {
                    static_path: "app".to_string(),
                },
                PathSchema::Static {
                    static_path: "env".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "year=2006/month=01/day=02".to_string(),
                },
                PathSchema::Static {
                    static_path: "hour".to_string(),
                },
                PathSchema::DateFormat {
                    datefmt: "h=15".to_string(),
                },
            ],
            only_prefix_patterns: None,
            extra: HashMap::new(),
        };

        let formatter = generate_path_formatter(&bucket);
        let result = formatter("20231225", "14").unwrap();
        // The first DateFormat doesn't match known patterns, so it's returned as-is
        // The second DateFormat doesn't match known patterns either
        assert_eq!(result, "app/env/year=2006/month=01/day=02/hour/h=15");
    }
}
