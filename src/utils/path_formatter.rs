// src/utils/path_formatter.rs
// Location: src/utils/path_formatter.rs
use crate::config::types::{BucketConfig, DateString, HourString, PathSchema};
use crate::utils::date::{common_date_format, empty_date_format, raw_logs_date_format};
use anyhow::Result;
use log::warn;
use regex::Regex;

/// Extracts a formatter function for a date format string
fn extract_single_format_date_and_prefix(
    prefix: String,
) -> Box<dyn Fn(&DateString, &HourString) -> Result<String> + Send + Sync> {
    if prefix.contains("dt=") && prefix.contains("/hour=") {
        let regex = Regex::new(r"dt=.*\/hour=\d\d").unwrap();
        let key_prefix = regex.replace_all(&prefix, "").to_string();

        return Box::new(move |date: &DateString, hour: &HourString| {
            Ok(format!("{}{}", key_prefix, common_date_format(date, hour)))
        });
    } else if prefix.contains("2006/01/02/15") {
        let regex = Regex::new(r"2006\/01\/02\/15").unwrap();
        let key_prefix = regex.replace_all(&prefix, "").to_string();

        return Box::new(move |date: &DateString, hour: &HourString| {
            let formatted = raw_logs_date_format(date, hour)?;
            Ok(format!("{}{}", key_prefix, formatted))
        });
    } else {
        warn!("No date formatter found for prefix: {}", prefix);

        let prefix_clone = prefix.clone();
        return Box::new(move |date: &DateString, hour: &HourString| {
            Ok(format!("{}{}", prefix_clone, empty_date_format(date, hour)))
        });
    }
}

/// Generates a path formatter function from a bucket config
pub fn generate_path_formatter(
    bucket: &BucketConfig,
) -> Box<dyn Fn(&DateString, &HourString) -> Result<String> + Send + Sync> {
    let path_components = bucket.path.clone();

    Box::new(
        move |date: &DateString, hour: &HourString| -> Result<String> {
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
                .filter(|part| !part.is_empty())
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("/");

            Ok(path)
        },
    )
}
