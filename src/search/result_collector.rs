// src/search/result_collector.rs
use anyhow::Result;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Trait for collecting search results
/// Implemented by both SearchResultCollector (file output) and HttpStreamingCollector (HTTP output)
pub trait SearchCollector {
    /// Add a match. Returns Err if writing failed and searching must stop.
    fn add_match(&mut self, bucket: &str, key: &str, line_number: u64, line: &str) -> Result<()>;
    fn mark_file_searched(&mut self);
    fn match_count(&self) -> usize;
}

/// A single search match
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMatch {
    pub bucket: String,
    pub key: String,
    pub line_number: u64,
    pub line_content: String,
}

/// Results from searching through S3 objects
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SearchResult {
    pub matches: Vec<SearchMatch>,
    pub file_counts: HashMap<String, u64>, // bucket/key -> match count
    pub total_matches: u64,
    pub files_searched: usize,
    pub files_with_matches: usize,
    pub matches_by_prefix: HashMap<String, Vec<SearchMatch>>, // prefix -> matches
}

/// Collects search results from multiple files
pub struct SearchResultCollector {
    matches: Vec<SearchMatch>,
    file_counts: HashMap<String, u64>,
    files_searched: usize,
}

impl SearchResultCollector {
    pub fn new() -> Self {
        Self {
            matches: Vec::new(),
            file_counts: HashMap::new(),
            files_searched: 0,
        }
    }

    /// Add a match from a specific file
    pub fn add_match(&mut self, bucket: &str, key: &str, line_number: u64, line: &str) {
        let file_key = format!("{bucket}/{key}");

        self.matches.push(SearchMatch {
            bucket: bucket.to_string(),
            key: key.to_string(),
            line_number,
            line_content: line.to_string(),
        });

        *self.file_counts.entry(file_key).or_insert(0) += 1;
    }

    /// Mark that a file was searched (even if no matches found)
    pub fn mark_file_searched(&mut self) {
        self.files_searched += 1;
    }

    /// Convert to final result
    pub fn into_result(self) -> SearchResult {
        let total_matches: u64 = self.file_counts.values().sum();
        let files_with_matches = self.file_counts.len();

        // Group matches by date/hour prefix (e.g., "20240115/10")
        let mut matches_by_prefix: HashMap<String, Vec<SearchMatch>> = HashMap::new();
        for match_item in &self.matches {
            // Extract date/hour from key like "log-archives/dt=20240115/hour=10/file.json.zst"
            let prefix = extract_date_hour_prefix(&match_item.key);
            matches_by_prefix
                .entry(prefix)
                .or_default()
                .push(match_item.clone());
        }

        SearchResult {
            matches: self.matches,
            file_counts: self.file_counts,
            total_matches,
            files_searched: self.files_searched,
            files_with_matches,
            matches_by_prefix,
        }
    }

    /// Get current match count
    pub fn match_count(&self) -> usize {
        self.matches.len()
    }

    /// Clear all collected results
    pub fn clear(&mut self) {
        self.matches.clear();
        self.file_counts.clear();
        self.files_searched = 0;
    }

    /// Merge another collector's results into this one
    pub fn merge(&mut self, other: SearchResultCollector) {
        self.matches.extend(other.matches);
        for (k, v) in other.file_counts {
            *self.file_counts.entry(k).or_insert(0) += v;
        }
        self.files_searched += other.files_searched;
    }
}

impl SearchCollector for SearchResultCollector {
    fn add_match(&mut self, bucket: &str, key: &str, line_number: u64, line: &str) -> Result<()> {
        let file_key = format!("{bucket}/{key}");

        self.matches.push(SearchMatch {
            bucket: bucket.to_string(),
            key: key.to_string(),
            line_number,
            line_content: line.to_string(),
        });

        *self.file_counts.entry(file_key).or_insert(0) += 1;
        Ok(())
    }

    fn mark_file_searched(&mut self) {
        self.files_searched += 1;
    }

    fn match_count(&self) -> usize {
        self.matches.len()
    }
}

impl Default for SearchResultCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract date/hour prefix from S3 key like "log-archives/dt=20240115/hour=10/file.json.zst"
/// Returns something like "20240115H10"
fn extract_date_hour_prefix(key: &str) -> String {
    let re = Regex::new(r"dt=(\d{8})/hour=(\d{2})").unwrap();
    if let Some(captures) = re.captures(key) {
        format!("{}H{}", &captures[1], &captures[2])
    } else {
        // Fallback: use the directory structure
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() >= 2 {
            parts[..parts.len() - 1].join("_").to_string()
        } else {
            "unknown".to_string()
        }
    }
}
