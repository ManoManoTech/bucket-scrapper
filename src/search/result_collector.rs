// src/search/result_collector.rs
use anyhow::Result;

/// Trait for collecting search results.
/// Implemented by DirectFileCollector (file output) and HttpStreamingCollector (HTTP output).
pub trait SearchCollector {
    /// Add a match. Returns Err if writing failed and searching must stop.
    fn add_match(&mut self, bucket: &str, key: &str, line_number: u64, line: &str) -> Result<()>;
    fn mark_file_searched(&mut self);
    fn match_count(&self) -> usize;
}
