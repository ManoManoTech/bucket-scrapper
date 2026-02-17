// src/search/result_collector.rs
use anyhow::Result;

/// Trait for collecting search results.
/// Implemented by DirectFileCollector (file output) and HttpStreamingCollector (HTTP output).
pub trait SearchCollector {
    /// Add a match line. Returns Err if writing failed and searching must stop.
    fn add_match(&mut self, line: &str) -> Result<()>;
    fn match_count(&self) -> usize;
}
