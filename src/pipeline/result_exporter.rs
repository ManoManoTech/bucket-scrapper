// src/search/result_exporter.rs
use anyhow::Result;

/// Trait for exporting search results.
/// Implemented by DirectFileExporter (file output) and HttpStreamingExporter (HTTP output).
pub trait SearchExporter {
    /// Add a match line. Lines are `\n`-terminated (guaranteed by grep-searcher's
    /// UTF8 sink and BufRead::read_line). Returns Err if writing failed and
    /// searching must stop.
    fn add_match(&mut self, line: &str) -> Result<()>;
    fn match_count(&self) -> usize;
}
