// src/search/searcher.rs
use anyhow::Result;
use grep_regex::RegexMatcherBuilder;
use grep_searcher::{BinaryDetection, MmapChoice, SearcherBuilder};
use std::io::Read;
use tracing::debug;

use super::result_collector::SearchResultCollector;

/// Configuration for the stream searcher
#[derive(Clone)]
pub struct SearchConfig {
    pub pattern: String,
    pub ignore_case: bool,
    pub context_lines: usize,
    pub count_only: bool,
    pub max_matches_per_file: Option<usize>,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            pattern: String::new(),
            ignore_case: false,
            context_lines: 0,
            count_only: false,
            max_matches_per_file: None,
        }
    }
}

/// A searcher that can process streams of data using ripgrep's engine
#[derive(Clone)]
pub struct StreamSearcher {
    config: SearchConfig,
}

impl StreamSearcher {
    pub fn new(config: SearchConfig) -> Self {
        Self { config }
    }

    /// Search through a readable stream and collect results
    pub fn search_stream<R: Read>(
        &self,
        bucket: &str,
        key: &str,
        mut reader: R,
        collector: &mut SearchResultCollector,
    ) -> Result<()> {
        debug!("Starting search in {}/{}", bucket, key);

        // Build the regex matcher
        let mut matcher_builder = RegexMatcherBuilder::new();
        matcher_builder.case_insensitive(self.config.ignore_case);

        let matcher = matcher_builder
            .build(&self.config.pattern)
            .map_err(|e| anyhow::anyhow!("Invalid regex pattern: {}", e))?;

        // Configure the searcher
        let mut searcher_builder = SearcherBuilder::new();
        searcher_builder
            .binary_detection(BinaryDetection::quit(b'\x00'))
            .line_number(true)
            .before_context(self.config.context_lines)
            .after_context(self.config.context_lines)
            .memory_map(MmapChoice::never()); // Never mmap since we're streaming

        let mut searcher = searcher_builder.build();

        // Create a sink that will collect results
        if self.config.count_only {
            // Just count matches without storing them
            let mut count = 0u64;
            searcher.search_reader(
                &matcher,
                &mut reader,
                grep_searcher::sinks::UTF8(|_line_num, _line| {
                    count += 1;
                    Ok(true)
                }),
            )?;

            collector.add_count(bucket, key, count);
            if count > 0 {
                debug!("Found {} matches in {}/{}", count, bucket, key);
            }
        } else {
            // Collect full match information
            let mut match_count = 0u64;
            let max_matches = self.config.max_matches_per_file.unwrap_or(usize::MAX);

            searcher.search_reader(
                &matcher,
                &mut reader,
                grep_searcher::sinks::UTF8(|line_num, line| {
                    if match_count >= max_matches as u64 {
                        return Ok(false); // Stop searching
                    }

                    collector.add_match(bucket, key, line_num, line);
                    match_count += 1;
                    Ok(true)
                }),
            )?;

            if match_count > 0 {
                debug!("Found {} matches in {}/{}", match_count, bucket, key);
            }
        }

        Ok(())
    }

    /// Search through a byte slice (for testing or small data)
    pub fn search_bytes(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        collector: &mut SearchResultCollector,
    ) -> Result<()> {
        self.search_stream(bucket, key, std::io::Cursor::new(data), collector)
    }
}
