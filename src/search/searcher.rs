// src/search/searcher.rs
use anyhow::Result;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{BinaryDetection, MmapChoice, SearcherBuilder};
use std::io::Read;
use tracing::debug;

use super::result_collector::SearchResultCollector;

/// Configuration for the stream searcher
#[derive(Clone)]
pub struct SearchConfig {
    pub pattern: String,
    pub ignore_case: bool,
    pub count_only: bool,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            pattern: String::new(),
            ignore_case: false,
            count_only: false,
        }
    }
}

/// A searcher that can process streams of data using ripgrep's engine
pub struct StreamSearcher {
    config: SearchConfig,
    matcher: RegexMatcher,
}

// We can't derive Clone because RegexMatcher doesn't implement it
// But we can work with Arc<StreamSearcher> instead

impl StreamSearcher {
    pub fn new(config: SearchConfig) -> Result<Self> {
        // Build the regex matcher once at initialization
        let mut matcher_builder = RegexMatcherBuilder::new();
        matcher_builder.case_insensitive(config.ignore_case);

        let matcher = matcher_builder
            .build(&config.pattern)
            .map_err(|e| anyhow::anyhow!("Invalid regex pattern: {}", e))?;

        Ok(Self { config, matcher })
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

        // Configure the searcher (no context lines needed, but keep line numbers for the sink API)
        let mut searcher_builder = SearcherBuilder::new();
        searcher_builder
            .binary_detection(BinaryDetection::quit(b'\x00'))
            .line_number(true) // Keep enabled for the sink API (we'll ignore the values)
            .memory_map(MmapChoice::never()); // Never mmap since we're streaming

        let mut searcher = searcher_builder.build();

        // Create a sink that will collect results
        if self.config.count_only {
            // Just count matches without storing them
            let mut count = 0u64;
            searcher.search_reader(
                &self.matcher,
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
            // Collect full match information (no line numbers)
            let mut match_count = 0u64;

            searcher.search_reader(
                &self.matcher,
                &mut reader,
                grep_searcher::sinks::UTF8(|_line_num, line| {
                    // Line number is always 0 since we disabled line tracking
                    collector.add_match(bucket, key, 0, line);
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
