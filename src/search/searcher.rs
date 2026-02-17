// src/search/searcher.rs
use anyhow::Result;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{BinaryDetection, MmapChoice, SearcherBuilder};
use std::io::BufRead;
use tracing::debug;

use super::result_collector::SearchCollector;

/// Configuration for the stream searcher
#[derive(Clone)]
#[derive(Default)]
pub struct SearchConfig {
    pub pattern: Option<String>,
    pub ignore_case: bool,
}


/// A searcher that can process streams of data using ripgrep's engine.
/// When no pattern is provided, all lines are returned without regex overhead.
pub struct StreamSearcher {
    matcher: Option<RegexMatcher>,
}

// We can't derive Clone because RegexMatcher doesn't implement it
// But we can work with Arc<StreamSearcher> instead

impl StreamSearcher {
    pub fn new(config: SearchConfig) -> Result<Self> {
        let matcher = if let Some(ref pattern) = config.pattern {
            let mut matcher_builder = RegexMatcherBuilder::new();
            matcher_builder.case_insensitive(config.ignore_case);
            Some(
                matcher_builder
                    .build(pattern)
                    .map_err(|e| anyhow::anyhow!("Invalid regex pattern: {e}"))?,
            )
        } else {
            None
        };

        Ok(Self { matcher })
    }

    /// Search through a readable stream and collect results using any SearchCollector.
    /// When no pattern is configured, all lines are yielded via a fast BufRead loop.
    pub fn search_stream<R: BufRead, C: SearchCollector>(
        &self,
        bucket: &str,
        key: &str,
        reader: R,
        collector: &mut C,
    ) -> Result<()> {
        debug!(bucket = %bucket, key = %key, "Starting search");

        match self.matcher {
            Some(ref matcher) => self.search_stream_regex(bucket, key, reader, collector, matcher),
            None => self.search_stream_all_lines(bucket, key, reader, collector),
        }
    }

    fn search_stream_regex<R: BufRead, C: SearchCollector>(
        &self,
        bucket: &str,
        key: &str,
        mut reader: R,
        collector: &mut C,
        matcher: &RegexMatcher,
    ) -> Result<()> {
        let mut searcher_builder = SearcherBuilder::new();
        searcher_builder
            .binary_detection(BinaryDetection::quit(b'\x00'))
            .line_number(true)
            .memory_map(MmapChoice::never());

        let mut searcher = searcher_builder.build();

        let mut match_count = 0u64;

        searcher.search_reader(
            matcher,
            &mut reader,
            grep_searcher::sinks::UTF8(|_line_num, line| {
                collector
                    .add_match(bucket, key, 0, line)
                    .map_err(std::io::Error::other)?;
                match_count += 1;
                Ok(true)
            }),
        )?;

        if match_count > 0 {
            debug!(matches = match_count, bucket = %bucket, key = %key, "Found matches");
        }

        Ok(())
    }

    fn search_stream_all_lines<R: BufRead, C: SearchCollector>(
        &self,
        bucket: &str,
        key: &str,
        mut reader: R,
        collector: &mut C,
    ) -> Result<()> {
        let mut buf = String::new();
        let mut line_count = 0u64;

        loop {
            buf.clear();
            if reader.read_line(&mut buf)? == 0 {
                break;
            }
            collector.add_match(bucket, key, 0, &buf)?;
            line_count += 1;
        }
        if line_count > 0 {
            debug!(lines = line_count, bucket = %bucket, key = %key, "Read all lines");
        }

        Ok(())
    }

}
