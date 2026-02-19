// src/pipeline/searcher.rs
use anyhow::Result;
use grep_matcher::Matcher;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};

/// Configuration for the stream searcher
#[derive(Clone, Default)]
pub struct SearchConfig {
    pub pattern: Option<String>,
    pub ignore_case: bool,
}

/// A searcher that tests individual lines against a regex pattern.
/// When no pattern is provided, all lines match.
pub struct StreamSearcher {
    matcher: Option<RegexMatcher>,
}

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

    /// Test whether a single line matches the configured pattern.
    /// Returns `true` when no pattern is set (all-lines mode).
    pub fn matches_line(&self, line: &[u8]) -> bool {
        match self.matcher {
            Some(ref matcher) => matcher.is_match(line).unwrap_or(false),
            None => true,
        }
    }
}
