// src/search/mod.rs
pub mod result_collector;
pub mod searcher;
pub mod streaming_collector;
pub mod streaming_writer;

pub use result_collector::{SearchResult, SearchResultCollector};
pub use searcher::{SearchConfig, StreamSearcher};
pub use streaming_collector::StreamingSearchCollector;
pub use streaming_writer::{extract_prefix, MatchToWrite, StreamingResultWriter};
