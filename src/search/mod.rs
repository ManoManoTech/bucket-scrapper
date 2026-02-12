// src/search/mod.rs
pub mod http_writer;
pub mod result_collector;
pub mod searcher;
pub mod streaming_collector;
pub mod streaming_writer;

pub use http_writer::{HttpMatchToSend, HttpResultWriter, HttpStreamingCollector, HttpWriterConfig};
pub use result_collector::{SearchCollector, SearchResult, SearchResultCollector};
pub use searcher::{SearchConfig, StreamSearcher};
pub use streaming_collector::StreamingSearchCollector;
pub use streaming_writer::{extract_prefix, MatchToWrite, StreamingResultWriter};
