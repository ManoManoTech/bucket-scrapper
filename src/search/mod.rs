// src/search/mod.rs
pub mod result_collector;
pub mod searcher;

pub use result_collector::{SearchResult, SearchResultCollector};
pub use searcher::{SearchConfig, StreamSearcher};
