// src/s3/mod.rs
pub mod client;
pub mod dns_cache;
pub mod streaming_downloader;
pub mod streaming_search_executor;

pub use streaming_downloader::{StreamingDownloader, StreamingDownloaderConfig};
pub use streaming_search_executor::{StreamingSearchConfig, StreamingSearchExecutor};
