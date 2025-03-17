// src/s3/downloader/mod.rs
mod s3_fetcher;
mod processor;
mod types;

pub use s3_fetcher::S3Fetcher;
pub use processor::Processor;
pub use types::{CompressionType, ProcessItem, S3Downloader};
