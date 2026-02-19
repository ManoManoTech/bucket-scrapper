// src/pipeline/mod.rs
pub mod http_writer;
pub mod searcher;
pub mod streaming_writer;

pub use http_writer::{HttpResultWriter, HttpWriterConfig, HttpWriterStats, PipelineObserver};
pub use searcher::{SearchConfig, StreamSearcher};
pub use streaming_writer::{FileWriterStats, SharedFileWriter};
