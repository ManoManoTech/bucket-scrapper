// src/search/streaming_collector.rs
use super::result_collector::SearchCollector;
use super::streaming_writer::SharedFileWriter;
use anyhow::Result;

/// A sync SearchCollector that writes matches directly to SharedFileWriter.
/// Used inside spawn_blocking grep context — writes go straight to per-prefix
/// zstd encoders with no channel overhead.
pub struct DirectFileCollector {
    writer: SharedFileWriter,
    prefix: String,
    match_count: usize,
}

impl DirectFileCollector {
    pub fn new(writer: SharedFileWriter, prefix: String) -> Self {
        Self {
            writer,
            prefix,
            match_count: 0,
        }
    }
}

impl SearchCollector for DirectFileCollector {
    fn add_match(&mut self, line: &str) -> Result<()> {
        self.writer.write_match(&self.prefix, line)?;
        self.match_count += 1;
        Ok(())
    }

    fn match_count(&self) -> usize {
        self.match_count
    }
}
