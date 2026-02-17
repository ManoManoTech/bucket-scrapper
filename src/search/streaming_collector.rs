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
    files_searched: usize,
}

impl DirectFileCollector {
    pub fn new(writer: SharedFileWriter, prefix: String) -> Self {
        Self {
            writer,
            prefix,
            match_count: 0,
            files_searched: 0,
        }
    }
}

impl SearchCollector for DirectFileCollector {
    fn add_match(&mut self, _bucket: &str, _key: &str, _line_number: u64, line: &str) -> Result<()> {
        self.writer.write_match(&self.prefix, line)?;
        self.match_count += 1;
        Ok(())
    }

    fn mark_file_searched(&mut self) {
        self.files_searched += 1;
    }

    fn match_count(&self) -> usize {
        self.match_count
    }
}
