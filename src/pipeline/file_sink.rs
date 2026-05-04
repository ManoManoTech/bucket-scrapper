//! `OutputSink` adapter for [`SharedFileWriter`].

use super::output::{BoxFinishFuture, OutputSink, OutputStats};
use super::streaming_writer::SharedFileWriter;
use anyhow::Result;
use serde_json::json;
use std::sync::Arc;

/// Wraps a [`SharedFileWriter`] in the trait-object-friendly [`OutputSink`] shape.
pub struct FileOutputSink {
    writer: Arc<SharedFileWriter>,
}

impl FileOutputSink {
    pub fn new(writer: Arc<SharedFileWriter>) -> Self {
        Self { writer }
    }
}

impl OutputSink for FileOutputSink {
    fn ingest(&self, prefix: &str, line: &[u8]) -> Result<()> {
        self.writer.write_match(prefix, line)
    }

    fn finish<'a>(&'a self) -> BoxFinishFuture<'a> {
        Box::pin(async move {
            let stats = self.writer.finalize()?;
            Ok(OutputStats {
                matched_lines: stats.lines_written as u64,
                plaintext_bytes: stats.plaintext_bytes,
                compressed_bytes: stats.compressed_bytes,
                lines_dropped: 0,
                extras: serde_json::Map::from_iter([(
                    "output_files".to_string(),
                    json!(stats.files_written),
                )]),
            })
        })
    }

    fn type_name(&self) -> &'static str {
        "file"
    }
}
