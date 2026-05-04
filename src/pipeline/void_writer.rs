//! Void output sink: drops matches with minimal syscalls.
//!
//! Used to benchmark the read/decompress/filter pipeline in isolation, with
//! no compression, no IO, and no allocation in the hot path.

use super::output::{BoxFinishFuture, OutputSink, OutputStats};
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// No-op sink. Counts matches and bytes for end-of-run reporting.
#[derive(Default)]
pub struct VoidOutputSink {
    lines: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl VoidOutputSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OutputSink for VoidOutputSink {
    fn ingest(&self, _prefix: &str, line: &[u8]) -> Result<()> {
        self.lines.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(line.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn finish<'a>(&'a self) -> BoxFinishFuture<'a> {
        Box::pin(async move {
            Ok(OutputStats {
                matched_lines: self.lines.load(Ordering::Relaxed),
                plaintext_bytes: self.bytes.load(Ordering::Relaxed),
                compressed_bytes: 0,
                lines_dropped: 0,
                extras: Default::default(),
            })
        })
    }

    fn type_name(&self) -> &'static str {
        "void"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn void_sink_counts_lines_and_bytes() {
        let sink = VoidOutputSink::new();
        sink.ingest("p", b"hello\n").unwrap();
        sink.ingest("p", b"world\n").unwrap();
        let stats = sink.finish().await.unwrap();
        assert_eq!(stats.matched_lines, 2);
        assert_eq!(stats.plaintext_bytes, 12);
        assert_eq!(stats.compressed_bytes, 0);
    }
}
