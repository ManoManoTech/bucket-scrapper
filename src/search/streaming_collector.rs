// src/search/streaming_collector.rs
use super::result_collector::SearchCollector;
use super::streaming_writer::{extract_prefix, MatchToWrite};
use tokio::sync::mpsc;
use tracing::warn;

/// A sync SearchCollector that streams matches to a file writer via blocking_send().
/// Used inside spawn_blocking grep context — provides real backpressure
/// (blocks the search thread when the channel is full, instead of dropping matches).
pub struct FileStreamingCollector {
    sender: mpsc::Sender<MatchToWrite>,
    match_count: usize,
    files_searched: usize,
}

impl FileStreamingCollector {
    pub fn new(sender: mpsc::Sender<MatchToWrite>) -> Self {
        Self {
            sender,
            match_count: 0,
            files_searched: 0,
        }
    }
}

impl SearchCollector for FileStreamingCollector {
    fn add_match(&mut self, bucket: &str, key: &str, _line_number: u64, line: &str) -> bool {
        self.match_count += 1;

        let prefix = extract_prefix(key);

        // blocking_send: blocks the calling thread until the channel has space.
        // This is safe because we're inside spawn_blocking. Provides real backpressure —
        // if the file writer can't keep up, the search thread waits instead of dropping data.
        match self.sender.blocking_send(MatchToWrite {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content: line.to_string(),
            prefix,
        }) {
            Ok(()) => true,
            Err(_) => {
                warn!("File writer channel closed, stopping search (sent {} matches)", self.match_count);
                false
            }
        }
    }

    fn add_count(&mut self, _bucket: &str, _key: &str, _count: u64) {
        // Count-only mode not supported for file streaming
    }

    fn mark_file_searched(&mut self) {
        self.files_searched += 1;
    }

    fn match_count(&self) -> usize {
        self.match_count
    }
}
