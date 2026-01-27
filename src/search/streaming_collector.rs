// src/search/streaming_collector.rs
use super::streaming_writer::{extract_prefix, MatchToWrite, StreamingResultWriter};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A search result collector that streams matches directly to compressed output files
pub struct StreamingSearchCollector {
    writer: Arc<StreamingResultWriter>,
    file_counts: Arc<Mutex<HashMap<String, u64>>>,
    files_searched: Arc<Mutex<usize>>,
}

impl StreamingSearchCollector {
    /// Create a new streaming collector
    pub fn new(output_dir: String) -> Result<Self> {
        let writer = Arc::new(StreamingResultWriter::new(output_dir)?);

        Ok(Self {
            writer,
            file_counts: Arc::new(Mutex::new(HashMap::new())),
            files_searched: Arc::new(Mutex::new(0)),
        })
    }

    /// Add a match and stream it directly to output
    pub async fn add_match(&self, bucket: &str, key: &str, line: &str) -> Result<()> {
        // Update counts
        {
            let file_key = format!("{}/{}", bucket, key);
            let mut counts = self.file_counts.lock().await;
            *counts.entry(file_key).or_insert(0) += 1;
        }

        // Extract prefix for grouping
        let prefix = extract_prefix(key);

        // Create match to write
        let match_to_write = MatchToWrite {
            bucket: bucket.to_string(),
            key: key.to_string(),
            content: line.to_string(),
            prefix,
        };

        // Stream to writer
        self.writer.write_match(match_to_write).await?;

        Ok(())
    }

    /// Add just a count for a file (when in count-only mode)
    pub async fn add_count(&self, bucket: &str, key: &str, count: u64) -> Result<()> {
        if count > 0 {
            let file_key = format!("{}/{}", bucket, key);
            let mut counts = self.file_counts.lock().await;
            counts.insert(file_key, count);
        }
        Ok(())
    }

    /// Mark that a file was searched
    pub async fn mark_file_searched(&self) -> Result<()> {
        let mut searched = self.files_searched.lock().await;
        *searched += 1;
        Ok(())
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> (usize, usize, u64) {
        let counts = self.file_counts.lock().await;
        let files_searched = *self.files_searched.lock().await;
        let total_matches: u64 = counts.values().sum();
        let files_with_matches = counts.len();

        (files_searched, files_with_matches, total_matches)
    }

    /// Finish streaming and return the number of output files written
    pub async fn finish(self) -> Result<usize> {
        // Get the writer from Arc (this will fail if there are other references)
        match Arc::try_unwrap(self.writer) {
            Ok(writer) => writer.finish().await,
            Err(_) => {
                // If we can't unwrap, just return 0
                Ok(0)
            }
        }
    }

    /// Get file counts for reporting
    pub async fn get_file_counts(&self) -> HashMap<String, u64> {
        self.file_counts.lock().await.clone()
    }
}
