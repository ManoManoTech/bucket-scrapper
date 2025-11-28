// src/s3/downloader/processor.rs
use crate::utils::character_counter::DetailedCharacterCount;
use crate::utils::signal_handler::ProgressTracker;
use anyhow::Result;
use futures::future::join_all;
use log::{debug, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task;

use super::types::{CompressionType, RawObjectData};

#[derive(Clone)]
pub struct Processor {
    // Can be extended with configuration options later
    max_processor_count: usize,
}

impl Processor {
    pub fn new(max_processor_count: usize) -> Self {
        Self {
            max_processor_count,
        }
    }

    /// Process data received from the S3 fetcher
    /// This asynchronous tasks waits for RawObjectData on the rx channel,
    /// and spawns up to max_processor_count decompress/downloaders
    pub async fn process_raw_data(
        &self,
        mut rx: mpsc::Receiver<RawObjectData>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Result<HashMap<String, DetailedCharacterCount>> {
        // Create shared counter for results
        let total_counts = Arc::new(Mutex::new(HashMap::<String, DetailedCharacterCount>::new()));

        // Create a simple semaphore to limit concurrent processing tasks
        let semaphore = Arc::new(Semaphore::new(self.max_processor_count));

        // Track pending futures
        let mut process_futures = Vec::new();

        while let Some(item) = rx.recv().await {
            let semaphore_clone = semaphore.clone();
            let progress_tracker_clone = progress_tracker.clone();

            // Acquire permit before spawning task
            let permit = semaphore_clone.acquire_owned().await?;

            // Spawn blocking task for CPU-intensive decompression and counting
            let handle = task::spawn_blocking(move || {
                // The permit will be dropped when this closure completes
                let _permit_guard = permit;

                // Process the data
                let result = Self::process_item(
                    &item.bucket.clone(),
                    &item.key,
                    &item.data,
                    &item.compression_type,
                );

                // Mark file as completed for progress tracking
                progress_tracker_clone.increment_completed();

                result.unwrap()
            });

            // Collect the processed results
            let total_counts_clone = Arc::clone(&total_counts);
            let aggregated_result = async move {
                match handle.await {
                    Ok(counts) => {
                        // Update the total counts
                        let mut locked_counts = total_counts_clone.lock().await;
                        if locked_counts.contains_key(&counts.bucket) {
                            let bucket_counts = locked_counts.get_mut(&counts.bucket).unwrap();
                            bucket_counts.add(&counts)
                        } else {
                            locked_counts.insert(counts.bucket.clone(), counts);
                        }
                    }
                    Err(e) => {
                        warn!("Processing task failed: {}", e);
                    }
                }
            };

            process_futures.push(aggregated_result);
        }

        // Wait for all processing to complete
        join_all(process_futures).await;

        // Return the final counts using proper pattern matching
        Ok(Arc::into_inner(total_counts)
            .map(|mutex| mutex.into_inner())
            .unwrap())
    }

    // Process a single downloaded item
    fn process_item(
        bucket: &str,
        key: &str,
        data: &crate::utils::memory_limited_allocator::LimitedVec,
        compression_type: &CompressionType,
    ) -> Result<DetailedCharacterCount> {
        debug!("Processing {})", key);

        let mut counts = DetailedCharacterCount::new();
        counts.bucket = bucket.to_owned();
        counts.prefix = key.to_owned();

        match compression_type {
            CompressionType::Gzip => {
                // Process gzip compressed data
                use flate2::read::MultiGzDecoder;
                let data_vec = data.as_vec();
                let mut decoder = MultiGzDecoder::new(data_vec.as_slice());
                Self::process_reader(&mut decoder, &mut counts)?;
            }
            CompressionType::Zstd => {
                // For zstd, use streaming decompression
                let data_vec = data.as_vec();
                let mut decompressor = zstd::Decoder::new(data_vec.as_slice())
                    .map_err(|e| anyhow::anyhow!("Failed to create Zstd decompressor: {}", e))?;

                Self::process_reader(&mut decompressor, &mut counts)?;
            }
            CompressionType::None => {
                // Process uncompressed data directly
                counts.increment_batch_unsafe(data.as_vec());
                // for &byte in data.as_vec() {
                //     counts.increment(byte);
                // }
            }
        }

        debug!("Finished processing {}: {} characters", key, counts.total());
        Ok(counts)
    }

    // Process any reader efficiently with a standard buffer
    fn process_reader<R: std::io::Read>(
        reader: &mut R,
        counts: &mut DetailedCharacterCount,
    ) -> Result<()> {
        // Use a fixed-size buffer for streaming decompression
        const BUFFER_SIZE: usize = 128 * 1024; // 64KB buffer
        let mut buffer = vec![0u8; BUFFER_SIZE];

        loop {
            match reader.read(&mut buffer[..]) {
                Ok(0) => break, // End of stream
                Ok(n) => {
                    // Count characters in this chunk
                    counts.increment_batch_unsafe(&buffer[0..n]);
                    // for &byte in &buffer[0..n] {
                    //     counts.increment(byte);
                    // }
                }
                Err(e) => return Err(anyhow::anyhow!("Read error: {}", e)),
            }
        }

        Ok(())
    }
}
