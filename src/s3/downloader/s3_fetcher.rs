// src/s3/downloader/s3_fetcher.rs
use crate::config::types::S3ObjectInfo;
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::signal_handler::ProgressTracker;
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::Client;
use log::{debug, warn, info};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::{mpsc, Semaphore};
use futures::future::join_all;

use super::types::{CompressionType, RawObjectData};

pub struct S3Fetcher {
    client: Client,
    memory_allocator: Arc<MemoryLimitedAllocator>,
    download_semaphore: Arc<Semaphore>,
}

impl S3Fetcher {
    pub fn new(client: Client, max_concurrent_downloads: usize, memory_allocator: Arc<MemoryLimitedAllocator>) -> Self {
        let download_semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));
        Self {
            client,
            memory_allocator,
            download_semaphore,
        }
    }

    /// Concurrently fetch multiple objects from S3
    pub async fn fetch_objects(
        &self,
        objects: &[S3ObjectInfo],
        tx: mpsc::Sender<RawObjectData>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Result<()> {
        // Log initial memory allocation stats
        info!("Starting downloads. Memory pool size: {} bytes", self.memory_allocator.stats().1);

        // Handle empty objects list gracefully
        if objects.is_empty() {
            // Deliberately drop tx to signal end of data
            drop(tx);
            return Err(anyhow!("No objects to fetch."));
        }

        // Start downloading files concurrently
        let mut download_handles = Vec::new();
        for obj in objects {
            let permit = self.download_semaphore.clone().acquire_owned().await?;
            let obj_clone = obj.clone();
            let bucket_str = obj.bucket.to_string();
            let client_clone = self.client.clone();
            let tx_clone = tx.clone();
            let allocator = Arc::clone(&self.memory_allocator);
            let progress_tracker = progress_tracker.clone();

            let handle = tokio::spawn(async move {
                let result = Self::download_object(
                    &client_clone,
                    &bucket_str,
                    &obj_clone,
                    tx_clone,
                    allocator,
                    progress_tracker
                ).await;

                drop(permit); // Release permit when done

                if let Err(e) = result {
                    warn!("Error downloading {}: {}", obj_clone.key, e);
                }
            });

            download_handles.push(handle);
        }

        // Wait for all downloads to complete
        let results = join_all(download_handles).await;
        for result in results {
            if let Err(e) = result {
                warn!("Download task failed: {}", e);
            }
        }

        // Close the channel to signal end of downloads
        // Explicitly drop tx here to close the channel
        drop(tx);

        debug!("All downloads completed.");
        Ok(())
    }

    // Downloads a single object and sends the data to the processing channel
    async fn download_object(
        client: &Client,
        bucket: &str,
        obj: &S3ObjectInfo,
        tx: mpsc::Sender<RawObjectData>,
        allocator: Arc<MemoryLimitedAllocator>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Result<()> {
        debug!("Downloading {} (size: {} bytes)", obj.key, obj.size);

        // Determine compression type
        let compression_type = if obj.key.ends_with(".gz") {
            CompressionType::Gzip
        } else if obj.key.ends_with(".zst") {
            CompressionType::Zstd
        } else {
            CompressionType::None
        };

        // Estimate buffer size needed (protect against 0 size)
        let estimated_size = obj.size.max(1024); // Minimum 1KB

        // Allocate a buffer from our memory-limited pool
        let mut buffer = allocator.alloc_vec(estimated_size).await;

        // Get the object from S3
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(&obj.key)
            .send()
            .await
            .with_context(|| format!("Failed to download S3 object: {}", obj.key))?;

        // Read the stream into our memory-limited buffer
        let byte_stream = resp.body;
        let async_read = byte_stream.into_async_read();
        let mut buf_reader = BufReader::new(async_read);

        // Reserve capacity in our buffer
        buffer.reserve(estimated_size);

        // Read the data
        match buf_reader.read_to_end(buffer.as_vec_mut()).await {
            Ok(bytes_read) => {
                debug!("Downloaded {} bytes for {}", bytes_read, obj.key);

                // Update progress tracker for download completion
                progress_tracker.increment_processed(bytes_read);

                // Send data for processing
                match tx.send(RawObjectData {
                    bucket: bucket.to_string(),
                    key: obj.key.clone(),
                    data: buffer,
                    compression_type,
                }).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!("Failed to send data for processing (channel closed): {}", e);
                        Err(anyhow::anyhow!("Channel closed, receiver likely dropped: {}", e))
                    }
                }
            },
            Err(e) => Err(anyhow::anyhow!("Failed to read data from S3 stream for {}: {}", obj.key, e)),
        }
    }
}