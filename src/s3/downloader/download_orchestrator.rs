// src/s3/downloader/download_orchestrator.rs
use crate::config::types::S3ObjectInfo;
use crate::utils::character_counter::DetailedCharacterCount;
use crate::utils::memory_limited_allocator::MemoryLimitedAllocator;
use crate::utils::signal_handler::ProgressTracker;
use anyhow::Result;
use aws_sdk_s3::Client;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc};

use super::downloader::Downloader;
use super::processor::Processor;

/// Main downloader structure that orchestrates the S3 fetching and processing
pub struct DownloadOrchestrator {
    s3_fetcher: Downloader,
    processor: Processor,
    progress_tracker: Arc<ProgressTracker>,
}

impl DownloadOrchestrator {
    pub fn new(
        client: Client,
        max_concurrent_downloads: usize,
        max_processor_threads: usize,
        memory_allocator: Arc<MemoryLimitedAllocator>,
        progress_tracker: Arc<ProgressTracker>,
    ) -> Self {
        info!("Initializing S3Downloader with {} concurrent downloads, {} process threads and existing memory allocator",
              max_concurrent_downloads, max_processor_threads);

        Self {
            s3_fetcher: Downloader::new(client, max_concurrent_downloads, memory_allocator),
            processor: Processor::new(max_processor_threads),
            progress_tracker,
        }
    }

    /// Process a batch of S3 objects with controlled concurrency
    /// Returns a DetailedCharacterCount per bucket.
    pub async fn download_decompress_count(&self, objects: &[S3ObjectInfo]) -> Result<HashMap<String, DetailedCharacterCount>> {
        // Early return for empty objects
        if objects.is_empty() {
            return Err(anyhow::anyhow!("No objects to download."));
        }

        // Update progress tracker
        let total_bytes = objects.iter().map(|obj| obj.size).sum();
        self.progress_tracker.update_total(objects.len(), total_bytes);
        self.progress_tracker.reset();

        // Create channel
        let (tx, rx) = mpsc::channel(1);

        // First, spawn a task that owns rx and calls processor
        let processor = self.processor.clone(); // Make processor Clone
        let progress_clone = self.progress_tracker.clone();

        let process_handle = tokio::spawn(async move {
            info!("Starting processor task");
            processor.process_raw_data(rx, progress_clone).await
        });

        // Then fetch objects
        info!("Starting downloads");
        self.s3_fetcher.fetch_objects(
            objects,
            tx,
            self.progress_tracker.clone()
        ).await?;

        // Wait for processing to complete and get result
        info!("Waiting for processor to complete");
        process_handle.await.unwrap_or_else(|e| {
            Err(anyhow::anyhow!("Processor task failed: {}", e))
        })
    }
}