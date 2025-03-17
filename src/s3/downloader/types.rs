// src/s3/downloader/types.rs
use crate::config::types::S3ObjectInfo;
use crate::utils::character_counter::DetailedCharacterCount;
use crate::utils::memory_limited_allocator::{MemoryLimitedAllocator, LimitedVec};
use crate::utils::signal_handler::ProgressTracker;
use anyhow::Result;
use aws_sdk_s3::Client;
use log::info;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

use super::s3_fetcher::S3Fetcher;
use super::processor::Processor;

/// Enum representing different compression types
#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
}

/// Structure to pass data between download and processing stages
pub struct ProcessItem {
    pub key: String,
    pub data: LimitedVec,
    pub compression_type: CompressionType,
}

/// Main downloader structure that orchestrates the S3 fetching and processing
pub struct S3Downloader {
    s3_fetcher: S3Fetcher,
    processor: Processor,
    download_semaphore: Arc<Semaphore>,
    process_threads: usize,
    memory_allocator: Arc<MemoryLimitedAllocator>,
    progress_tracker: Option<Arc<ProgressTracker>>,
}

impl S3Downloader {
    pub fn new_with_allocator(
        client: Client,
        max_concurrent_downloads: usize,
        process_threads: usize,
        memory_allocator: Arc<MemoryLimitedAllocator>
    ) -> Self {
        info!("Initializing S3Downloader with {} concurrent downloads, {} process threads and existing memory allocator",
              max_concurrent_downloads, process_threads);

        let download_semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));

        Self {
            s3_fetcher: S3Fetcher::new(client, memory_allocator.clone()),
            processor: Processor::new(),
            download_semaphore,
            process_threads,
            memory_allocator,
            progress_tracker: None,
        }
    }

    pub fn set_progress_tracker(&mut self, progress_tracker: Arc<ProgressTracker>) {
        self.progress_tracker = Some(progress_tracker);
    }

    pub fn get_progress_tracker(&self) -> Option<Arc<ProgressTracker>> {
        self.progress_tracker.clone()
    }

    pub fn get_memory_allocator(&self) -> Arc<MemoryLimitedAllocator> {
        Arc::clone(&self.memory_allocator)
    }

    /// Process a batch of S3 objects from a bucket with controlled concurrency
    pub async fn process_objects(&self, bucket: &str, objects: &[S3ObjectInfo]) -> Result<DetailedCharacterCount> {
        // Early return for empty objects
        if objects.is_empty() {
            return Ok(DetailedCharacterCount::new());
        }

        // Update progress tracker
        if let Some(tracker) = &self.progress_tracker {
            let total_bytes = objects.iter().map(|obj| obj.size).sum();
            tracker.update_total(objects.len(), total_bytes);
            tracker.update_current_bucket(bucket);
            tracker.reset();
        }

        // Create channel
        let (tx, rx) = mpsc::channel(self.process_threads);

        // First, spawn a task that owns rx and calls processor
        let processor = self.processor.clone(); // Make processor Clone
        let thread_count = self.process_threads;
        let progress_clone = self.progress_tracker.clone();

        let process_handle = tokio::spawn(async move {
            info!("Starting processor task");
            processor.process_data(rx, thread_count, progress_clone).await
        });

        // Then fetch objects
        info!("Starting downloads");
        self.s3_fetcher.fetch_objects(
            bucket,
            objects,
            &self.download_semaphore,
            tx,
            self.progress_tracker.clone()
        ).await?;

        // Wait for processing to complete and get result
        info!("Waiting for processor to complete");
        match process_handle.await {
            Ok(result) => result,
            Err(e) => {
                Err(anyhow::anyhow!("Processor task failed: {}", e))
            }
        }
    }
}