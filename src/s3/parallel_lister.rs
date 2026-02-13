// src/s3/parallel_lister.rs
use crate::config::types::S3ObjectInfo;
use crate::s3::client::WrappedS3Client;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

/// Performs parallel S3 LIST operations across multiple prefixes
pub struct ParallelLister {
    semaphore: Arc<Semaphore>,
}

impl ParallelLister {
    /// Create a new parallel lister with the specified concurrency
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    /// List objects from multiple prefixes in parallel
    pub async fn list_prefixes_parallel(
        &self,
        s3_client: &WrappedS3Client,
        bucket: &str,
        prefixes: Vec<String>,
        filter: Option<&str>,
    ) -> Result<Vec<S3ObjectInfo>> {
        if prefixes.is_empty() {
            return Ok(Vec::new());
        }

        let total_prefixes = prefixes.len();
        info!(
            "Starting parallel listing of {} prefixes in bucket {}",
            total_prefixes, bucket
        );

        // Use futures for parallel execution instead of spawning tasks
        let mut futures = Vec::new();

        for (idx, prefix) in prefixes.into_iter().enumerate() {
            let bucket = bucket.to_string();
            let filter = filter.map(|s| s.to_string());
            let semaphore = self.semaphore.clone();
            let total_count = total_prefixes;

            let future = async move {
                let _permit = semaphore.acquire().await?;

                debug!(
                    "Listing prefix {}/{} ({}/{})",
                    bucket,
                    prefix,
                    idx + 1,
                    total_count
                );

                let result: Result<Vec<S3ObjectInfo>> = s3_client
                    .get_matching_filenames_from_s3(&bucket, &prefix, filter.as_deref())
                    .await;

                match &result {
                    Ok(objects) if !objects.is_empty() => {
                        debug!("Found {} objects in {}/{}", objects.len(), bucket, prefix);
                    }
                    Ok(_) => {
                        debug!("No objects found in {}/{}", bucket, prefix);
                    }
                    Err(e) => {
                        warn!("Failed to list {}/{}: {}", bucket, prefix, e);
                    }
                }

                result
            };

            futures.push(future);
        }

        // Execute all futures concurrently
        let results = futures::future::join_all(futures).await;

        // Collect all results
        let mut all_objects = Vec::new();
        let mut errors = Vec::new();
        let mut successful_prefixes = 0;
        let mut total_objects = 0;

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(objects) => {
                    successful_prefixes += 1;
                    total_objects += objects.len();
                    all_objects.extend(objects);
                }
                Err(e) => {
                    errors.push(e.to_string());
                }
            }

            // Progress reporting for large operations
            if (idx + 1) % 100 == 0 {
                info!(
                    "Progress: Listed {}/{} prefixes, found {} objects so far",
                    idx + 1,
                    total_prefixes,
                    total_objects
                );
            }
        }

        if !errors.is_empty() {
            let first_error = errors.first().unwrap_or(&"unknown".to_string()).clone();

            if successful_prefixes == 0 {
                // All listings failed - this is a fatal error
                return Err(anyhow::anyhow!(
                    "All {} prefix listings failed (first error: {})",
                    errors.len(),
                    first_error
                ));
            } else {
                // Some listings failed - warn but continue
                warn!(
                    "{} prefix listings failed (first error: {})",
                    errors.len(),
                    first_error
                );
            }
        }

        info!(
            "Parallel listing complete: {}/{} prefixes successful, {} total objects found",
            successful_prefixes, total_prefixes, total_objects
        );

        Ok(all_objects)
    }

    /// List objects from a single bucket with optional date range (convenience method)
    pub async fn list_with_date_range(
        &self,
        s3_client: &WrappedS3Client,
        bucket: &str,
        date_hours: &[crate::utils::date::DateHour],
        filter: Option<&str>,
        prefix_format: Option<&str>, // Optional format like "logs/{}/{}"
    ) -> Result<Vec<S3ObjectInfo>> {
        let prefixes: Vec<String> = date_hours
            .iter()
            .map(|dh| {
                if let Some(format) = prefix_format {
                    format
                        .replace("{date}", &dh.date)
                        .replace("{hour}", &dh.hour)
                } else {
                    format!("{}/{}", dh.date, dh.hour)
                }
            })
            .collect();

        self.list_prefixes_parallel(s3_client, bucket, prefixes, filter)
            .await
    }
}

/// Builder pattern for parallel listing configuration
pub struct ParallelListBuilder {
    max_concurrent: usize,
    report_interval: usize,
}

impl Default for ParallelListBuilder {
    fn default() -> Self {
        Self {
            max_concurrent: 32,
            report_interval: 100,
        }
    }
}

impl ParallelListBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent = max;
        self
    }

    pub fn report_interval(mut self, interval: usize) -> Self {
        self.report_interval = interval;
        self
    }

    pub fn build(self) -> ParallelLister {
        ParallelLister::new(self.max_concurrent)
    }
}
