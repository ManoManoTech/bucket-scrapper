// src/s3/client.rs
// Location: src/s3/client.rs
use crate::config::types::{BucketConfig, DateString, HourString, S3FileList, S3ObjectInfo};
use crate::utils::path_formatter::generate_path_formatter;
use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use log::{debug, info};
use regex::Regex;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct WrappedS3Client {
    pub client: RwLock<(std::time::Instant, Client)>,
    max_age: Duration,
    region: String,
}

impl WrappedS3Client {
    pub async fn new(region: &str, max_age_minutes: u64, client: Option<Client>) -> Result<Self> {
        let client = match client {
            Some(existing_client) => existing_client,
            None => Self::create_client(region).await?,
        };

        Ok(Self {
            client: RwLock::new((std::time::Instant::now(), client)),
            max_age: Duration::from_secs(max_age_minutes * 60),
            region: region.to_string(),
        })
    }

    async fn create_client(region: &str) -> Result<Client> {
        let retry_config = RetryConfig::standard().with_max_attempts(3);

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_owned()))
            .retry_config(retry_config)
            .load()
            .await;

        Ok(Client::new(&config))
    }

    pub async fn get_client(&self) -> Result<Client> {
        let now = std::time::Instant::now();

        // Check if we have fresh client using read lock
        {
            let guard = self.client.read().await;
            let (last_refresh, client) = &*guard;

            // Return early if client is still fresh
            if now.duration_since(*last_refresh) <= self.max_age {
                return Ok(client.clone());
            }
        }

        let mut guard = self.client.write().await;
        if now.duration_since(guard.0) > self.max_age {
            info!("Refreshing S3 client");
            *guard = (now, Self::create_client(&self.region).await?);
        }
        Ok(guard.1.clone())
    }

    /// Lists objects in a bucket with a prefix and returns information about them
    pub async fn get_matching_filenames_from_s3(
        &self,
        bucket_config: &BucketConfig,
        date: &DateString,
        hour: &HourString,
        will_filter: bool,
    ) -> Result<S3FileList> {
        let formatter = generate_path_formatter(bucket_config);
        let prefix = formatter(date, hour)?;
        let bucket = &bucket_config.bucket;

        debug!("Get filenames for {} in {}", prefix, bucket);

        let mut result = Vec::new();
        let mut continuation_token = None;
        let mut total_size: usize = 0;

        // Build regex filters if needed
        let filename_pattern_filter = if let Some(patterns) = &bucket_config.only_prefix_patterns {
            let compiled_patterns = patterns
                .iter()
                .map(|pattern| Regex::new(pattern).unwrap())
                .collect::<Vec<_>>();

            Some(compiled_patterns)
        } else {
            None
        };

        // List objects in the bucket with the specified prefix
        loop {
            debug!(
                "Listing objects for {} in {} with continuation token {:?}",
                prefix, bucket, continuation_token
            );

            let client = self.get_client().await?;
            let list_objects_req = client.list_objects_v2().bucket(bucket).prefix(&prefix);

            let list_objects_req = if let Some(token) = &continuation_token {
                list_objects_req.continuation_token(token)
            } else {
                list_objects_req
            };

            let response = list_objects_req.send().await?;
            debug!(
                "Got {} objects for {} in {}",
                response.contents().len(),
                prefix,
                bucket
            );

            if !response.contents().is_empty() {
                let mapped = response
                    .contents()
                    .iter()
                    .map(|o| {
                        // +1 to remove the trailing slash
                        let filename = o.key().unwrap_or_default();
                        let filename_only = if let Some(stripped) = filename.strip_prefix(&prefix) {
                            if stripped.starts_with('/') {
                                &stripped[1..]
                            } else {
                                stripped
                            }
                        } else {
                            filename
                        };

                        (
                            S3ObjectInfo {
                                bucket: bucket.clone(),
                                key: filename.to_string(),
                                size: o.size().unwrap_or_default() as usize,
                                last_modified: o
                                    .last_modified()
                                    .map(|dt| {
                                        chrono::DateTime::from_timestamp_nanos(dt.as_nanos() as i64)
                                    })
                                    .unwrap_or_default(),
                            },
                            filename_only.to_string(),
                        )
                    })
                    .collect::<Vec<_>>();

                for (obj_info, _) in &mapped {
                    total_size += obj_info.size;
                }

                result.extend(mapped);
            }

            continuation_token = response.next_continuation_token().map(|s| s.to_owned());

            if continuation_token.is_none() {
                break;
            }
        }

        debug!(
            "Before filter: Found {} files for {} in {} ({} bytes)",
            result.len(),
            prefix,
            bucket,
            total_size
        );

        // Apply filters if needed
        let filtered = if will_filter {
            let filtered_items = result
                .into_iter()
                .filter(|(obj_info, filename_only)| {
                    // First check if it matches our extension filters
                    let key = &obj_info.key;
                    let ext_match = key.ends_with(".json.zst")
                        || key.ends_with(".json.gz")
                        || key.ends_with(".log.gz");

                    // Then check if it matches any pattern filters if they exist
                    let pattern_match = if let Some(patterns) = &filename_pattern_filter {
                        patterns.iter().any(|regex| regex.is_match(filename_only))
                    } else {
                        true
                    };

                    ext_match && pattern_match
                })
                .map(|(obj_info, _)| obj_info)
                .collect::<Vec<_>>();

            let filtered_size = filtered_items.iter().map(|item| item.size).sum();

            debug!(
                "After filter: Found {} files for {} in {} ({} bytes)",
                filtered_items.len(),
                prefix,
                bucket,
                filtered_size
            );

            (filtered_items, filtered_size)
        } else {
            let items = result.into_iter().map(|(obj_info, _)| obj_info).collect();
            (items, total_size)
        };

        // Calculate a checksum of all filenames to enable identifying changes in file lists
        let files_checksum = {
            let mut filenames = filtered.0.iter().map(|f| f.key.clone()).collect::<Vec<_>>();
            filenames.sort();
            let joined = filenames.join("");
            format!("{:x}", md5::compute(joined))
        };

        Ok(S3FileList {
            bucket: bucket.clone(),
            checksum: files_checksum.clone(),
            key_prefix: prefix,
            files: filtered.0,
            total_archives_size: filtered.1,
        })
    }
}
