// src/s3/client.rs
// Location: src/s3/client.rs
use crate::config::types::{BucketConfig, DateString, HourString, S3FileList, S3ObjectInfo};
use crate::s3::dns_cache::{self, AwsDnsResolverAdapter};
use crate::utils::path_formatter::generate_path_formatter;
use anyhow::Result;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::Client;
use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider};
use aws_smithy_http_client::Builder as HttpClientBuilder;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_types::region::Region;
use regex::Regex;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

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
        // Pre-warm DNS cache if available to reduce DNS load
        dns_cache::prewarm_global_dns_cache(region).await;

        // Enable AWS SDK retries for transient failures (rate limiting, network issues)
        // Downloads also have additional retry logic in downloader.rs with backon
        // for full visibility via structured logging (date, hour, bucket, key, error)
        let retry_config = RetryConfig::standard()
            .with_max_attempts(5)
            .with_initial_backoff(Duration::from_millis(500))
            .with_max_backoff(Duration::from_secs(10));

        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(30))
            .operation_attempt_timeout(Duration::from_secs(60))
            .build();

        // Build custom HTTP client with our cached DNS resolver if available
        // This is the KEY integration - it ensures all S3 requests use our
        // hickory-resolver + moka cache instead of hyper's default system resolver
        let http_client = if let Some(dns_cache) = dns_cache::get_global_dns_cache() {
            info!("Creating S3 client with cached DNS resolver");
            let resolver = AwsDnsResolverAdapter::new(dns_cache.clone());
            HttpClientBuilder::new()
                .tls_provider(Provider::rustls(CryptoMode::AwsLc))
                .build_with_resolver(resolver)
        } else {
            warn!("DNS cache not available, using default resolver (will cause more DNS queries)");
            HttpClientBuilder::new()
                .tls_provider(Provider::rustls(CryptoMode::AwsLc))
                .build_https()
        };

        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_owned()))
            .retry_config(retry_config)
            .timeout_config(timeout_config)
            .http_client(http_client)
            .load()
            .await;

        // Build S3 client with disabled response checksum validation
        // This avoids warnings about part-level checksums from multipart uploads
        // which the SDK cannot validate
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();

        Ok(Client::from_conf(s3_config))
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

    /// Lists objects using a provided client - use this when making multiple calls
    /// to avoid repeated get_client() overhead
    pub async fn get_matching_filenames_from_s3_with_client(
        &self,
        client: &Client,
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

            let list_objects_req = client.list_objects_v2().bucket(bucket).prefix(&prefix);

            let list_objects_req = if let Some(token) = &continuation_token {
                list_objects_req.continuation_token(token)
            } else {
                list_objects_req
            };

            let response = list_objects_req.send().await.map_err(|e| {
                let err_msg = format!("{:#}", e); // Use alternate format for full error chain
                let err_debug = format!("{:?}", e); // Debug format for maximum details

                // Log detailed error information for debugging
                warn!(
                    bucket = %bucket,
                    prefix = %prefix,
                    error_message = %err_msg,
                    error_debug = %err_debug,
                    "S3 list_objects_v2 failed"
                );

                if err_msg.contains("dispatch failure") {
                    anyhow::anyhow!(
                        "S3 request failed: {}. This often indicates expired AWS credentials. \
                         Try running 'aws sso login' or check your AWS_* environment variables.",
                        err_msg
                    )
                } else if err_msg.contains("service error") {
                    // Extract more detail from service errors
                    anyhow::anyhow!(
                        "S3 list_objects_v2 to bucket '{}' prefix '{}' failed: {}",
                        bucket,
                        prefix,
                        err_msg
                    )
                } else {
                    anyhow::anyhow!(
                        "S3 list_objects_v2 to bucket '{}' prefix '{}' failed: {}",
                        bucket,
                        prefix,
                        err_msg
                    )
                }
            })?;
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
            files: filtered.0,
            total_archives_size: filtered.1,
        })
    }

    /// Get matching files from S3 with simple parameters
    pub async fn get_matching_filenames_from_s3(
        &self,
        bucket: &str,
        prefix: &str,
        filter_pattern: Option<&str>,
    ) -> Result<Vec<S3ObjectInfo>> {
        let client = self.get_client().await?;

        debug!("Listing objects in s3://{}/{}", bucket, prefix);

        let mut result = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = client.list_objects_v2().bucket(bucket).prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let (Some(key), Some(size), Some(last_modified)) =
                        (obj.key, obj.size, obj.last_modified)
                    {
                        // Apply filter if provided
                        if let Some(pattern) = filter_pattern {
                            if !key.contains(pattern) {
                                continue;
                            }
                        }

                        result.push(S3ObjectInfo {
                            bucket: bucket.to_string(),
                            key,
                            size: size as usize,
                            last_modified: chrono::DateTime::from_timestamp_nanos(
                                last_modified.as_nanos() as i64,
                            ),
                        });
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        debug!(
            "Found {} objects in s3://{}/{}",
            result.len(),
            bucket,
            prefix
        );
        Ok(result)
    }

    /// Downloads an object using a provided client - use this when making multiple downloads
    /// to avoid repeated get_client() overhead and reduce DNS lookups
    pub async fn download_object_with_client(
        &self,
        client: &Client,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<u8>> {
        debug!("Downloading object s3://{}/{}", bucket, key);

        let response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let err_msg = format!("{:#}", e); // Use alternate format for full error chain
                let err_debug = format!("{:?}", e); // Debug format for maximum details

                // Log detailed error information for debugging
                warn!(
                    bucket = %bucket,
                    key = %key,
                    error_message = %err_msg,
                    error_debug = %err_debug,
                    "S3 get_object failed"
                );

                if err_msg.contains("dispatch failure") {
                    anyhow::anyhow!(
                        "S3 download failed: {}. This often indicates expired AWS credentials. \
                         Try running 'aws sso login' or check your AWS_* environment variables.",
                        err_msg
                    )
                } else {
                    anyhow::anyhow!(
                        "S3 get_object from bucket '{}' key '{}' failed: {}",
                        bucket,
                        key,
                        err_msg
                    )
                }
            })?;

        let bytes = response.body.collect().await?.into_bytes().to_vec();

        debug!(
            "Downloaded {} bytes from s3://{}/{}",
            bytes.len(),
            bucket,
            key
        );

        Ok(bytes)
    }

    /// Uploads an object to S3
    pub async fn upload_object(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        let client = self.get_client().await?;
        self.upload_object_with_client(&client, bucket, key, data)
            .await
    }

    /// Uploads an object using a provided client
    pub async fn upload_object_with_client(
        &self,
        client: &Client,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<()> {
        debug!("Uploading object to s3://{}/{}", bucket, key);

        let body = aws_sdk_s3::primitives::ByteStream::from(data);

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .content_type("application/json")
            .send()
            .await
            .map_err(|e| {
                let err_msg = format!("{:#}", e); // Use alternate format for full error chain
                let err_debug = format!("{:?}", e); // Debug format for maximum details

                // Log detailed error information for debugging
                warn!(
                    bucket = %bucket,
                    key = %key,
                    error_message = %err_msg,
                    error_debug = %err_debug,
                    "S3 put_object failed"
                );

                anyhow::anyhow!(
                    "S3 put_object to bucket '{}' key '{}' failed: {}",
                    bucket,
                    key,
                    err_msg
                )
            })?;

        info!("Uploaded check result to s3://{}/{}", bucket, key);

        Ok(())
    }
}

/// Check if an S3 error is recoverable (can be skipped/retried) or fatal (should crash).
///
/// Non-recoverable errors indicate configuration/permission issues:
/// - AccessDenied: IAM permission issue
/// - InvalidAccessKeyId, SignatureDoesNotMatch: credential issue
/// - ExpiredToken: credential expired
/// - NoSuchBucket: bucket doesn't exist (configuration issue)
/// - AccountProblem, InvalidSecurity: account issues
///
/// Recoverable errors are transient and can be retried/skipped:
/// - Throttling, SlowDown: rate limiting
/// - ServiceUnavailable, InternalError: temporary AWS issues
/// - Network timeouts, connection errors
pub fn is_recoverable_s3_error(error_msg: &str) -> bool {
    // Non-recoverable error patterns - these should cause the checker to crash
    let fatal_patterns = [
        "AccessDenied",
        "Access Denied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "ExpiredTokenException",
        "NoSuchBucket",
        "InvalidBucketName",
        "AccountProblem",
        "InvalidSecurity",
        "NotSignedUp",
        "InvalidIdentityToken",
        "MalformedPolicy",
        "InvalidClientTokenId",
    ];

    // If any fatal pattern is found, the error is NOT recoverable
    for pattern in &fatal_patterns {
        if error_msg.contains(pattern) {
            return false;
        }
    }

    // All other errors are considered recoverable (transient)
    true
}
