// src/s3/client.rs
// Location: src/s3/client.rs
use crate::config::types::S3ObjectInfo;
use crate::s3::dns_cache::{self, AwsDnsResolverAdapter};
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

    /// Get matching files from S3 with simple parameters
    pub async fn get_matching_filenames_from_s3(
        &self,
        bucket: &str,
        prefix: &str,
        filter_pattern: Option<&str>,
    ) -> Result<Vec<S3ObjectInfo>> {
        let client = self.get_client().await?;

        // Compile regex filter up front (before pagination loop)
        let filter_regex = filter_pattern
            .map(Regex::new)
            .transpose()
            .map_err(|e| anyhow::anyhow!("Invalid filter regex '{}': {}", filter_pattern.unwrap_or(""), e))?;

        debug!("Listing objects in s3://{}/{}", bucket, prefix);

        let mut all_objects = Vec::new();
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
                        let obj_info = S3ObjectInfo {
                            bucket: bucket.to_string(),
                            key,
                            size: size as usize,
                            last_modified: chrono::DateTime::from_timestamp_nanos(
                                last_modified.as_nanos() as i64,
                            ),
                        };

                        // Apply regex filter if provided
                        if let Some(ref regex) = filter_regex {
                            all_objects.push(obj_info.key.clone());
                            if regex.is_match(&obj_info.key) {
                                result.push(obj_info);
                            }
                        } else {
                            result.push(obj_info);
                        }
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        if let Some(pattern) = filter_pattern {
            info!(
                "Filter '{}': {} objects matched out of {} total in s3://{}/{}",
                pattern,
                result.len(),
                all_objects.len(),
                bucket,
                prefix
            );
        } else {
            debug!(
                "Found {} objects in s3://{}/{}",
                result.len(),
                bucket,
                prefix
            );
        }
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
