use super::types::S3ObjectInfo;
use crate::s3::dns_cache::{self, AwsDnsResolverAdapter};
use anyhow::{Context, Result};
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::Client;
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider, TlsContext, TrustStore};
use aws_smithy_http_client::{Builder as HttpClientBuilder, Connector};
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
        // Downloads also have range-based resume in the pipeline orchestrator
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

        // Build custom HTTP client with proxy support, custom CA, and cached DNS resolver
        let proxy_config = ProxyConfig::from_env();
        if !proxy_config.is_disabled() {
            info!("Proxy configured from environment variables");
        }

        let tls_context = build_tls_context()?;

        let http_client = if let Some(dns_cache) = dns_cache::get_global_dns_cache() {
            info!("Creating S3 client with cached DNS resolver");
            let resolver = AwsDnsResolverAdapter::new(dns_cache.clone());
            HttpClientBuilder::new().build_with_connector_fn(move |_, _| {
                Connector::builder()
                    .proxy_config(proxy_config.clone())
                    .tls_provider(Provider::rustls(CryptoMode::AwsLc))
                    .tls_context(tls_context.clone())
                    .build_with_resolver(resolver.clone())
            })
        } else {
            warn!("DNS cache not available, using default resolver");
            HttpClientBuilder::new().build_with_connector_fn(move |_, _| {
                Connector::builder()
                    .proxy_config(proxy_config.clone())
                    .tls_provider(Provider::rustls(CryptoMode::AwsLc))
                    .tls_context(tls_context.clone())
                    .build()
            })
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
        let filter_regex = filter_pattern.map(Regex::new).transpose().map_err(|e| {
            anyhow::anyhow!(
                "Invalid filter regex '{}': {}",
                filter_pattern.unwrap_or(""),
                e
            )
        })?;

        debug!(bucket = %bucket, prefix = %prefix, "Listing objects");

        let mut all_keys = Vec::new();
        let mut matched = Vec::new();
        let mut continuation_token = None;
        let mut pages = 0u32;

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
                            prefix: prefix.to_string(),
                        };

                        // Apply regex filter if provided
                        if let Some(ref regex) = filter_regex {
                            all_keys.push(obj_info.key.clone());
                            if regex.is_match(&obj_info.key) {
                                matched.push(obj_info);
                            }
                        } else {
                            matched.push(obj_info);
                        }
                    }
                }
            }

            pages += 1;

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
                debug!(
                    bucket = %bucket,
                    prefix = %prefix,
                    page = pages,
                    keys = matched.len(),
                    "Listing page, continuing"
                );
            } else {
                break;
            }
        }

        if filter_pattern.is_some() {
            debug!(
                bucket = %bucket,
                prefix = %prefix,
                matched = matched.len(),
                total = all_keys.len(),
                pages = pages,
                "Listed with filter"
            );
        } else {
            debug!(
                bucket = %bucket,
                prefix = %prefix,
                objects = matched.len(),
                pages = pages,
                "Listed"
            );
        }
        Ok(matched)
    }
}

/// Build TLS context, adding a custom CA cert if `AWS_CA_BUNDLE` is set
/// or auto-detecting `~/.mitmproxy/mitmproxy-ca-cert.pem` when a proxy env var is present.
fn build_tls_context() -> Result<TlsContext> {
    let mut trust_store = TrustStore::default();

    if let Some(path) = crate::utils::proxy::resolve_ca_bundle_path() {
        let pem =
            std::fs::read(&path).with_context(|| format!("Failed to read CA bundle: {path}"))?;
        info!(path = %path, "Loaded custom CA bundle for S3 client");
        trust_store = trust_store.with_pem_certificate(pem);
    }

    TlsContext::builder()
        .with_trust_store(trust_store)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build TLS context: {e}"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_s3_errors_are_not_recoverable() {
        let fatal = [
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
        for pattern in &fatal {
            assert!(
                !is_recoverable_s3_error(pattern),
                "'{pattern}' should be fatal"
            );
        }
    }

    #[test]
    fn transient_s3_errors_are_recoverable() {
        let transient = [
            "Throttling",
            "SlowDown",
            "ServiceUnavailable",
            "InternalError",
            "connection reset",
            "timeout",
        ];
        for pattern in &transient {
            assert!(
                is_recoverable_s3_error(pattern),
                "'{pattern}' should be recoverable"
            );
        }
    }

    #[test]
    fn empty_error_is_recoverable() {
        assert!(is_recoverable_s3_error(""));
    }

    #[test]
    fn fatal_pattern_as_substring() {
        assert!(!is_recoverable_s3_error("Something AccessDenied happened"));
        assert!(!is_recoverable_s3_error("Error: NoSuchBucket for arn:..."));
    }
}
