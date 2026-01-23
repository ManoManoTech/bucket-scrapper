// src/s3/dns_cache.rs
//! DNS caching layer to reduce load on CoreDNS when running many concurrent processes.
//!
//! Uses hickory-resolver with moka cache for application-level DNS caching.
//! This significantly reduces DNS queries when running hundreds of CHECK commands in parallel.
//!
//! The key integration is the `AwsDnsResolverAdapter` which implements the AWS SDK's
//! `ResolveDns` trait, allowing our cached resolver to be used by the S3 client.

use anyhow::Result;
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use moka::future::Cache;
use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Caching DNS resolver that reduces DNS lookup load.
///
/// Uses hickory-resolver for DNS lookups and moka for in-memory caching.
/// Cache entries have a configurable TTL (default 5 minutes).
#[derive(Clone)]
pub struct CachingDnsResolver {
    resolver: Arc<hickory_resolver::TokioAsyncResolver>,
    cache: Cache<String, Vec<IpAddr>>,
    ttl_seconds: u64,
}

impl CachingDnsResolver {
    /// Create a new caching DNS resolver with the specified TTL.
    ///
    /// # Arguments
    /// * `ttl_seconds` - How long to cache DNS results (default: 300 = 5 minutes)
    pub async fn new(ttl_seconds: u64) -> Result<Self> {
        let resolver = hickory_resolver::TokioAsyncResolver::tokio_from_system_conf()
            .map_err(|e| anyhow::anyhow!("Failed to create DNS resolver: {}", e))?;

        let cache = Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(ttl_seconds))
            .build();

        info!(ttl_seconds = ttl_seconds, "DNS cache initialized");

        Ok(Self {
            resolver: Arc::new(resolver),
            cache,
            ttl_seconds,
        })
    }

    /// Resolve a hostname to IP addresses, using cache if available.
    pub async fn resolve(&self, hostname: &str) -> Result<Vec<IpAddr>> {
        // Check cache first
        if let Some(cached) = self.cache.get(hostname).await {
            debug!(hostname = hostname, cached_ips = ?cached, "DNS cache hit");
            return Ok(cached);
        }

        // Cache miss - perform DNS lookup
        debug!(hostname = hostname, "DNS cache miss, performing lookup");

        let lookup = self
            .resolver
            .lookup_ip(hostname)
            .await
            .map_err(|e| anyhow::anyhow!("DNS lookup failed for {}: {}", hostname, e))?;

        let addresses: Vec<IpAddr> = lookup.iter().collect();

        if addresses.is_empty() {
            return Err(anyhow::anyhow!("No addresses found for {}", hostname));
        }

        // Cache the result
        self.cache
            .insert(hostname.to_string(), addresses.clone())
            .await;

        debug!(
            hostname = hostname,
            addresses = ?addresses,
            ttl_seconds = self.ttl_seconds,
            "DNS lookup successful, cached"
        );

        Ok(addresses)
    }

    /// Pre-warm the DNS cache with commonly used AWS hostnames.
    ///
    /// This should be called at application startup to reduce DNS load
    /// during concurrent operations.
    pub async fn prewarm_aws_endpoints(&self, region: &str) {
        let hostnames = vec![
            format!("s3.{}.amazonaws.com", region),
            format!("s3.dualstack.{}.amazonaws.com", region),
            format!("sts.{}.amazonaws.com", region),
            format!("sts.amazonaws.com"),
        ];

        info!(
            region = region,
            endpoints = hostnames.len(),
            "Pre-warming DNS cache for AWS endpoints"
        );

        for hostname in hostnames {
            match self.resolve(&hostname).await {
                Ok(addrs) => {
                    debug!(
                        hostname = hostname,
                        addresses = ?addrs,
                        "Pre-warmed DNS cache"
                    );
                }
                Err(e) => {
                    warn!(
                        hostname = hostname,
                        error = %e,
                        "Failed to pre-warm DNS cache"
                    );
                }
            }
        }

        info!("DNS cache pre-warming complete");
    }
}

/// Adapter that implements AWS SDK's `ResolveDns` trait using our `CachingDnsResolver`.
///
/// This bridges our hickory-resolver + moka cache to the AWS SDK's HTTP client,
/// ensuring all S3 requests use our cached DNS resolver instead of creating
/// new DNS queries for each connection.
#[derive(Clone)]
pub struct AwsDnsResolverAdapter {
    inner: CachingDnsResolver,
}

impl Debug for AwsDnsResolverAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsDnsResolverAdapter")
            .field("cache_entries", &self.inner.cache.entry_count())
            .field("ttl_seconds", &self.inner.ttl_seconds)
            .finish()
    }
}

impl AwsDnsResolverAdapter {
    /// Create a new adapter wrapping the given caching resolver.
    pub fn new(resolver: CachingDnsResolver) -> Self {
        Self { inner: resolver }
    }
}

impl ResolveDns for AwsDnsResolverAdapter {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let resolver = self.inner.clone();
        let name = name.to_string();

        DnsFuture::new(async move {
            debug!(hostname = %name, "AWS SDK DNS resolution request");

            match resolver.resolve(&name).await {
                Ok(addresses) => {
                    debug!(
                        hostname = %name,
                        address_count = addresses.len(),
                        "DNS resolution successful"
                    );
                    Ok(addresses)
                }
                Err(e) => {
                    warn!(hostname = %name, error = %e, "DNS resolution failed");
                    Err(ResolveDnsError::new(e))
                }
            }
        })
    }
}

/// Global DNS cache instance for sharing across the application.
///
/// This allows DNS caching to work across multiple S3 client instances
/// and reduces DNS lookups when the client is refreshed.
static DNS_CACHE: std::sync::OnceLock<CachingDnsResolver> = std::sync::OnceLock::new();

/// Initialize the global DNS cache.
///
/// Should be called once at application startup before creating S3 clients.
pub async fn init_global_dns_cache(ttl_seconds: u64) -> Result<()> {
    let resolver = CachingDnsResolver::new(ttl_seconds).await?;
    DNS_CACHE
        .set(resolver)
        .map_err(|_| anyhow::anyhow!("DNS cache already initialized"))?;
    Ok(())
}

/// Get the global DNS cache instance.
pub fn get_global_dns_cache() -> Option<&'static CachingDnsResolver> {
    DNS_CACHE.get()
}

/// Pre-warm the global DNS cache for AWS endpoints.
///
/// This should be called at application startup to reduce initial DNS load.
pub async fn prewarm_global_dns_cache(region: &str) {
    if let Some(cache) = get_global_dns_cache() {
        cache.prewarm_aws_endpoints(region).await;
    } else {
        warn!("DNS cache not initialized, skipping pre-warm");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dns_cache_creation() {
        let cache = CachingDnsResolver::new(60).await.unwrap();
        assert_eq!(cache.ttl_seconds, 60);
    }

    #[tokio::test]
    async fn test_dns_cache_resolve() {
        let cache = CachingDnsResolver::new(300).await.unwrap();

        // First lookup should be a cache miss - skip test if no DNS available
        let result = cache.resolve("dns.google").await;
        if result.is_err() {
            // DNS not available in test environment, skip the rest
            eprintln!("Skipping DNS cache resolve test - no DNS available");
            return;
        }

        let addresses = result.unwrap();
        assert!(!addresses.is_empty());

        // Second lookup should be a cache hit
        let result2 = cache.resolve("dns.google").await;
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), addresses);

        // Run any pending cache operations before checking stats
        cache.cache.run_pending_tasks().await;

        // Check cache stats
        let entries = cache.cache.entry_count();
        assert!(
            entries >= 1,
            "Cache should have at least 1 entry after resolution"
        );
    }
}
