#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

use bucket_scrapper::config::loader::load_config;
use bucket_scrapper::config::types::BucketConfig;
use bucket_scrapper::s3::S3ObjectInfo;
use bucket_scrapper::s3::client::WrappedS3Client;
use bucket_scrapper::s3::dns_cache;
use bucket_scrapper::pipeline::{StreamingDownloader, StreamingDownloaderConfig};
use bucket_scrapper::matcher::{LineMatcher, MatcherConfig};
use bucket_scrapper::pipeline::{HttpResultWriter, HttpWriterConfig, SharedFileWriter};
use bucket_scrapper::utils::date::date_range_to_date_hour_list;
use bucket_scrapper::config::path_formatter::generate_path_formatter;

/// High-performance S3 bucket content searcher using ripgrep
#[derive(Parser)]
#[command(name = "bucket-scrapper")]
#[command(about = "Search through S3 bucket contents using ripgrep patterns")]
struct Cli {
    /// Path to the config file (optional, for AWS credentials and default buckets)
    #[arg(long, default_value = "config-scrapper.yml")]
    config: PathBuf,

    /// AWS region
    #[arg(short, long, default_value = "eu-west-3")]
    region: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'v', long, default_value = "info")]
    log_level: String,

    /// Log output format (text for human-readable, json for structured)
    #[arg(long, default_value = "text")]
    log_format: LogFormat,

    /// Regex pattern to filter lines (omit to extract all lines)
    #[arg(long)]
    line_pattern_regex: Option<String>,

    /// Regex filter pattern applied to S3 object keys (e.g., "\\.log$", "service-a")
    #[arg(short, long)]
    filter: Option<String>,

    /// Start date in ISO 8601 format (e.g., 2023-01-01T00:00:00Z)
    #[arg(short, long)]
    start: String,

    /// End date in ISO 8601 format (defaults to now)
    #[arg(short, long)]
    end: Option<String>,

    /// Case insensitive search
    #[arg(short, long)]
    ignore_case: bool,

    /// Maximum parallel downloads
    #[arg(long, default_value = "32")]
    max_parallel: usize,

    /// HTTP line channel capacity (max matched lines buffered before compressors)
    #[arg(long, default_value = "1000")]
    http_line_channel_size: usize,

    /// Maximum retry attempts for failed downloads
    #[arg(long, default_value = "10")]
    max_retries: u32,

    /// Initial retry delay in seconds
    #[arg(long, default_value = "2")]
    retry_delay: u64,

    /// Progress report interval in seconds (supports fractional, e.g. 0.5)
    #[arg(long, default_value = "1")]
    progress_interval: f64,

    /// Maximum age of the S3 client in minutes (longer = fewer DNS queries)
    #[arg(long, default_value = "60")]
    client_max_age: u64,

    /// Zstd compression level (1-22, higher = smaller but slower)
    #[arg(long, default_value = "3")]
    compression_level: i32,

    /// Send results to HTTP API instead of writing to files
    #[arg(long)]
    http_output: bool,

    /// HTTP API URL for log ingestion (e.g., https://logs.example.com/api/v1/logs)
    #[arg(long, env = "HTTP_URL")]
    http_url: Option<String>,

    /// Bearer token for HTTP authentication (can also use HTTP_BEARER_AUTH env var)
    #[arg(long, env = "HTTP_BEARER_AUTH")]
    http_bearer_auth: Option<String>,

    /// Maximum batch size in MB for HTTP requests.
    #[arg(long, default_value = "2")]
    http_batch_max_mb: f64,

    /// Timeout for HTTP requests in seconds
    #[arg(long, default_value = "30")]
    http_timeout: u64,

    /// Number of concurrent HTTP upload tasks (default: 4× compressor tasks)
    #[arg(long)]
    http_upload_tasks: Option<usize>,

    /// Number of concurrent HTTP compressor tasks (default: cpu_count / 8, minimum 1)
    #[arg(long)]
    http_compressor_tasks: Option<usize>,

    /// Batch channel buffer between compressors and uploaders (RAM ≈ this × batch_max_bytes)
    #[arg(long, default_value = "4")]
    http_upload_channel_size: usize,

    /// Number of filter worker tasks (default: cpu_count / 2)
    #[arg(long)]
    filter_tasks: Option<usize>,

    /// Line channel capacity between download+decompress and filter workers
    /// (RAM ≈ this × ~200 bytes avg line)
    #[arg(long, default_value = "1000")]
    line_buffer_size: usize,

    /// Memory limit in GB (enforced via setrlimit RLIMIT_AS, 0 = no limit)
    #[arg(long, default_value = "0")]
    memory_limit_gb: u64,

    /// Per-batch submission time threshold in seconds for AIMD upload throttle (0 = disabled)
    #[arg(long, default_value = "4.0")]
    max_submission_time: f64,

    /// AIMD multiplicative decrease factor (0.15 = reduce rate by 15% on congestion)
    #[arg(long, default_value = "0.15")]
    http_aimd_decrease_factor: f64,

    /// AIMD additive increase in MB/s per healthy batch
    #[arg(long, default_value = "1.0")]
    http_aimd_increase: f64,

    /// Global upload rate limit in MB/s (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_upload_rate: f64,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    // Set nice priority to prevent system resource starvation
    #[cfg(unix)]
    {
        unsafe {
            let current_priority = libc::getpriority(libc::PRIO_PROCESS, 0);
            if current_priority < 10 {
                // Set to nice 10 if we're running at higher priority
                if libc::setpriority(libc::PRIO_PROCESS, 0, 10) != 0 {
                    eprintln!("Warning: Could not set nice priority to 10");
                }
            }
        }
    }

    let cli = Cli::parse();

    // Enforce memory limit via RLIMIT_AS (virtual address space)
    #[cfg(unix)]
    if cli.memory_limit_gb > 0 {
        let limit_bytes = cli.memory_limit_gb * 1024 * 1024 * 1024;
        let rlim = libc::rlimit {
            rlim_cur: limit_bytes,
            rlim_max: limit_bytes,
        };
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_AS, &rlim) };
        if rc != 0 {
            eprintln!(
                "Warning: Could not set memory limit to {}GB",
                cli.memory_limit_gb
            );
        }
    }

    // Initialize logging
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    match cli.log_format {
        LogFormat::Json => fmt().with_env_filter(env_filter).with_target(false).json().init(),
        LogFormat::Text => fmt().with_env_filter(env_filter).with_target(false).init(),
    }

    // Set up DNS cache with 5 minute TTL
    dns_cache::init_global_dns_cache(300).await.ok();

    // Load config if file exists
    let config = if cli.config.exists() {
        match load_config(&cli.config) {
            Ok(cfg) => {
                info!(path = %cli.config.display(), "Loaded config");
                Some(cfg)
            }
            Err(e) => {
                info!(path = %cli.config.display(), error = %e, "Could not load config");
                None
            }
        }
    } else {
        info!(
            path = %cli.config.display(),
            "Config file not found, using command line arguments only"
        );
        None
    };

    let end_date = if let Some(end) = cli.end {
        end.parse::<DateTime<Utc>>()
            .context("Invalid end date format")?
    } else {
        Utc::now()
    };

    let start_date = cli
        .start
        .parse::<DateTime<Utc>>()
        .context("Invalid start date format")?;

    // Create S3 client (Arc for sharing across spawned listing tasks)
    let s3_client =
        Arc::new(WrappedS3Client::new(&cli.region, cli.client_max_age, None).await?);

    // Configure search
    let matcher_config = MatcherConfig {
        pattern: cli.line_pattern_regex.clone(),
        ignore_case: cli.ignore_case,
    };

    // Get buckets from config
    let config_buckets: Vec<&BucketConfig> = if let Some(ref cfg) = config {
        cfg.buckets.iter().collect()
    } else {
        eprintln!("Error: No buckets specified. Provide a config file with bucket definitions.");
        std::process::exit(1);
    };

    if config_buckets.is_empty() {
        eprintln!("Error: No buckets to search. Add buckets to your config file.");
        std::process::exit(1);
    }

    for bucket_cfg in &config_buckets {
        if let Err(e) = bucket_cfg.validate() {
            error!("{}", e);
            std::process::exit(1);
        }
    }

    let searcher = Arc::new(LineMatcher::new(matcher_config)?);

    // Configure downloader
    let filter_tasks = cli.filter_tasks.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get() / 2)
            .unwrap_or(2)
            .max(1)
    });

    let download_config = StreamingDownloaderConfig {
        max_concurrent_downloads: cli.max_parallel,
        max_retries: cli.max_retries,
        initial_retry_delay: Duration::from_secs(cli.retry_delay),
        progress_interval: Duration::from_secs_f64(cli.progress_interval),
        filter_tasks,
        line_buffer_size: cli.line_buffer_size,
    };

    let downloader = StreamingDownloader::new(s3_client.get_client().await?, download_config);

    // Check if we're using HTTP streaming output
    let http_streaming = if cli.http_output {
        // Validate HTTP config early
        let api_url = cli.http_url.clone()
            .or_else(|| config.as_ref().and_then(|c| c.http_output.as_ref().map(|h| h.url.clone())))
            .ok_or_else(|| anyhow::anyhow!(
                "HTTP output enabled but no URL specified. Use --http-url or set HTTP_URL env var"
            ))?;

        let bearer_token = cli.http_bearer_auth.clone()
            .or_else(|| config.as_ref().and_then(|c| c.http_output.as_ref().and_then(|h| h.bearer_auth.clone())));

        let timeout_secs = config.as_ref()
            .and_then(|c| c.http_output.as_ref().map(|h| h.timeout_secs))
            .unwrap_or(cli.http_timeout);

        let batch_max_bytes = (cli.http_batch_max_mb * 1024.0 * 1024.0) as usize;

        let num_compressor_tasks = cli.http_compressor_tasks.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get() / 8)
                .unwrap_or(1)
                .max(1)
        });
        let num_upload_tasks = cli.http_upload_tasks.unwrap_or(4 * num_compressor_tasks);

        let max_submission_time = if cli.max_submission_time > 0.0 {
            Some(Duration::from_secs_f64(cli.max_submission_time))
        } else {
            None
        };

        let max_upload_rate = if cli.max_upload_rate > 0.0 {
            Some(cli.max_upload_rate * 1_000_000.0)
        } else {
            None
        };

        info!(
            url = %api_url,
            batch_max_mb = cli.http_batch_max_mb,
            compressor_tasks = num_compressor_tasks,
            upload_tasks = num_upload_tasks,
            upload_channel_size = cli.http_upload_channel_size,
            max_submission_time_s = cli.max_submission_time,
            aimd_decrease_factor = cli.http_aimd_decrease_factor,
            aimd_increase_mbps = cli.http_aimd_increase,
            max_upload_rate_mbps = cli.max_upload_rate,
            "HTTP streaming mode enabled"
        );

        let http_config = HttpWriterConfig {
            url: api_url,
            bearer_token,
            batch_max_bytes,
            timeout_secs,
            max_retries: cli.max_retries.min(10),
            channel_buffer_size: cli.http_line_channel_size,
            num_compressor_tasks,
            num_upload_tasks,
            upload_channel_size: cli.http_upload_channel_size,
            compression_level: cli.compression_level,
            max_submission_time,
            max_upload_rate,
            aimd_decrease_factor: cli.http_aimd_decrease_factor,
            aimd_increase_bytes: cli.http_aimd_increase * 1_000_000.0,
        };

        Some(HttpResultWriter::new(http_config)?)
    } else {
        None
    };

    // List all objects in parallel across all buckets × hourly prefixes
    let all_bucket_objects = {
        let date_hours = date_range_to_date_hour_list(&start_date, &end_date)?;
        let semaphore = Arc::new(Semaphore::new(cli.max_parallel));
        let mut join_set: JoinSet<Result<Vec<S3ObjectInfo>>> = JoinSet::new();
        let mut total_tasks = 0usize;

        for bucket_cfg in &config_buckets {
            let formatter = generate_path_formatter(bucket_cfg);
            for dh in &date_hours {
                let prefix = formatter(&dh.date, &dh.hour)?;
                let bucket = bucket_cfg.bucket.clone();
                let filter = cli.filter.clone();
                let client = Arc::clone(&s3_client);
                let sem = Arc::clone(&semaphore);

                join_set.spawn(async move {
                    let _permit = sem.acquire().await
                        .map_err(|e| anyhow::anyhow!("semaphore closed: {e}"))?;

                    debug!(bucket = %bucket, prefix = %prefix, "Listing");
                    let result = client
                        .get_matching_filenames_from_s3(&bucket, &prefix, filter.as_deref())
                        .await;

                    match &result {
                        Ok(objs) if !objs.is_empty() => {
                            debug!(objects = objs.len(), bucket = %bucket, prefix = %prefix, "Found objects");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(bucket = %bucket, prefix = %prefix, error = %e, "Failed to list");
                        }
                    }
                    result
                });
                total_tasks += 1;
            }
        }

        info!(tasks = total_tasks, buckets = config_buckets.len(), "Spawned listing tasks");

        // Drain results — abort all remaining tasks on first failure
        let mut all_objects = Vec::new();
        let mut successful = 0usize;

        let listing_start = std::time::Instant::now();
        let mut last_report = listing_start;

        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(Ok(objects)) => {
                    successful += 1;
                    all_objects.extend(objects);
                }
                Ok(Err(e)) => {
                    join_set.abort_all();
                    return Err(e.context("Prefix listing failed, aborting"));
                }
                Err(e) => {
                    join_set.abort_all();
                    return Err(anyhow::anyhow!("Listing task panicked: {e}"));
                }
            }

            if successful < total_tasks && last_report.elapsed() >= std::time::Duration::from_secs(5) {
                last_report = std::time::Instant::now();
                info!(
                    prefixes_done = successful,
                    prefixes_total = total_tasks,
                    elapsed_s = listing_start.elapsed().as_secs_f32(),
                    objects = all_objects.len(),
                    "Listing progress"
                );
            }
        }

        info!(
            prefixes_ok = successful,
            prefixes_total = total_tasks,
            objects = all_objects.len(),
            elapsed_s = listing_start.elapsed().as_secs_f32(),
            "Listing complete"
        );

        // Sort by size for better load balancing
        all_objects.sort_by_key(|o| o.size);
        all_objects
    };

    if all_bucket_objects.is_empty() {
        anyhow::bail!("No objects found to search");
    } else {
        let total_compressed_input: usize = all_bucket_objects.iter().map(|o| o.size).sum();
        info!(
            objects = all_bucket_objects.len(),
            mb = total_compressed_input / 1_000_000,
            "Processing objects"
        );

        let batch_start = std::time::Instant::now();

        if let Some(http_writer) = http_streaming {
            let http_sender = http_writer.get_sender();
            let observer = http_writer.observer();
            let fatal_error = http_writer.fatal_error_flag();
            let (files_searched, matched_lines) = downloader
                .search_objects_to_http(&all_bucket_objects, searcher.clone(), http_sender, observer, fatal_error)
                .await?;

            let api_url = http_writer.url().to_string();
            let stats = http_writer.finish().await?;

            if stats.lines_dropped > 0 {
                warn!(
                    lines_dropped = stats.lines_dropped,
                    "Some lines were dropped due to HTTP send failures"
                );
            }

            let elapsed = batch_start.elapsed().as_secs_f64();
            let read_compressed_mb = total_compressed_input as f64 / 1_000_000.0;
            let wrote_compressed_mb = stats.compressed_bytes_sent as f64 / 1_000_000.0;
            let plaintext_mb = stats.plaintext_bytes_sent as f64 / 1_000_000.0;
            info!(
                elapsed_s = elapsed,
                files = files_searched,
                matched_lines = matched_lines,
                lines_sent = stats.lines_sent,
                lines_dropped = stats.lines_dropped,
                read_compressed_mb = read_compressed_mb,
                wrote_compressed_mb = wrote_compressed_mb,
                plaintext_mb = plaintext_mb,
                compression_ratio = if wrote_compressed_mb > 0.0 { plaintext_mb / wrote_compressed_mb } else { 0.0 },
                pattern = cli.line_pattern_regex.as_deref().unwrap_or("(all lines)"),
                url = %api_url,
                "Search completed"
            );
        } else {
            let output_dir = config
                .as_ref()
                .and_then(|c| c.output_dir.clone())
                .unwrap_or_else(|| "./scrapper-output".to_string());

            let file_writer =
                SharedFileWriter::new(output_dir.clone(), cli.compression_level)?;

            let (files_searched, matched_lines) = downloader
                .search_objects_to_file(
                    &all_bucket_objects,
                    searcher.clone(),
                    file_writer.clone(),
                )
                .await?;

            let stats = file_writer.finish()?;
            let elapsed = batch_start.elapsed().as_secs_f64();
            let plaintext_mb = stats.plaintext_bytes as f64 / 1_000_000.0;
            let compressed_mb = stats.compressed_bytes as f64 / 1_000_000.0;

            info!(
                elapsed_s = elapsed,
                files = files_searched,
                matched_lines = matched_lines,
                output_files = stats.files_written,
                plaintext_mb = plaintext_mb,
                compressed_mb = compressed_mb,
                plaintext_mbps = plaintext_mb / elapsed,
                compressed_mbps = compressed_mb / elapsed,
                compression_ratio = if compressed_mb > 0.0 { plaintext_mb / compressed_mb } else { 0.0 },
                pattern = cli.line_pattern_regex.as_deref().unwrap_or("(all lines)"),
                output_dir = %output_dir,
                "Search completed"
            );
        }
    }

    Ok(())
}

