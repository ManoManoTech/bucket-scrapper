// src/main.rs
mod config;
mod s3;
mod search;
mod utils;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use s3::{StreamingDownloader, StreamingDownloaderConfig};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::loader::load_config;
use crate::config::types::BucketConfig;
use crate::s3::client::WrappedS3Client;
use crate::s3::dns_cache;
use crate::s3::ParallelLister;
use crate::search::{
    HttpMatchToSend, HttpResultWriter, HttpWriterConfig, SearchConfig, StreamSearcher,
};
use crate::utils::date::date_range_to_date_hour_list;

/// High-performance S3 bucket content searcher using ripgrep
#[derive(Parser)]
#[command(name = "bucket-scrapper")]
#[command(about = "Search through S3 bucket contents using ripgrep patterns")]
struct Cli {
    /// Path to the config file (optional, for AWS credentials and default buckets)
    #[arg(short, long, global = true, default_value = "config-scrapper.yml")]
    config: PathBuf,

    /// AWS region
    #[arg(short, long, global = true, default_value = "eu-west-3")]
    region: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'v', long, global = true, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Search through S3 bucket contents
    Search {
        /// Regex pattern to search for
        #[arg(short, long)]
        pattern: String,

        /// Regex filter pattern applied to S3 object keys (e.g., "\\.log$", "service-a")
        #[arg(short, long)]
        filter: Option<String>,

        /// Start date in ISO 8601 format (e.g., 2023-01-01T00:00:00Z)
        #[arg(short, long)]
        start: Option<String>,

        /// End date in ISO 8601 format (defaults to now)
        #[arg(short, long)]
        end: Option<String>,

        /// Lines of context to show around matches
        #[arg(short = 'C', long, default_value = "0")]
        context: usize,

        /// Case insensitive search
        #[arg(short, long)]
        ignore_case: bool,

        /// Only show match counts per file
        #[arg(short = 'c', long)]
        count: bool,

        /// Maximum parallel downloads
        #[arg(long, default_value = "32")]
        max_parallel: usize,

        /// Stream buffer size in KB
        #[arg(long, default_value = "64")]
        buffer_size_kb: usize,

        /// Channel buffer size for match backpressure (max matches buffered in memory)
        #[arg(long, default_value = "1000")]
        channel_buffer: usize,

        /// Maximum retry attempts for failed downloads
        #[arg(long, default_value = "10")]
        max_retries: u32,

        /// Initial retry delay in seconds
        #[arg(long, default_value = "2")]
        retry_delay: u64,

        /// Maximum age of the S3 client in minutes (longer = fewer DNS queries)
        #[arg(long, default_value = "60")]
        client_max_age: u64,

        /// Output format (json, text, or quiet)
        #[arg(long, default_value = "text")]
        output: OutputFormat,

        /// Send results to HTTP API instead of writing to files
        #[arg(long)]
        http_output: bool,

        /// HTTP API URL for log ingestion (e.g., https://intake.handy-mango.http.com/api/v1/logs)
        #[arg(long, env = "HTTP_URL")]
        http_url: Option<String>,

        /// API key for HTTP authentication (can also use HTTP_BEARER_AUTH env var)
        #[arg(long, env = "HTTP_BEARER_AUTH")]
        http_api_key: Option<String>,

        /// Maximum batch size in MB for HTTP requests.
        #[arg(long, default_value = "2")]
        http_batch_max_mb: usize,

        /// Timeout for HTTP requests in seconds
        #[arg(long, default_value = "30")]
        http_timeout: u64,

        /// Hostname to include in log entries (defaults to machine hostname)
        #[arg(long, env = "HTTP_HOSTNAME")]
        http_hostname: Option<String>,

        /// Service name to include in log entries
        #[arg(long, env = "HTTP_SERVICE", default_value = "bucket-scrapper")]
        http_service: String,
    },

    /// List objects in buckets matching the filter
    List {
        /// Regex filter pattern applied to S3 object keys (e.g., "\\.log$", "service-a")
        #[arg(short, long)]
        filter: Option<String>,

        /// Start date in ISO 8601 format
        #[arg(short, long)]
        start: Option<String>,

        /// End date in ISO 8601 format (defaults to now)
        #[arg(short, long)]
        end: Option<String>,
    },
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Quiet,
}

#[tokio::main]
async fn main() -> Result<()> {
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

    // Initialize logging
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    fmt().with_env_filter(env_filter).with_target(false).init();

    // Set up DNS cache with 5 minute TTL
    dns_cache::init_global_dns_cache(300).await.ok();

    // Load config if file exists
    let config = if cli.config.exists() {
        match load_config(&cli.config) {
            Ok(cfg) => {
                info!("Loaded config from {}", cli.config.display());
                Some(cfg)
            }
            Err(e) => {
                info!("Could not load config from {}: {}", cli.config.display(), e);
                None
            }
        }
    } else {
        info!(
            "Config file {} not found, using command line arguments only",
            cli.config.display()
        );
        None
    };

    match cli.command {
        Commands::Search {
            pattern,
            filter,
            start,
            end,
            context: _,
            ignore_case,
            count,
            max_parallel,
            buffer_size_kb,
            channel_buffer,
            max_retries,
            retry_delay,
            client_max_age,
            output,
            http_output,
            http_url,
            http_api_key,
            http_batch_max_mb,
            http_timeout,
            http_hostname,
            http_service,
        } => {
            let end_date = if let Some(end) = end {
                end.parse::<DateTime<Utc>>()
                    .context("Invalid end date format")?
            } else {
                Utc::now()
            };

            let start_date = if let Some(start) = start {
                Some(
                    start
                        .parse::<DateTime<Utc>>()
                        .context("Invalid start date format")?,
                )
            } else {
                None
            };

            // Create S3 client
            let mut s3_client = WrappedS3Client::new(&cli.region, client_max_age, None).await?;

            // Configure search
            let search_config = SearchConfig {
                pattern: pattern.clone(),
                ignore_case,
                count_only: count,
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

            let searcher = Arc::new(StreamSearcher::new(search_config)?);

            // Configure downloader
            let download_config = StreamingDownloaderConfig {
                max_concurrent_downloads: max_parallel,
                buffer_size_bytes: buffer_size_kb * 1024,
                channel_buffer_size: channel_buffer,
                max_retries,
                initial_retry_delay: Duration::from_secs(retry_delay),
            };

            let downloader =
                StreamingDownloader::new(s3_client.get_client().await?, download_config);

            // Create parallel lister for efficient prefix listing
            let parallel_lister = ParallelLister::new(max_parallel);

            // Check if we're using HTTP streaming output
            let http_streaming = if http_output {
                // Validate HTTP config early
                let api_url = http_url.clone()
                    .or_else(|| config.as_ref().and_then(|c| c.http_output.as_ref().map(|h| h.url.clone())))
                    .ok_or_else(|| anyhow::anyhow!(
                        "HTTP output enabled but no URL specified. Use --http-url or set HTTP_URL env var"
                    ))?;

                let api_key = http_api_key.clone()
                    .or_else(|| config.as_ref().and_then(|c| c.http_output.as_ref().and_then(|h| h.api_key.clone())))
                    .ok_or_else(|| anyhow::anyhow!(
                        "HTTP output enabled but no API key specified. Use --http-api-key or set HTTP_BEARER_AUTH env var"
                    ))?;

                let timeout_secs = config.as_ref()
                    .and_then(|c| c.http_output.as_ref().map(|h| h.timeout_secs))
                    .unwrap_or(http_timeout);

                let batch_max_bytes = http_batch_max_mb * 1024 * 1024;

                info!("HTTP streaming mode enabled - results will be sent to: {} (max batch: {} MB)", api_url, http_batch_max_mb);

                let hostname = http_hostname.clone().unwrap_or_else(|| {
                    gethostname::gethostname().to_string_lossy().to_string()
                });

                let http_config = HttpWriterConfig {
                    url: api_url,
                    api_key,
                    batch_max_bytes,
                    timeout_secs,
                    max_retries,
                    hostname,
                    service: http_service.clone(),
                    channel_buffer_size: channel_buffer,
                };

                Some(HttpResultWriter::new(http_config)?)
            } else {
                None
            };

            // Track stats for both modes
            let mut total_files_searched = 0usize;
            let mut total_matches = 0usize;

            use crate::utils::path_formatter::generate_path_formatter;

            // Process all buckets
            let all_bucket_objects = {
                let mut all_objects = Vec::new();

                for bucket_cfg in &config_buckets {
                    info!("Listing bucket: {}", bucket_cfg.bucket);

                    let objects = if let Some(start) = start_date {
                        let date_hours = date_range_to_date_hour_list(&start, &end_date)?;
                        let formatter = generate_path_formatter(bucket_cfg);
                        let mut prefixes = Vec::new();
                        for dh in &date_hours {
                            prefixes.push(formatter(&dh.date, &dh.hour)?);
                        }
                        parallel_lister
                            .list_prefixes_parallel(&s3_client, &bucket_cfg.bucket, prefixes, filter.as_deref())
                            .await?
                    } else {
                        let prefix = if bucket_cfg.path.is_empty() {
                            String::new()
                        } else {
                            let formatter = generate_path_formatter(bucket_cfg);
                            formatter(&String::new(), &String::new())?
                        };
                        s3_client
                            .get_matching_filenames_from_s3(&bucket_cfg.bucket, &prefix, filter.as_deref())
                            .await?
                    };

                    all_objects.extend(objects);
                }

                // Sort by size for better load balancing
                all_objects.sort_by_key(|o| o.size);
                all_objects
            };

            if all_bucket_objects.is_empty() {
                info!("No objects found to search");
            } else {
                let total_size: usize = all_bucket_objects.iter().map(|o| o.size).sum();
                info!(
                    "Processing {} total objects ({} MB)",
                    all_bucket_objects.len(),
                    total_size / 1_000_000
                );

                let batch_start = std::time::Instant::now();

                if let Some(ref http_writer) = http_streaming {
                    // HTTP streaming mode - stream results directly to API
                    let http_sender = http_writer.get_sender();
                    let (files, matches) = downloader
                        .search_objects_to_http(&all_bucket_objects, searcher.clone(), http_sender)
                        .await?;
                    total_files_searched = files;
                    total_matches = matches;
                } else {
                    // File output mode - stream results through bounded channel to file writer
                    let output_dir = config
                        .as_ref()
                        .and_then(|c| c.output_dir.clone())
                        .unwrap_or_else(|| "./scrapper-output".to_string());
                    std::fs::create_dir_all(&output_dir)?;

                    let (file_tx, file_rx) = tokio::sync::mpsc::channel::<HttpMatchToSend>(channel_buffer);

                    let file_writer_handle =
                        tokio::spawn(file_writer_task(file_rx, output_dir.clone()));

                    let (files, matches) = downloader
                        .search_objects_to_http(
                            &all_bucket_objects,
                            searcher.clone(),
                            file_tx,
                        )
                        .await?;
                    total_files_searched = files;
                    total_matches = matches;

                    // Wait for file writer to finish flushing
                    let files_written = file_writer_handle.await??;
                    info!(
                        "File output complete: {} files written to {}",
                        files_written, output_dir
                    );
                }

                info!(
                    "Search completed in {:.1}s",
                    batch_start.elapsed().as_secs_f32()
                );
            }

            // Determine output mode and finalize
            let (output_count, output_destination) = if let Some(http_writer) = http_streaming {
                // Finish HTTP writer and get final count
                let api_url = http_writer.url().to_string();
                let lines_sent = http_writer.finish().await?;
                info!("HTTP streaming complete: {} lines sent to {}", lines_sent, api_url);

                (lines_sent, api_url)
            } else {
                // File output mode - results already written by file_writer_task
                let output_dir = config
                    .as_ref()
                    .and_then(|c| c.output_dir.clone())
                    .unwrap_or_else(|| "./scrapper-output".to_string());

                (total_matches, output_dir)
            };

            // Print summary
            match output {
                OutputFormat::Json => {
                    let summary = if http_output {
                        serde_json::json!({
                            "pattern": pattern,
                            "files_searched": total_files_searched,
                            "total_matches": total_matches,
                            "lines_sent": output_count,
                            "http_url": output_destination
                        })
                    } else {
                        serde_json::json!({
                            "pattern": pattern,
                            "files_searched": total_files_searched,
                            "total_matches": total_matches,
                            "output_files_written": output_count,
                            "output_directory": output_destination
                        })
                    };
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                }
                OutputFormat::Text | OutputFormat::Quiet => {
                    println!("\n=== Search Complete ===");
                    println!("Pattern: {}", pattern);
                    println!("Files searched: {}", total_files_searched);
                    println!("Total matches: {}", total_matches);
                    if http_output {
                        println!("Lines sent: {}", output_count);
                        println!("HTTP URL: {}", output_destination);
                    } else {
                        println!("Output files written: {}", output_count);
                        println!("Output directory: {}", output_destination);
                    }
                }
            }
        }
        Commands::List {
            filter,
            start,
            end,
        } => {
            let end_date = if let Some(end) = end {
                end.parse::<DateTime<Utc>>()
                    .context("Invalid end date format")?
            } else {
                Utc::now()
            };

            let start_date = if let Some(start) = start {
                Some(
                    start
                        .parse::<DateTime<Utc>>()
                        .context("Invalid start date format")?,
                )
            } else {
                None
            };

            // Get buckets from config
            let config_buckets: Vec<&BucketConfig> = if let Some(ref cfg) = config {
                cfg.buckets.iter().collect()
            } else {
                eprintln!("Error: No buckets specified. Provide a config file with bucket definitions.");
                std::process::exit(1);
            };

            if config_buckets.is_empty() {
                eprintln!("Error: No buckets to list. Add buckets to your config file.");
                std::process::exit(1);
            }

            // Create S3 client
            let mut s3_client = WrappedS3Client::new(&cli.region, 60, None).await?;

            // Create parallel lister for efficient prefix listing (use default max_parallel of 32)
            let parallel_lister = ParallelLister::new(32);

            use crate::utils::path_formatter::generate_path_formatter;
            for bucket_cfg in &config_buckets {
                println!("\nBucket: {}", bucket_cfg.bucket);

                if let Some(start) = start_date {
                    // Use path formatter for date-based paths
                    let date_hours = date_range_to_date_hour_list(&start, &end_date)?;
                    let formatter = generate_path_formatter(bucket_cfg);

                    // Build all prefixes for parallel listing
                    let mut prefixes = Vec::new();
                    for dh in &date_hours {
                        prefixes.push(formatter(&dh.date, &dh.hour)?);
                    }

                    // Use parallel listing
                    let all_objects = parallel_lister
                        .list_prefixes_parallel(
                            &s3_client,
                            &bucket_cfg.bucket,
                            prefixes,
                            filter.as_deref(),
                        )
                        .await?;

                    if all_objects.is_empty() {
                        println!("  No matching objects found");
                    } else {
                        println!("  Found {} objects:", all_objects.len());
                        for obj in all_objects.iter().take(100) {
                            println!("    {} ({} bytes)", obj.key, obj.size);
                        }
                        if all_objects.len() > 100 {
                            println!("    ... and {} more", all_objects.len() - 100);
                        }
                    }
                } else {
                    // No date range, use static prefix or empty
                    let prefix = if bucket_cfg.path.is_empty() {
                        String::new()
                    } else {
                        let formatter = generate_path_formatter(bucket_cfg);
                        let empty_date = String::new();
                        let empty_hour = String::new();
                        formatter(&empty_date, &empty_hour)?
                    };

                    let objects = s3_client
                        .get_matching_filenames_from_s3(
                            &bucket_cfg.bucket,
                            &prefix,
                            filter.as_deref(),
                        )
                        .await?;

                    if objects.is_empty() {
                        println!("  No matching objects found");
                    } else {
                        println!("  Found {} objects:", objects.len());
                        for obj in objects.iter().take(100) {
                            println!("    {} ({} bytes)", obj.key, obj.size);
                        }
                        if objects.len() > 100 {
                            println!("    ... and {} more", objects.len() - 100);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Background task that receives search matches from a bounded channel
/// and writes them to gzip-compressed output files grouped by date/hour prefix.
async fn file_writer_task(
    mut rx: tokio::sync::mpsc::Receiver<HttpMatchToSend>,
    output_dir: String,
) -> anyhow::Result<usize> {
    use crate::search::extract_prefix;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::collections::HashMap;
    use std::io::Write;

    let mut encoders: HashMap<String, GzEncoder<std::fs::File>> = HashMap::new();
    let mut files_written = 0usize;

    while let Some(match_item) = rx.recv().await {
        let prefix = extract_prefix(&match_item.key);

        let encoder = match encoders.get_mut(&prefix) {
            Some(enc) => enc,
            None => {
                let output_file = format!("{}/{}.gz", output_dir, prefix);
                let file = std::fs::File::create(&output_file)?;
                let enc = GzEncoder::new(file, Compression::default());
                encoders.insert(prefix.clone(), enc);
                files_written += 1;
                encoders.get_mut(&prefix).unwrap()
            }
        };

        let output_line = serde_json::json!({
            "file": format!("{}/{}", match_item.bucket, match_item.key),
            "content": match_item.content.trim()
        });
        writeln!(encoder, "{}", output_line)?;
    }

    // Finish all encoders
    for (prefix, encoder) in encoders {
        encoder.finish().map_err(|e| {
            anyhow::anyhow!("Failed to finish gzip encoder for {}: {}", prefix, e)
        })?;
        info!("Finished writing {}.gz", prefix);
    }

    Ok(files_written)
}
