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
use tracing::{debug, info};
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::loader::load_config;
use crate::config::types::BucketConfig;
use crate::s3::client::WrappedS3Client;
use crate::s3::dns_cache;
use crate::s3::{StreamingSearchConfig, StreamingSearchExecutor};
use crate::search::{
    SearchConfig, SearchResultCollector, StreamSearcher, StreamingSearchCollector,
};
use crate::utils::date::{date_range_to_date_hour_list, DateHour};

/// High-performance S3 bucket content searcher using ripgrep
#[derive(Parser)]
#[command(name = "bucket-scrapper")]
#[command(about = "Search through S3 bucket contents using ripgrep patterns")]
struct Cli {
    /// Path to the config file (optional, for AWS credentials and default buckets)
    #[arg(short, long, default_value = "config-scrapper.yml")]
    config: PathBuf,

    /// AWS region
    #[arg(short, long, default_value = "eu-west-3")]
    region: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'v', long, default_value = "info")]
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

        /// S3 buckets to search (can specify multiple, uses config if not provided)
        #[arg(short, long, value_delimiter = ',')]
        buckets: Option<Vec<String>>,

        /// Object key filter pattern (e.g., "*.log", "2024/*")
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

        /// Maximum matches per file to return
        #[arg(long)]
        max_matches_per_file: Option<usize>,

        /// Maximum parallel downloads
        #[arg(long, default_value = "32")]
        max_parallel: usize,

        /// Number of processing threads (defaults to CPU count)
        #[arg(long)]
        process_threads: Option<usize>,

        /// Memory pool size in MB
        #[arg(long, default_value = "2048")]
        memory_pool_mb: usize,

        /// Stream buffer size in KB
        #[arg(long, default_value = "64")]
        buffer_size_kb: usize,

        /// Channel buffer size for backpressure control
        #[arg(long, default_value = "100")]
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

        /// Use streaming output (reduces memory usage)
        #[arg(long, default_value = "true")]
        streaming: bool,
    },

    /// List objects in buckets matching the filter
    List {
        /// S3 buckets to list (can specify multiple, uses config if not provided)
        #[arg(short, long, value_delimiter = ',')]
        buckets: Option<Vec<String>>,

        /// Object key filter pattern
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
            buckets,
            filter,
            start,
            end,
            context,
            ignore_case,
            count,
            max_matches_per_file,
            max_parallel,
            process_threads,
            memory_pool_mb: _,
            buffer_size_kb,
            channel_buffer,
            max_retries,
            retry_delay,
            client_max_age,
            output,
            streaming,
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

            // Get buckets from command line or config
            let (simple_buckets, config_buckets): (Vec<String>, Vec<&BucketConfig>) =
                if let Some(b) = buckets {
                    // Command line buckets are simple strings
                    (b, vec![])
                } else if let Some(ref cfg) = config {
                    // Config buckets have path formatting
                    (vec![], cfg.buckets.iter().collect())
                } else {
                    eprintln!("Error: No buckets specified. Use -b flag or provide config file.");
                    std::process::exit(1);
                };

            if simple_buckets.is_empty() && config_buckets.is_empty() {
                eprintln!("Error: No buckets to search.");
                std::process::exit(1);
            }

            let searcher = Arc::new(StreamSearcher::new(search_config)?);

            // Determine output directory
            let output_dir = config
                .as_ref()
                .and_then(|c| c.output_dir.clone())
                .unwrap_or_else(|| "./scrapper-output".to_string());

            // Create collector based on mode
            let collector = Arc::new(tokio::sync::Mutex::new(SearchResultCollector::new()));

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

            // Process simple buckets (from command line)
            for bucket in &simple_buckets {
                info!("Searching bucket: {}", bucket);

                // Get objects from S3
                let mut all_objects = if let Some(start) = start_date {
                    let date_hours = date_range_to_date_hour_list(&start, &end_date)?;

                    let mut objects = Vec::new();
                    for dh in date_hours {
                        let prefix = format!("{}/{}", dh.date, dh.hour);
                        let objs = s3_client
                            .get_matching_filenames_from_s3(bucket, &prefix, filter.as_deref())
                            .await?;

                        if !objs.is_empty() {
                            info!("Found {} objects in prefix {}", objs.len(), prefix);
                            objects.extend(objs);
                        }
                    }
                    objects
                } else {
                    s3_client
                        .get_matching_filenames_from_s3(bucket, "", filter.as_deref())
                        .await?
                };

                if all_objects.is_empty() {
                    info!("No matching objects found in {}", bucket);
                    continue;
                }

                // Sort by size for better load balancing (small files first)
                all_objects.sort_by_key(|o| o.size);

                let total_size: usize = all_objects.iter().map(|o| o.size).sum();
                info!(
                    "Processing {} objects ({} MB) in {}",
                    all_objects.len(),
                    total_size / 1_000_000,
                    bucket
                );

                let batch_start = std::time::Instant::now();

                // Search through objects
                downloader
                    .search_objects(&all_objects, searcher.clone(), collector.clone())
                    .await?;

                info!(
                    "Completed bucket {} in {:.1}s",
                    bucket,
                    batch_start.elapsed().as_secs_f32()
                );
            }

            // Process config buckets (with path formatting)
            use crate::utils::path_formatter::generate_path_formatter;
            for bucket_cfg in &config_buckets {
                info!("Searching bucket: {}", bucket_cfg.bucket);

                if let Some(start) = start_date {
                    // Use path formatter for date-based paths
                    let date_hours = date_range_to_date_hour_list(&start, &end_date)?;
                    let formatter = generate_path_formatter(bucket_cfg);

                    // Collect all objects across all prefixes first
                    let mut all_objects = Vec::new();
                    let mut total_size = 0usize;

                    for dh in date_hours {
                        let prefix = formatter(&dh.date, &dh.hour)?;
                        let objs = s3_client
                            .get_matching_filenames_from_s3(
                                &bucket_cfg.bucket,
                                &prefix,
                                filter.as_deref(),
                            )
                            .await?;

                        if objs.is_empty() {
                            debug!("No objects found for prefix: {}", prefix);
                            continue;
                        }

                        let prefix_size: usize = objs.iter().map(|o| o.size).sum();
                        info!(
                            "Found {} objects in prefix {}: {} MB",
                            objs.len(),
                            prefix,
                            prefix_size / 1_000_000
                        );

                        total_size += prefix_size;
                        all_objects.extend(objs);
                    }

                    if all_objects.is_empty() {
                        info!("No objects found across all prefixes");
                        continue;
                    }

                    // Sort by size for better load balancing (small files first)
                    all_objects.sort_by_key(|o| o.size);

                    info!(
                        "Processing {} total objects ({} MB) across all prefixes",
                        all_objects.len(),
                        total_size / 1_000_000
                    );

                    let batch_start = std::time::Instant::now();
                    let matches_before = collector.lock().await.match_count();

                    // Process all objects in one batch for maximum parallelism
                    downloader
                        .search_objects(&all_objects, searcher.clone(), collector.clone())
                        .await?;

                    let matches_after = collector.lock().await.match_count();
                    let matches_found = matches_after - matches_before;

                    info!(
                        "Completed all prefixes: {} objects, {} matches found in {:.1}s",
                        all_objects.len(),
                        matches_found,
                        batch_start.elapsed().as_secs_f32()
                    );
                } else {
                    // No date range, just use static prefix if any
                    let prefix = if bucket_cfg.path.is_empty() {
                        String::new()
                    } else {
                        // Generate prefix without date formatting
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
                        info!("No matching objects found in {}", bucket_cfg.bucket);
                        continue;
                    }

                    info!("Found {} objects to search", objects.len());
                    downloader
                        .search_objects(&objects, searcher.clone(), collector.clone())
                        .await?;
                }
            }

            // Get and display results
            let final_collector = Arc::try_unwrap(collector)
                .map_err(|_| anyhow::anyhow!("Failed to get collector"))?
                .into_inner();

            let results = final_collector.into_result();

            // Determine output directory
            let output_dir = config
                .as_ref()
                .and_then(|c| c.output_dir.clone())
                .unwrap_or_else(|| "./scrapper-output".to_string());

            // Create output directory if it doesn't exist
            std::fs::create_dir_all(&output_dir)?;

            info!("Writing results to {}", output_dir);

            // Write results grouped by date/hour
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;

            let mut files_written = 0;
            for (prefix, matches) in &results.matches_by_prefix {
                if matches.is_empty() {
                    continue;
                }

                // Create output file like "20240115H10.gz"
                let output_file = format!("{}/{}.gz", output_dir, prefix);
                let file = std::fs::File::create(&output_file)?;
                let mut encoder = GzEncoder::new(file, Compression::default());

                // Write matches as JSON lines (one per line for easy streaming)
                for match_item in matches {
                    // Write just the line content with metadata as JSON
                    let output_line = serde_json::json!({
                        "file": format!("{}/{}", match_item.bucket, match_item.key),
                        "line": match_item.line_number,
                        "content": match_item.line_content.trim()
                    });
                    writeln!(encoder, "{}", output_line)?;
                }

                encoder.finish()?;
                files_written += 1;
                info!("Wrote {} matches to {}", matches.len(), output_file);
            }

            // Print summary
            match output {
                OutputFormat::Json => {
                    let summary = serde_json::json!({
                        "pattern": pattern,
                        "files_searched": results.files_searched,
                        "files_with_matches": results.files_with_matches,
                        "total_matches": results.total_matches,
                        "output_files_written": files_written,
                        "output_directory": output_dir
                    });
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                }
                OutputFormat::Text | OutputFormat::Quiet => {
                    println!("\n=== Search Complete ===");
                    println!("Pattern: {}", pattern);
                    println!("Files searched: {}", results.files_searched);
                    println!("Files with matches: {}", results.files_with_matches);
                    println!("Total matches: {}", results.total_matches);
                    println!("Output files written: {}", files_written);
                    println!("Output directory: {}", output_dir);

                    if count && !results.file_counts.is_empty() {
                        println!("\n--- Match Counts by File ---");
                        let mut counts: Vec<_> = results.file_counts.iter().collect();
                        counts.sort_by(|a, b| b.1.cmp(a.1));
                        for (file, count) in counts.iter().take(10) {
                            println!("  {}: {}", file, count);
                        }
                        if counts.len() > 10 {
                            println!("  ... and {} more files", counts.len() - 10);
                        }
                    }
                }
            }
        }
        Commands::List {
            buckets,
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

            // Get buckets from command line or config
            let (simple_buckets, config_buckets): (Vec<String>, Vec<&BucketConfig>) =
                if let Some(b) = buckets {
                    (b, vec![])
                } else if let Some(ref cfg) = config {
                    (vec![], cfg.buckets.iter().collect())
                } else {
                    eprintln!("Error: No buckets specified. Use -b flag or provide config file.");
                    std::process::exit(1);
                };

            if simple_buckets.is_empty() && config_buckets.is_empty() {
                eprintln!("Error: No buckets to list.");
                std::process::exit(1);
            }

            // Create S3 client
            let mut s3_client = WrappedS3Client::new(&cli.region, 60, None).await?;

            // List objects in simple buckets
            for bucket in &simple_buckets {
                println!("\nBucket: {}", bucket);

                let objects = if let Some(start) = start_date {
                    let date_hours = date_range_to_date_hour_list(&start, &end_date)?;

                    let mut all_objects = Vec::new();
                    for dh in date_hours {
                        let prefix = format!("{}/{}", dh.date, dh.hour);
                        let objs = s3_client
                            .get_matching_filenames_from_s3(bucket, &prefix, filter.as_deref())
                            .await?;
                        all_objects.extend(objs);
                    }
                    all_objects
                } else {
                    s3_client
                        .get_matching_filenames_from_s3(bucket, "", filter.as_deref())
                        .await?
                };

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

            // List objects in config buckets (with path formatting)
            use crate::utils::path_formatter::generate_path_formatter;
            for bucket_cfg in &config_buckets {
                println!("\nBucket: {} (configured)", bucket_cfg.bucket);

                if let Some(start) = start_date {
                    // Use path formatter for date-based paths
                    let date_hours = date_range_to_date_hour_list(&start, &end_date)?;
                    let formatter = generate_path_formatter(bucket_cfg);

                    let mut all_objects = Vec::new();
                    for dh in date_hours {
                        let prefix = formatter(&dh.date, &dh.hour)?;
                        let objs = s3_client
                            .get_matching_filenames_from_s3(
                                &bucket_cfg.bucket,
                                &prefix,
                                filter.as_deref(),
                            )
                            .await?;
                        all_objects.extend(objs);
                    }

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
