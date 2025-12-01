// src/main.rs
// Location: src/main.rs
mod config;
mod s3;
mod utils;

use crate::config::loader::{get_archived_buckets, get_consolidated_buckets, load_config};
use crate::s3::checker::Checker;
use crate::s3::client::WrappedS3Client;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use jemallocator::Jemalloc;
use log::{info, warn, LevelFilter};
use std::path::PathBuf;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};
use utils::date::date_range_to_date_hour_list;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// S3 Log Consolidator Checker
#[derive(Parser)]
#[command(name = "consolidator-checker")]
#[command(about = "Checks that log consolidation in S3 has been done correctly")]
struct Cli {
    /// Path to the config file
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,

    /// AWS region
    #[arg(short, long, default_value = "eu-west-3")]
    region: String,

    /// Maximum age of the S3 client in minutes
    #[arg(long, default_value = "15")]
    client_max_age: u64,

    /// Maximum parallel downloads
    #[arg(long, default_value = "32")]
    max_parallel: usize,

    /// Number of processing threads (defaults to CPU count)
    #[arg(long)]
    process_threads: Option<usize>,

    /// Memory pool size in MB
    #[arg(long, default_value = "12288")] // 12GB default
    memory_pool_mb: usize,

    /// Enable signal handling for memory monitoring (SIGUSR2)
    #[arg(long, default_value = "true")]
    enable_signals: bool,

    /// Subcommands
    #[command(subcommand)]
    command: Commands,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: LevelFilter,
}

#[derive(Subcommand)]
enum Commands {
    /// List files in archived and consolidated buckets
    List {
        /// Start date in ISO 8601 format (e.g. 2023-01-01T00:00:00Z)
        #[arg(short, long)]
        start: String,

        /// End date in ISO 8601 format (e.g. 2023-01-01T01:00:00Z)
        #[arg(short, long)]
        end: String,
    },

    /// Check a specific date and hour
    Check {
        /// Date in YYYYMMDD format
        #[arg(short, long)]
        date: String,

        /// Hour in HH format (00-23)
        #[arg(short = 'H', long)]
        hour: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    env_logger::builder()
        .filter_level(cli.log_level)
        .format_timestamp_millis()
        .init();

    // Load configuration
    info!("Loading config from {}", cli.config.display());
    let config = load_config(&cli.config)?;

    // Initialize S3 client
    let s3_client = WrappedS3Client::new(&cli.region, cli.client_max_age, None).await?;

    match &cli.command {
        Commands::List { start, end } => {
            // Parse dates
            let start_date = DateTime::parse_from_rfc3339(start)
                .with_context(|| format!("Invalid start date: {}", start))?
                .with_timezone(&Utc);

            let end_date = DateTime::parse_from_rfc3339(end)
                .with_context(|| format!("Invalid end date: {}", end))?
                .with_timezone(&Utc);

            // Get date range
            let date_hours = date_range_to_date_hour_list(&start_date, &end_date)?;
            info!(
                "Checking buckets for {} hours (from {}/{} to {}/{})",
                date_hours.len(),
                date_hours.first().unwrap().date,
                date_hours.first().unwrap().hour,
                date_hours.last().unwrap().date,
                date_hours.last().unwrap().hour,
            );

            // Create a mutable reference to the client for use in this scope
            let s3_client = s3_client;

            // List files in archived buckets
            info!("Checking archived buckets");
            let archived_buckets = get_archived_buckets(&config);
            for bucket in archived_buckets {
                info!("Checking archived bucket: {}", bucket.bucket);

                for date_hour in &date_hours {
                    let result = s3_client
                        .get_matching_filenames_from_s3(
                            bucket,
                            &date_hour.date,
                            &date_hour.hour,
                            true,
                        )
                        .await?;

                    info!(
                        "Found {} files in archived bucket {} for {}/{} (total size: {} bytes)",
                        result.files.len(),
                        bucket.bucket,
                        date_hour.date,
                        date_hour.hour,
                        result.total_archives_size
                    );

                    // Print first few files if any
                    if !result.files.is_empty() {
                        info!("First few files:");
                        for (i, file) in result.files.iter().take(5).enumerate() {
                            info!("  {}: {} ({} bytes)", i + 1, file.key, file.size);
                        }
                    }
                }
            }

            // List files in consolidated bucket
            info!("Checking consolidated buckets");
            let consolidated_buckets = get_consolidated_buckets(&config);
            for bucket in consolidated_buckets {
                info!("Checking consolidated bucket: {}", bucket.bucket);

                for date_hour in &date_hours {
                    let result = s3_client
                        .get_matching_filenames_from_s3(
                            bucket,
                            &date_hour.date,
                            &date_hour.hour,
                            true,
                        )
                        .await?;

                    info!(
                        "Found {} files in consolidated bucket {} for {}/{} (total size: {} bytes)",
                        result.files.len(),
                        bucket.bucket,
                        date_hour.date,
                        date_hour.hour,
                        result.total_archives_size
                    );

                    // Print first few files if any
                    if !result.files.is_empty() {
                        info!("First few files:");
                        for (i, file) in result.files.iter().take(5).enumerate() {
                            info!("  {}: {} ({} bytes)", i + 1, file.key, file.size);
                        }
                    }
                }
            }
        }

        Commands::Check { date, hour } => {

            // Log memory buffer settings

            // Create a checker with memory limits
            let checker = Checker::new(
                s3_client,
                cli.max_parallel,
                cli.process_threads,
                cli.memory_pool_mb,
            );

            // Set up signal handler for memory monitoring if enabled
            if cli.enable_signals {
                info!("Setting up signal handler for memory monitoring (SIGUSR2)");

                // Get references to memory allocators from the checker
                    if let Err(e) = memory_monitor.setup_signal_handler() {
                    } else {
                    }

                    // Log initial memory stats
                    // memory_monitor.log_memory_stats();
                }
            }

            // Spawn a background task for periodic updates
                tokio::spawn(async move {
                    loop {
                        // Display stats
                        memory_monitor.log_memory_stats();

                        // Wait 30 seconds
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    }
                });
            }

            // Get archived and consolidated buckets
            let archived_buckets = get_archived_buckets(&config);
            let consolidated_buckets = get_consolidated_buckets(&config);
            let consolidated_bucket = consolidated_buckets
                .first()
                .ok_or_else(|| anyhow::anyhow!("No consolidated bucket found"))?;

            // Perform comparison
            let result = checker
                .get_comparison_results(&archived_buckets, consolidated_bucket, date, hour)
                .await?;

	    // Generate PGM visualizations
            {
                use crate::utils::pgm_visualizer::PgmVisualizer;

                let visualizer = PgmVisualizer::with_default();
                match visualizer.generate_visualization(
                    &result.archived_counts,
                    &result.consolidated_counts,
                    &result.differences,
                ) {
                    Ok(_) => {
                        info!("Generated PGM visualizations:");
                        info!("  - /tmp/lhs.pgm (archived counts)");
                        info!("  - /tmp/rhs.pgm (consolidated counts)");
                        info!("  - /tmp/diff.pgm (differences)");
                    }
                    Err(e) => {
                        warn!("Failed to generate PGM visualizations: {}", e);
                    }
                }
            }

            // Output result
            if result.ok {
                info!("✅ Check passed for {}/{}", date, hour);
                info!("Message: {}", result.message);

                // info!("Archived buckets:");
                // for (i, archived) in result.archived_data.iter().enumerate() {
                //     info!(
                //         "  {}: {} files in {} (total size: {} bytes)",
                //         i + 1,
                //         archived.files.len(),
                //         archived.bucket,
                //         archived.total_archives_size
                //     );
                // }
                //
                // info!(
                //     "Consolidated bucket: {} files in {} (total size: {} bytes)",
                //     result.consolidated_data.files.len(),
                //     result.consolidated_data.bucket,
                //     result.consolidated_data.total_archives_size
                // );
            } else {
                warn!("❌ Check failed for {}/{}", date, hour);
                warn!("Message: {}", result.message);

                // In a real implementation, we would output detailed failure information
            }

            // Log final memory stats if signal handling is enabled
            if cli.enable_signals {
                    memory_monitor.log_memory_stats();
                }
            }

            // Output result and exit with appropriate code
            if result.ok {
                info!(target_date = %date, target_hour = %hour, "Check passed");
            } else {
                error!(target_date = %date, target_hour = %hour, message = %result.message, "Check failed");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
