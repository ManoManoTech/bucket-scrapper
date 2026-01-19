// src/main.rs
// Location: src/main.rs
mod config;
mod s3;
mod utils;

use crate::config::loader::{
    get_archived_buckets, get_consolidated_buckets, get_results_bucket, load_config,
};
use crate::s3::checker::Checker;
use crate::s3::client::WrappedS3Client;
use crate::s3::dns_cache;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use futures::{stream, StreamExt};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};
use utils::date::date_range_to_date_hour_list;
use utils::recap_html::{aggregate_by_day, generate_recap_html};

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

    /// Maximum age of the S3 client in minutes (longer = fewer DNS queries)
    #[arg(long, default_value = "60")]
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

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
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

    /// Generate a recap/summary of consolidation checks over a date range
    Recap {
        /// Start date in ISO 8601 format (e.g. 2023-01-01T00:00:00Z)
        #[arg(short, long)]
        start: String,

        /// End date in ISO 8601 format (e.g. 2023-01-01T01:00:00Z)
        #[arg(short, long)]
        end: String,

        /// Output format (json, text, csv, html)
        #[arg(short, long, default_value = "text")]
        format: String,

        /// Output file path (required for html format)
        #[arg(short = 'o', long)]
        output: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        // Output error as JSON to maintain consistent log format
        let error_json = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": "ERROR",
            "message": format!("{:#}", e),
            "error": true,
            "target": "log_consolidator_checker_rust",
        });
        eprintln!(
            "{}",
            serde_json::to_string(&error_json)
                .unwrap_or_else(|_| format!("{{\"error\": \"{}\"}}", e))
        );
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging with JSON format
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    fmt()
        .json()
        .flatten_event(true)
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    // Load configuration
    info!("Loading config from {}", cli.config.display());
    let config = load_config(&cli.config)?;

    // Initialize DNS cache to reduce CoreDNS load when running many concurrent processes
    // TTL of 300 seconds (5 minutes) - AWS endpoints are stable
    if let Err(e) = dns_cache::init_global_dns_cache(300).await {
        warn!(error = %e, "Failed to initialize DNS cache, continuing without caching");
    }

    // Initialize S3 client (will pre-warm DNS cache if available)
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

            // Get client once upfront to avoid repeated get_client() calls and DNS lookups
            let client = s3_client.get_client().await?;

            // List files in archived buckets
            info!("Checking archived buckets");
            let archived_buckets = get_archived_buckets(&config);
            for bucket in archived_buckets {
                info!("Checking archived bucket: {}", bucket.bucket);

                for date_hour in &date_hours {
                    let result = s3_client
                        .get_matching_filenames_from_s3_with_client(
                            &client,
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
                        .get_matching_filenames_from_s3_with_client(
                            &client,
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
            info!(target_date = %date, target_hour = %hour, "Starting consolidation check");

            // Log memory buffer settings
            info!(target_date = %date, target_hour = %hour, memory_pool_mb = cli.memory_pool_mb, "Memory pool configured");

            // Create a checker with memory limits
            let checker = Checker::new(
                s3_client,
                cli.max_parallel,
                cli.process_threads,
                cli.memory_pool_mb,
            );

            // Set up signal handler for memory monitoring if enabled
            if cli.enable_signals {
                // Get references to memory allocators from the checker
                if let Some(memory_monitor) = checker.get_memory_monitor(date, hour) {
                    if let Err(e) = memory_monitor.setup_signal_handler() {
                        warn!(target_date = %date, target_hour = %hour, error = %e, "Failed to set up signal handler");
                    } else {
                        info!(target_date = %date, target_hour = %hour, pid = std::process::id(), "Signal handler ready (SIGUSR2)");
                    }
                }
            }

            // Spawn a background task for periodic updates with cancellation
            let cancel_token = tokio_util::sync::CancellationToken::new();
            let cancel_token_clone = cancel_token.clone();
            if let Some(memory_monitor) = checker.get_memory_monitor(date, hour) {
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = cancel_token_clone.cancelled() => {
                                break;
                            }
                            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                                // Display stats
                                memory_monitor.log_memory_stats();
                            }
                        }
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

            // Upload check result to S3
            if let Some(results_bucket) = get_results_bucket(&config) {
                checker
                    .upload_check_result(results_bucket, date, hour, &result)
                    .await?;
            } else {
                warn!("No results bucket configured, skipping upload");
            }

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
                        info!(
                            lhs = "/tmp/lhs.pgm",
                            rhs = "/tmp/rhs.pgm",
                            diff = "/tmp/diff.pgm",
                            "Generated PGM visualizations"
                        );
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

            // Cancel periodic logging task and log final memory stats
            cancel_token.cancel();
            if cli.enable_signals {
                if let Some(memory_monitor) = checker.get_memory_monitor(date, hour) {
                    memory_monitor.log_memory_stats();
                }
            }

            // Output result - exit 0 regardless of check result
            // The check result (pass/fail) is reported in logs and S3, not via exit code
            if result.ok {
                info!(target_date = %date, target_hour = %hour, "Check passed");
            } else {
                error!(target_date = %date, target_hour = %hour, message = %result.message, "Check failed");
            }
        }

        Commands::Recap {
            start,
            end,
            format,
            output,
        } => {
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
                "Generating recap for {} hours (from {}/{} to {}/{})",
                date_hours.len(),
                date_hours.first().unwrap().date,
                date_hours.first().unwrap().hour,
                date_hours.last().unwrap().date,
                date_hours.last().unwrap().hour,
            );

            // Get results bucket config
            let results_bucket = get_results_bucket(&config)
                .ok_or_else(|| anyhow::anyhow!("No results bucket configured"))?;

            info!(
                bucket = %results_bucket.bucket,
                "Reading check results from bucket"
            );

            // Get client once upfront to avoid repeated get_client() calls and DNS lookups
            let client = s3_client.get_client().await?;

            // Fast path for missing-txt: parallel existence checks only, no downloads
            if format == "missing-txt" {
                const CONCURRENT_CHECKS: usize = 64;

                info!(
                    total_hours = date_hours.len(),
                    concurrency = CONCURRENT_CHECKS,
                    "Starting parallel existence checks"
                );

                let missing_checks: Vec<(String, String)> = stream::iter(date_hours.iter())
                    .map(|date_hour| {
                        let client = &client;
                        let s3_client = &s3_client;
                        let results_bucket = results_bucket;
                        async move {
                            let file_list = s3_client
                                .get_matching_filenames_from_s3_with_client(
                                    client,
                                    results_bucket,
                                    &date_hour.date,
                                    &date_hour.hour,
                                    false,
                                )
                                .await;

                            match file_list {
                                Ok(fl) if fl.files.is_empty() => {
                                    Some((date_hour.date.clone(), date_hour.hour.clone()))
                                }
                                Ok(_) => None,
                                Err(e) => {
                                    warn!(
                                        date = %date_hour.date,
                                        hour = %date_hour.hour,
                                        error = %e,
                                        "Error checking existence, treating as missing"
                                    );
                                    Some((date_hour.date.clone(), date_hour.hour.clone()))
                                }
                            }
                        }
                    })
                    .buffer_unordered(CONCURRENT_CHECKS)
                    .filter_map(|x| async { x })
                    .collect()
                    .await;

                let mut sorted_missing = missing_checks;
                sorted_missing.sort();

                info!(
                    total_hours = date_hours.len(),
                    total_missing = sorted_missing.len(),
                    "Parallel existence check complete"
                );

                // Generate tab-separated output
                let txt_output: String = sorted_missing
                    .iter()
                    .map(|(date, hour)| format!("{}\t{}", date, hour))
                    .collect::<Vec<_>>()
                    .join("\n");

                if let Some(output_path) = output {
                    std::fs::write(output_path, &txt_output)?;
                    info!(
                        path = %output_path,
                        total_hours = date_hours.len(),
                        total_missing = sorted_missing.len(),
                        "Missing checks txt list written"
                    );
                } else {
                    println!("{}", txt_output);
                }

                return Ok(());
            }

            // Fast parallel path for formats that need results (html, retry-txt, retry-json)
            const CONCURRENT_CHECKS: usize = 64;

            info!(
                total_hours = date_hours.len(),
                concurrency = CONCURRENT_CHECKS,
                "Starting parallel check result fetching"
            );

            // Phase 1: Parallel list + download
            // Tuple: (date, hour, json_result, check_count)
            let fetch_results: Vec<(String, String, Option<serde_json::Value>, usize)> =
                stream::iter(date_hours.iter())
                    .map(|date_hour| {
                        let client = &client;
                        let s3_client = &s3_client;
                        let results_bucket = results_bucket;
                        async move {
                            // List files for this date/hour
                            let file_list = match s3_client
                                .get_matching_filenames_from_s3_with_client(
                                    client,
                                    results_bucket,
                                    &date_hour.date,
                                    &date_hour.hour,
                                    false,
                                )
                                .await
                            {
                                Ok(fl) => fl,
                                Err(e) => {
                                    warn!(
                                        date = %date_hour.date,
                                        hour = %date_hour.hour,
                                        error = %e,
                                        "Error listing files"
                                    );
                                    return (
                                        date_hour.date.clone(),
                                        date_hour.hour.clone(),
                                        None,
                                        0,
                                    );
                                }
                            };

                            let check_count = file_list.files.len();

                            if file_list.files.is_empty() {
                                return (date_hour.date.clone(), date_hour.hour.clone(), None, 0);
                            }

                            // Download the last result file (check_result.json comes last lexicographically)
                            if let Some(file) = file_list.files.last() {
                                match s3_client
                                    .download_object_with_client(client, &file.bucket, &file.key)
                                    .await
                                {
                                    Ok(bytes) => {
                                        match serde_json::from_slice::<serde_json::Value>(&bytes) {
                                            Ok(json) => {
                                                return (
                                                    date_hour.date.clone(),
                                                    date_hour.hour.clone(),
                                                    Some(json),
                                                    check_count,
                                                );
                                            }
                                            Err(e) => {
                                                warn!(
                                                    date = %date_hour.date,
                                                    hour = %date_hour.hour,
                                                    error = %e,
                                                    "Error parsing JSON"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            date = %date_hour.date,
                                            hour = %date_hour.hour,
                                            error = %e,
                                            "Error downloading file"
                                        );
                                    }
                                }
                            }

                            (
                                date_hour.date.clone(),
                                date_hour.hour.clone(),
                                None,
                                check_count,
                            )
                        }
                    })
                    .buffer_unordered(CONCURRENT_CHECKS)
                    .collect()
                    .await;

            // Phase 2: Build results and missing lists
            let mut results: HashMap<String, serde_json::Value> = HashMap::new();
            let mut missing_checks: Vec<(String, String)> = Vec::new();

            for (date, hour, json_opt, check_count) in fetch_results {
                let key = format!("{}/{}", date, hour);
                match json_opt {
                    Some(mut json) => {
                        // Inject check_count into the JSON result
                        if let Some(obj) = json.as_object_mut() {
                            obj.insert("check_count".to_string(), serde_json::json!(check_count));
                        }
                        results.insert(key, json);
                    }
                    None => {
                        missing_checks.push((date, hour));
                    }
                }
            }

            info!(
                total_hours = date_hours.len(),
                found = results.len(),
                missing = missing_checks.len(),
                "Parallel check result fetching complete"
            );

            // Output based on format
            match format.as_str() {
                "json" => println!("{}", serde_json::to_string_pretty(&results)?),
                "text" => {
                    // TODO: Implement human-readable text summary
                    info!("Text format not yet implemented, falling back to JSON");
                    println!("{}", serde_json::to_string_pretty(&results)?);
                }
                "csv" => {
                    // TODO: Implement CSV output format
                    info!("CSV format not yet implemented, falling back to JSON");
                    println!("{}", serde_json::to_string_pretty(&results)?);
                }
                "html" => {
                    let output_path = output
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("--output is required for html format"))?;

                    let summaries = aggregate_by_day(&results, &missing_checks);
                    let html = generate_recap_html(&summaries);

                    std::fs::write(output_path, &html)?;
                    info!(path = %output_path, days = summaries.len(), "HTML recap written");
                }
                "retry-json" => {
                    // Filter for failed checks and generate retry list (exclude cleaned entries)
                    let mut failed_checks: Vec<serde_json::Value> = Vec::new();

                    for (key, value) in &results {
                        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
                        let message = value.get("message").and_then(|v| v.as_str()).unwrap_or("");

                        // Skip cleaned entries (check both old and new message patterns)
                        let is_cleaned = message.contains("No input files found")
                            && message.contains("consolidated files exist")
                            || message.starts_with("Cleaned:");

                        if !ok && !is_cleaned {
                            // Key format: "YYYYMMDD/HH"
                            let parts: Vec<&str> = key.split('/').collect();
                            if parts.len() == 2 {
                                failed_checks.push(serde_json::json!({
                                    "date": parts[0],
                                    "hour": parts[1]
                                }));
                            }
                        }
                    }

                    // Sort by date then hour
                    failed_checks.sort_by(|a, b| {
                        let a_date = a.get("date").and_then(|v| v.as_str()).unwrap_or("");
                        let b_date = b.get("date").and_then(|v| v.as_str()).unwrap_or("");
                        let a_hour = a.get("hour").and_then(|v| v.as_str()).unwrap_or("");
                        let b_hour = b.get("hour").and_then(|v| v.as_str()).unwrap_or("");
                        (a_date, a_hour).cmp(&(b_date, b_hour))
                    });

                    let retry_report = serde_json::json!({
                        "generated_at": chrono::Utc::now().to_rfc3339(),
                        "date_range": {
                            "start": start,
                            "end": end
                        },
                        "total_checked": results.len(),
                        "total_failed": failed_checks.len(),
                        "failed_checks": failed_checks
                    });

                    let json_output = serde_json::to_string_pretty(&retry_report)?;

                    if let Some(output_path) = output {
                        std::fs::write(output_path, &json_output)?;
                        info!(
                            path = %output_path,
                            total_checked = results.len(),
                            total_failed = failed_checks.len(),
                            "Retry list written"
                        );
                    } else {
                        println!("{}", json_output);
                    }
                }
                "retry-txt" => {
                    // Filter for failed checks and generate tab-separated retry list
                    // Format: YYYYMMDD\tHH (compatible with retry-from-textfile.sh)
                    let mut failed_checks: Vec<(String, String)> = Vec::new();

                    for (key, value) in &results {
                        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
                        let message = value.get("message").and_then(|v| v.as_str()).unwrap_or("");

                        // Skip cleaned entries (check both old and new message patterns)
                        let is_cleaned = message.contains("No input files found")
                            && message.contains("consolidated files exist")
                            || message.starts_with("Cleaned:");

                        if !ok && !is_cleaned {
                            // Key format: "YYYYMMDD/HH"
                            let parts: Vec<&str> = key.split('/').collect();
                            if parts.len() == 2 {
                                failed_checks.push((parts[0].to_string(), parts[1].to_string()));
                            }
                        }
                    }

                    // Sort by date then hour
                    failed_checks.sort();

                    // Generate tab-separated output
                    let txt_output: String = failed_checks
                        .iter()
                        .map(|(date, hour)| format!("{}\t{}", date, hour))
                        .collect::<Vec<_>>()
                        .join("\n");

                    if let Some(output_path) = output {
                        std::fs::write(output_path, &txt_output)?;
                        info!(
                            path = %output_path,
                            total_checked = results.len(),
                            total_failed = failed_checks.len(),
                            "Retry txt list written"
                        );
                    } else {
                        println!("{}", txt_output);
                    }
                }
                "to-fix-txt" => {
                    // Filter for pending and KO checks only (exclude cleaned entries)
                    // Format: YYYYMMDD\tHH\tTYPE (PEND or KO)
                    let mut to_fix: Vec<(String, String, String)> = Vec::new();

                    for (key, value) in &results {
                        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
                        let message = value.get("message").and_then(|v| v.as_str()).unwrap_or("");

                        // Skip cleaned entries (check both old and new message patterns)
                        let is_cleaned = message.contains("No input files found")
                            && message.contains("consolidated files exist")
                            || message.starts_with("Cleaned:");

                        if !ok && !is_cleaned {
                            // Key format: "YYYYMMDD/HH"
                            let parts: Vec<&str> = key.split('/').collect();
                            if parts.len() == 2 {
                                let status_type = if message.contains("Consolidation pending") {
                                    "PEND"
                                } else {
                                    "KO"
                                };
                                to_fix.push((
                                    parts[0].to_string(),
                                    parts[1].to_string(),
                                    status_type.to_string(),
                                ));
                            }
                        }
                    }

                    // Sort by date then hour
                    to_fix.sort();

                    // Count by type
                    let pend_count = to_fix.iter().filter(|(_, _, t)| t == "PEND").count();
                    let ko_count = to_fix.iter().filter(|(_, _, t)| t == "KO").count();

                    // Generate tab-separated output
                    let txt_output: String = to_fix
                        .iter()
                        .map(|(date, hour, status)| format!("{}\t{}\t{}", date, hour, status))
                        .collect::<Vec<_>>()
                        .join("\n");

                    if let Some(output_path) = output {
                        std::fs::write(output_path, &txt_output)?;
                        info!(
                            path = %output_path,
                            total_checked = results.len(),
                            total_to_fix = to_fix.len(),
                            pending = pend_count,
                            ko = ko_count,
                            "To-fix txt list written"
                        );
                    } else {
                        println!("{}", txt_output);
                    }
                }
                // Note: "missing-txt" is handled above with fast parallel path
                _ => return Err(anyhow::anyhow!("Unknown format: {}", format)),
            }
        }
    }

    Ok(())
}
