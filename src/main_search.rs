// Simplified search command implementation for main.rs
use crate::search_executor::SearchExecutor;
use crate::search::{SearchConfig, StreamSearcher};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tracing::info;

pub async fn execute_search(
    pattern: String,
    buckets: Option<Vec<String>>,
    filter: Option<String>,
    start: Option<String>,
    end: Option<String>,
    ignore_case: bool,
    count: bool,
    max_parallel: usize,
    buffer_size_kb: usize,
    channel_buffer: usize,
    max_retries: u32,
    retry_delay: u64,
    client_max_age: u64,
    streaming: bool,
    output: OutputFormat,
    config: Option<&crate::config::types::Config>,
    s3_client: &mut crate::s3::WrappedS3Client,
) -> Result<()> {
    // Parse dates
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
    
    // Configure search
    let search_config = SearchConfig {
        pattern: pattern.clone(),
        ignore_case,
        count_only: count,
    };
    
    // Create searcher
    let searcher = Arc::new(StreamSearcher::new(search_config)?);
    
    // Get buckets
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
        eprintln!("Error: No buckets to search.");
        std::process::exit(1);
    }
    
    // Determine output directory
    let output_dir = config
        .and_then(|c| c.output_dir.clone())
        .unwrap_or_else(|| "./scrapper-output".to_string());
    
    // Create search executor
    let mut executor = SearchExecutor::new(
        s3_client.clone(),
        searcher,
        max_parallel,
        buffer_size_kb,
        channel_buffer,
        max_retries,
        retry_delay,
    );
    
    // Execute search
    if streaming {
        info!("Using streaming output mode to {}", output_dir);
        std::fs::create_dir_all(&output_dir)?;
        
        let (files_searched, files_with_matches, total_matches, files_written) = 
            executor.search_streaming(
                &simple_buckets,
                &config_buckets,
                filter.as_deref(),
                start_date,
                end_date,
                output_dir.clone(),
            ).await?;
        
        // Print summary
        match output {
            OutputFormat::Json => {
                let summary = serde_json::json!({
                    "pattern": pattern,
                    "files_searched": files_searched,
                    "files_with_matches": files_with_matches,
                    "total_matches": total_matches,
                    "output_files_written": files_written,
                    "output_directory": output_dir,
                    "streaming": true
                });
                println!("{}", serde_json::to_string_pretty(&summary)?);
            }
            OutputFormat::Text | OutputFormat::Quiet => {
                println!("\n=== Search Complete (Streaming Mode) ===");
                println!("Pattern: {}", pattern);
                println!("Files searched: {}", files_searched);
                println!("Files with matches: {}", files_with_matches);
                println!("Total matches: {}", total_matches);
                println!("Output files written: {}", files_written);
                println!("Output directory: {}", output_dir);
            }
        }
    } else {
        info!("Using buffered output mode");
        
        let results = executor.search_buffered(
            &simple_buckets,
            &config_buckets,
            filter.as_deref(),
            start_date,
            end_date,
        ).await?;
        
        // Create output directory and write results
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
            
            // Create output file
            let output_file = format!("{}/{}.gz", output_dir, prefix);
            let file = std::fs::File::create(&output_file)?;
            let mut encoder = GzEncoder::new(file, Compression::default());
            
            // Write matches as JSON lines
            for match_item in matches {
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
                    "output_directory": output_dir,
                    "streaming": false
                });
                println!("{}", serde_json::to_string_pretty(&summary)?);
            }
            OutputFormat::Text | OutputFormat::Quiet => {
                println!("\n=== Search Complete (Buffered Mode) ===");
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
    
    Ok(())
}