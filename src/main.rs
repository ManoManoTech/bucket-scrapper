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
use bucket_scrapper::config::output::OutputConfig;
use bucket_scrapper::config::path_formatter::generate_path_formatter;
use bucket_scrapper::config::resolve::{resolve_output, CliOutputFormat, OutputCli, OutputKind};
use bucket_scrapper::config::types::{BucketConfig, ConfigSchema};
use bucket_scrapper::matcher::{LineMatcher, MatcherConfig};
use bucket_scrapper::pipeline::codec::Codec;
use bucket_scrapper::pipeline::codec::CodecFormat;
use bucket_scrapper::pipeline::{
    FileOutputSink, HttpOutputSink, HttpResultWriter, HttpWriterConfig, OutputSink, OutputStats,
    S3OutputSink, SharedFileWriter, StreamingDownloader, StreamingDownloaderConfig, VoidOutputSink,
};
use bucket_scrapper::s3::client::WrappedS3Client;
use bucket_scrapper::s3::dns_cache;
use bucket_scrapper::s3::S3ObjectInfo;
use bucket_scrapper::sampling::{parse_unit_interval, FileSampler};
use bucket_scrapper::sharding::ShardSelector;
use bucket_scrapper::utils::date::date_range_to_date_hour_list;
use std::collections::HashMap;

/// High-performance S3 bucket content searcher using ripgrep
#[derive(Parser)]
#[command(name = "bucket-scrapper")]
#[command(about = "Search through S3 bucket contents using ripgrep patterns")]
#[command(version)]
struct Cli {
    /// Path to the config file (optional, for AWS credentials and default buckets)
    #[arg(long, default_value = "sample-config.yaml")]
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

    /// File-level sample rate in (0.0, 1.0]: fraction of input files kept
    /// after key filtering. Coarsest work-shedding mechanism — sheds whole
    /// files. Per-bucket `sample_files` in config overrides this.
    #[arg(long, value_parser = parse_unit_interval)]
    sample_files: Option<f64>,

    /// Seed for the sampling RNG. Omit for fresh entropy each run.
    /// Per-config `sampling_seed` (top-level) is used as fallback.
    #[arg(long)]
    sampling_seed: Option<u64>,

    /// Total number of shards across distributed scrapper instances. When set,
    /// only objects whose index in the deterministically sorted, sampled list
    /// satisfies `index % shard_count == shard_number` are processed by this
    /// instance. Naive partition: workload imbalance scales with per-file size
    /// variance. Must be set together with `--shard-number`.
    #[arg(long, requires = "shard_number")]
    shard_count: Option<usize>,

    /// Zero-indexed shard this instance owns. Must be `< shard_count`.
    /// Must be set together with `--shard-count`.
    #[arg(long, requires = "shard_count")]
    shard_number: Option<usize>,

    // ── Output selection / per-output overrides ────────────────────────────
    //
    // These are per-output settings. Either every flag is unset (the active
    // output is then taken from the config file's `outputs:` block) or you
    // supply `--output <type>` and the flags relevant to that type. Mixing
    // CLI per-output flags with a config `outputs:` block is a hard error;
    // see `crate::config::resolve` for the rules.
    /// Active output: `file`, `http`, `s3`, or `void`. Required when the
    /// config file has no `outputs:` block.
    #[arg(long, value_enum)]
    output: Option<OutputKind>,

    /// Directory for the file output.
    #[arg(long)]
    output_dir: Option<String>,

    /// Per-prefix output filename template for the file output. Supports
    /// `{prefix}`, `{prefix_hash}`, `{run_id}`, `{ext}`. Default `{prefix}.{ext}`.
    /// Must contain `{prefix}` or `{prefix_hash}` to avoid collisions.
    #[arg(long)]
    output_path_template: Option<String>,

    /// Compression format for outputs that compress (file/http/s3).
    #[arg(long, value_enum)]
    compression_format: Option<CodecFormatArg>,

    /// Compression level. Codec-dependent: zstd 1–22 (default 3),
    /// gzip 0–9 (default 6). Must be unset when `--compression-format=none`.
    #[arg(long)]
    compression_level: Option<i32>,

    /// Output framing for matched lines. `json_lines` (default) writes
    /// NDJSON; `json_array` wraps each file/batch in a JSON array;
    /// `json_array_pretty` keeps one item per line inside the array. The
    /// HTTP sink also flips `Content-Type` to `application/json` for the
    /// array variants. Lines are assumed to already be valid JSON values —
    /// no parsing is performed.
    #[arg(long, value_enum)]
    output_format: Option<CliOutputFormat>,

    /// HTTP API URL for log ingestion (e.g., https://logs.example.com/api/v1/logs)
    #[arg(long, env = "HTTP_URL")]
    http_url: Option<String>,

    /// Bearer token for HTTP authentication
    #[arg(long, env = "HTTP_BEARER_AUTH")]
    http_bearer_auth: Option<String>,

    /// Maximum batch size in MB for HTTP requests.
    #[arg(long)]
    http_batch_max_mb: Option<f64>,

    /// Timeout for HTTP requests in seconds
    #[arg(long = "http-timeout")]
    http_timeout_secs: Option<u64>,

    /// Number of concurrent HTTP upload tasks
    #[arg(long)]
    http_upload_tasks: Option<usize>,

    /// Number of concurrent HTTP compressor tasks
    #[arg(long)]
    http_compressor_tasks: Option<usize>,

    /// Batch channel buffer between compressors and uploaders
    #[arg(long)]
    http_upload_channel_size: Option<usize>,

    /// HTTP line channel capacity (max matched lines buffered before compressors)
    #[arg(long)]
    http_line_channel_size: Option<usize>,

    /// Max retries on HTTP send failures (capped at 10)
    #[arg(long)]
    http_max_retries: Option<u32>,

    /// Per-batch submission time threshold in seconds for AIMD upload throttle (0 = disabled)
    #[arg(long)]
    max_submission_time: Option<f64>,

    /// AIMD multiplicative decrease factor (0.15 = reduce rate by 15% on congestion)
    #[arg(long)]
    http_aimd_decrease_factor: Option<f64>,

    /// AIMD additive increase in MB/s per healthy batch
    #[arg(long)]
    http_aimd_increase: Option<f64>,

    /// Global upload rate limit in MB/s (0 = unlimited)
    #[arg(long)]
    max_upload_rate: Option<f64>,

    /// Destination bucket for the s3 output.
    #[arg(long)]
    s3_output_bucket: Option<String>,

    /// Region for the s3 output (defaults to global --region).
    #[arg(long)]
    s3_output_region: Option<String>,

    /// Endpoint URL for non-AWS S3 backends (Garage, MinIO, …).
    #[arg(long)]
    s3_output_endpoint_url: Option<String>,

    /// Key template for the s3 output. Supports {prefix}, {prefix_hash}, {seq}, {run_id}.
    #[arg(long)]
    s3_output_key_template: Option<String>,

    /// Per-prefix mid-run flush threshold in MB for the s3 output. When set,
    /// each prefix's encoder is finalized and uploaded as soon as its
    /// compressed buffer crosses this size, then a fresh encoder starts with
    /// `{seq}` incremented; end-of-run flushes the trailing partial. Omit for
    /// one object per prefix (default).
    #[arg(long)]
    s3_output_batch_max_mb: Option<f64>,

    /// Batch size threshold (MB) at which the s3 sink switches from a
    /// single PutObject to multipart upload. Default 5 (AWS minimum
    /// part size); values below 5 are rejected at startup.
    #[arg(long)]
    s3_output_multipart_threshold_mb: Option<u64>,

    /// Target multipart part size (MB). Default 5. Range 5..=5000;
    /// AWS hard limits are 5 MiB per part and 5 GiB max.
    #[arg(long)]
    s3_output_multipart_part_mb: Option<u64>,

    /// Concurrent multipart parts in flight across all uploads on the
    /// s3 sink. Omit for the AWS transfer manager's auto-tuning;
    /// pass a positive integer for an explicit cap.
    #[arg(long)]
    s3_output_multipart_concurrency: Option<usize>,

    /// Number of concurrent uploader tasks for the s3 output.
    #[arg(long)]
    s3_output_upload_tasks: Option<usize>,
}

/// CLI mirror of [`CodecFormat`] — kept separate so clap's `ValueEnum`
/// derive lives in the binary crate and doesn't leak into the library.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum CodecFormatArg {
    Zstd,
    Gzip,
    None,
}

impl From<CodecFormatArg> for CodecFormat {
    fn from(v: CodecFormatArg) -> Self {
        match v {
            CodecFormatArg::Zstd => CodecFormat::Zstd,
            CodecFormatArg::Gzip => CodecFormat::Gzip,
            CodecFormatArg::None => CodecFormat::None,
        }
    }
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

impl Cli {
    fn to_output_cli(&self) -> OutputCli {
        OutputCli {
            output: self.output,
            output_dir: self.output_dir.clone(),
            http_url: self.http_url.clone(),
            http_bearer_auth: self.http_bearer_auth.clone(),
            http_timeout_secs: self.http_timeout_secs,
            http_batch_max_mb: self.http_batch_max_mb,
            http_compressor_tasks: self.http_compressor_tasks,
            http_upload_tasks: self.http_upload_tasks,
            http_upload_channel_size: self.http_upload_channel_size,
            http_line_channel_size: self.http_line_channel_size,
            http_max_retries: self.http_max_retries,
            http_max_upload_rate_mbps: self.max_upload_rate,
            http_aimd_decrease_factor: self.http_aimd_decrease_factor,
            http_aimd_increase_mbps: self.http_aimd_increase,
            http_aimd_max_submission_time_s: self.max_submission_time,
            s3_bucket: self.s3_output_bucket.clone(),
            s3_region: self.s3_output_region.clone(),
            s3_endpoint_url: self.s3_output_endpoint_url.clone(),
            s3_key_template: self.s3_output_key_template.clone(),
            s3_batch_max_mb: self.s3_output_batch_max_mb,
            s3_multipart_threshold_mb: self.s3_output_multipart_threshold_mb,
            s3_multipart_part_mb: self.s3_output_multipart_part_mb,
            s3_multipart_concurrency: self.s3_output_multipart_concurrency,
            s3_upload_tasks: self.s3_output_upload_tasks,
            compression_format: self.compression_format.map(Into::into),
            compression_level: self.compression_level,
            file_path_template: self.output_path_template.clone(),
            output_format: self.output_format,
        }
    }
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
            if current_priority < 10 && libc::setpriority(libc::PRIO_PROCESS, 0, 10) != 0 {
                eprintln!("Warning: Could not set nice priority to 10");
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

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    match cli.log_format {
        LogFormat::Json => fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .json()
            .flatten_event(true)
            .init(),
        LogFormat::Text => fmt().with_env_filter(env_filter).with_target(false).init(),
    }

    dns_cache::init_global_dns_cache(300).await.ok();

    let config: Option<ConfigSchema> = if cli.config.exists() {
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

    let end_date = if let Some(end) = cli.end.clone() {
        end.parse::<DateTime<Utc>>()
            .context("Invalid end date format")?
    } else {
        Utc::now()
    };

    let start_date = cli
        .start
        .parse::<DateTime<Utc>>()
        .context("Invalid start date format")?;

    let s3_client = Arc::new(WrappedS3Client::new(&cli.region, cli.client_max_age, None).await?);

    let matcher_config = MatcherConfig {
        pattern: cli.line_pattern_regex.clone(),
        ignore_case: cli.ignore_case,
    };

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

    // Resolve output configuration before listing — fail fast if misconfigured.
    let resolved_output = resolve_output(
        &cli.to_output_cli(),
        config.as_ref().unwrap_or(&ConfigSchema::default()),
    )?;

    let sink = build_sink(&resolved_output, &s3_client, cli.max_retries).await?;

    info!(output = sink.type_name(), "Output configured");

    // List all objects in parallel across all buckets × hourly prefixes
    let mut all_bucket_objects = {
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

        info!(
            tasks = total_tasks,
            buckets = config_buckets.len(),
            "Spawned listing tasks"
        );

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

            if successful < total_tasks
                && last_report.elapsed() >= std::time::Duration::from_secs(5)
            {
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

        // Total order: size first (pipeline expects size-ascending), then
        // bucket+key as tiebreaker so distributed shard workers agree on the
        // partition order despite nondeterministic JoinSet completion order.
        all_objects.sort_by(|a, b| {
            a.size
                .cmp(&b.size)
                .then_with(|| a.bucket.cmp(&b.bucket))
                .then_with(|| a.key.cmp(&b.key))
        });
        all_objects
    };

    {
        let per_bucket: HashMap<String, f64> = config_buckets
            .iter()
            .filter_map(|b| b.sample_files.map(|r| (b.bucket.clone(), r)))
            .collect();
        let default_rate = cli.sample_files.unwrap_or(1.0);
        let seed = cli
            .sampling_seed
            .or_else(|| config.as_ref().and_then(|c| c.sampling_seed));

        if default_rate < 1.0 || !per_bucket.is_empty() {
            let before = all_bucket_objects.len();
            let mut sampler = FileSampler::new(default_rate, per_bucket, seed);
            let (kept, dropped) = sampler.apply(&mut all_bucket_objects);
            info!(
                before,
                kept,
                dropped,
                seed = ?seed,
                default_rate,
                "Applied file-level sampling"
            );
        }
    }

    if let (Some(count), Some(number)) = (cli.shard_count, cli.shard_number) {
        let selector = ShardSelector::new(count, number)
            .map_err(|e| anyhow::anyhow!("invalid shard config: {e}"))?;
        let before = all_bucket_objects.len();
        let (kept, dropped) = selector.apply(&mut all_bucket_objects);
        info!(
            before,
            kept,
            dropped,
            shard_count = count,
            shard_number = number,
            "Applied shard partitioning"
        );
    }

    if all_bucket_objects.is_empty() {
        anyhow::bail!("No objects found to search");
    }

    let total_compressed_input: usize = all_bucket_objects.iter().map(|o| o.size).sum();
    info!(
        objects = all_bucket_objects.len(),
        mb = total_compressed_input / 1_000_000,
        "Processing objects"
    );

    let batch_start = std::time::Instant::now();
    let (files_searched, matched_lines) = downloader
        .search_objects(&all_bucket_objects, searcher.clone(), sink.clone())
        .await?;

    let stats = sink.finish().await?;
    report_completion(
        &cli,
        sink.type_name(),
        files_searched,
        matched_lines,
        total_compressed_input,
        batch_start.elapsed().as_secs_f64(),
        &stats,
    );

    Ok(())
}

/// Build an [`OutputSink`] from the resolved [`OutputConfig`].
async fn build_sink(
    cfg: &OutputConfig,
    s3_client: &Arc<WrappedS3Client>,
    cli_max_retries: u32,
) -> Result<Arc<dyn OutputSink>> {
    Ok(match cfg {
        OutputConfig::File(file_cfg) => {
            let codec = Codec::from_config(&file_cfg.compression)?;
            let writer = SharedFileWriter::new(
                file_cfg.dir.clone(),
                file_cfg.path_template.clone(),
                codec,
                file_cfg.format.clone(),
            )?;
            Arc::new(FileOutputSink::new(Arc::new(writer)))
        }
        OutputConfig::Http(http_cfg) => {
            let num_compressor_tasks = http_cfg.compressor_tasks.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get() / 8)
                    .unwrap_or(1)
                    .max(1)
            });
            let num_upload_tasks = http_cfg.upload_tasks.unwrap_or(4 * num_compressor_tasks);
            let max_submission_time = if http_cfg.aimd.max_submission_time_s > 0.0 {
                Some(Duration::from_secs_f64(http_cfg.aimd.max_submission_time_s))
            } else {
                None
            };
            let max_upload_rate = if http_cfg.max_upload_rate_mbps > 0.0 {
                Some(http_cfg.max_upload_rate_mbps * 1_000_000.0)
            } else {
                None
            };

            let codec = Codec::from_config(&http_cfg.compression)?;
            let writer_cfg = HttpWriterConfig {
                url: http_cfg.url.clone(),
                bearer_token: http_cfg.bearer_auth.clone(),
                batch_max_bytes: (http_cfg.batch_max_mb * 1_000_000.0) as usize,
                timeout_secs: http_cfg.timeout_secs,
                max_retries: http_cfg.max_retries.min(cli_max_retries.max(1)).min(10),
                channel_buffer_size: http_cfg.line_channel_size,
                num_compressor_tasks,
                num_upload_tasks,
                upload_channel_size: http_cfg.upload_channel_size,
                codec,
                max_submission_time,
                max_upload_rate,
                aimd_decrease_factor: http_cfg.aimd.decrease_factor,
                aimd_increase_bytes: http_cfg.aimd.increase_mbps * 1_000_000.0,
                format: http_cfg.format.clone(),
            };

            info!(
                url = %http_cfg.url,
                batch_max_mb = http_cfg.batch_max_mb,
                compressor_tasks = num_compressor_tasks,
                upload_tasks = num_upload_tasks,
                "HTTP output configured"
            );

            Arc::new(HttpOutputSink::new(HttpResultWriter::new(writer_cfg)?))
        }
        OutputConfig::S3(s3_cfg) => {
            if s3_cfg.region.is_some() {
                warn!(
                    "outputs[].region override is not yet wired through; \
                     using the global S3 client's region"
                );
            }
            if s3_cfg.endpoint_url.is_some() {
                warn!(
                    "outputs[].endpoint_url override is not yet wired through; \
                     using the global S3 client's endpoint"
                );
            }
            let client = s3_client.get_client().await?;
            info!(
                bucket = %s3_cfg.bucket,
                key_template = %s3_cfg.key_template,
                batch_max_mb = ?s3_cfg.batch_max_mb,
                "S3 output configured"
            );
            Arc::new(S3OutputSink::new(client, s3_cfg)?)
        }
        OutputConfig::Void => {
            info!("Void output configured (matches will be counted, not stored)");
            Arc::new(VoidOutputSink::new())
        }
    })
}

fn report_completion(
    cli: &Cli,
    output_kind: &str,
    files_searched: usize,
    matched_lines: usize,
    total_compressed_input: usize,
    elapsed_s: f64,
    stats: &OutputStats,
) {
    if stats.lines_dropped > 0 {
        warn!(
            lines_dropped = stats.lines_dropped,
            "Some lines were dropped due to output failures"
        );
    }

    let read_compressed_mb = total_compressed_input as f64 / 1_000_000.0;
    let plaintext_mb = stats.plaintext_bytes as f64 / 1_000_000.0;
    let compressed_mb = stats.compressed_bytes as f64 / 1_000_000.0;
    let compression_ratio = if compressed_mb > 0.0 {
        plaintext_mb / compressed_mb
    } else {
        0.0
    };

    info!(
        output = output_kind,
        elapsed_s = elapsed_s,
        files = files_searched,
        matched_lines = matched_lines,
        lines_recorded = stats.matched_lines,
        lines_dropped = stats.lines_dropped,
        read_compressed_mb = read_compressed_mb,
        wrote_compressed_mb = compressed_mb,
        plaintext_mb = plaintext_mb,
        compression_ratio = compression_ratio,
        plaintext_mbps = if elapsed_s > 0.0 { plaintext_mb / elapsed_s } else { 0.0 },
        pattern = cli.line_pattern_regex.as_deref().unwrap_or("(all lines)"),
        extras = ?stats.extras,
        "Search completed"
    );
}
