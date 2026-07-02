//! `bsctl` — control client for a running `bucket-scrapper`.
//!
//! Connects to the pipeline's Unix domain socket (the daemon's
//! `--control-socket <PATH>`) and issues one command: read live `status`, or
//! adjust a tuning knob. One request line in, one response line out — a thin
//! blocking client over `serde_json`, no async runtime needed.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use bucket_scrapper::control::{
    encode_request, ControlRequest, ControlResponse, StatusSnapshot, DEFAULT_SOCKET_PATH,
};

#[derive(Parser)]
#[command(name = "bsctl")]
#[command(about = "Runtime control client for bucket-scrapper", version)]
struct Cli {
    /// Path to the pipeline's control socket (its --control-socket value).
    /// Defaults to `bs.sock` in the working directory, matching the daemon.
    #[arg(long, short, default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,

    /// Emit the raw JSON response instead of the human-readable rendering.
    #[arg(long)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a live snapshot of effective params + gauges.
    Status,
    /// Adjust filter worker count by a signed delta (e.g. +2, -1).
    FilterWorkers {
        #[arg(allow_hyphen_values = true)]
        delta: i64,
    },
    /// Adjust concurrent download tasks ("downloaders") by a signed delta.
    DownloadTasks {
        #[arg(allow_hyphen_values = true)]
        delta: i64,
    },
    /// Adjust range-GET concurrency within chunked downloads by a signed delta.
    RangeConcurrency {
        #[arg(allow_hyphen_values = true)]
        delta: i64,
    },
    /// Set the chunked-download part size in MB (0 disables chunking).
    PartSize { mb: u64 },
    /// Set the line-channel capacity (unsupported in v1; returns a notice).
    LineBuffer { size: usize },
}

impl Command {
    fn to_request(&self) -> ControlRequest {
        match *self {
            Command::Status => ControlRequest::Status,
            Command::FilterWorkers { delta } => ControlRequest::AdjustFilterWorkers { delta },
            Command::DownloadTasks { delta } => ControlRequest::AdjustDownloadTasks { delta },
            Command::RangeConcurrency { delta } => ControlRequest::AdjustRangeConcurrency { delta },
            Command::PartSize { mb } => ControlRequest::SetPartSizeMb { mb },
            Command::LineBuffer { size } => ControlRequest::SetLineBufferSize { size },
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(resp) => {
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&resp).unwrap());
            } else {
                print_human(&resp);
            }
            // A daemon-side error is a non-zero exit so scripts can detect it.
            match resp {
                ControlResponse::Error(_) => ExitCode::FAILURE,
                _ => ExitCode::SUCCESS,
            }
        }
        Err(e) => {
            eprintln!("bsctl: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> Result<ControlResponse> {
    let stream = UnixStream::connect(&cli.socket).with_context(|| {
        format!(
            "connecting to control socket {} (is the pipeline running with \
             --control-socket?)",
            cli.socket.display()
        )
    })?;
    let mut writer = stream.try_clone().context("cloning socket handle")?;
    let mut line = encode_request(&cli.command.to_request());
    line.push('\n');
    writer
        .write_all(line.as_bytes())
        .context("sending request")?;
    writer.flush().ok();

    let mut reader = BufReader::new(stream);
    let mut resp_line = String::new();
    let n = reader
        .read_line(&mut resp_line)
        .context("reading response")?;
    if n == 0 {
        anyhow::bail!("connection closed before a response was received");
    }
    serde_json::from_str(resp_line.trim()).context("parsing response")
}

fn print_human(resp: &ControlResponse) {
    match resp {
        ControlResponse::Status(s) => print_status(s),
        ControlResponse::Applied {
            knob,
            before,
            after,
            note,
        } => {
            print!("{knob}: {before} → {after}");
            if let Some(note) = note {
                print!("  ({note})");
            }
            println!();
        }
        ControlResponse::Unsupported(msg) => println!("unsupported: {msg}"),
        ControlResponse::Error(msg) => println!("error: {msg}"),
    }
}

fn print_status(s: &StatusSnapshot) {
    println!("filter_workers_alive    {}", s.filter_workers_alive);
    if s.filter_retire_pending > 0 {
        println!("filter_retire_pending   {}", s.filter_retire_pending);
    }
    println!("download_tasks_limit    {}", s.download_tasks_limit);
    println!("range_concurrency_limit {}", s.range_concurrency_limit);
    println!("part_size_mb            {}", s.part_size_mb);
    println!("line_buffer_size        {}", s.line_buffer_size);
    println!("--");
    println!("bottleneck              {}", s.bottleneck);
    println!("dl_active               {}", s.dl_active);
    println!("files_in_flight         {}", s.files_in_flight);
    println!("decoders_input_wait     {}", s.decoders_input_wait);
    println!(
        "line_channel            {}/{}",
        s.line_channel_len, s.line_channel_cap
    );
    println!(
        "download_mbps           {:.1} / {:.1} / {:.1}   (10s/30s/60s)",
        s.download_mbps_10s, s.download_mbps_30s, s.download_mbps_60s
    );
    println!(
        "filter_mbps             {:.1} / {:.1} / {:.1}   (10s/30s/60s)",
        s.filter_mbps_10s, s.filter_mbps_30s, s.filter_mbps_60s
    );
    println!("downloaded_bytes        {}", s.downloaded_bytes);
    println!("match_count             {}", s.match_count);
}
