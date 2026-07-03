//! `bsctl` — control client for a running `bucket-scrapper`.
//!
//! Connects to the pipeline's Unix domain socket (the daemon's
//! `--control-socket <PATH>`) and issues one command: read live `status`, or
//! adjust a tuning knob. One request line in, one response line out — a thin
//! blocking client over `serde_json`, no async runtime needed.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};

use bucket_scrapper::control::{
    encode_request, ControlRequest, ControlResponse, StatusSnapshot, DEFAULT_SOCKET_PATH,
};
use bucket_scrapper::tune::{HillClimbPolicy, Knobs, PolicyConfig, Proposal, Trial, TunePolicy};

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
    /// Classifier-guided auto-tuner: reset to a conservative baseline, then
    /// hill-climb download/filter concurrency on the live run, reverting on
    /// regression and stopping at the CPU/memory-pressure ceiling.
    Autotune(AutotuneOpts),
}

#[derive(clap::Args)]
struct AutotuneOpts {
    /// Starting download concurrency (climbs from here).
    #[arg(long, default_value = "8")]
    dl_start: usize,
    /// Upper bound on download concurrency (kept generous; the pressure
    /// ceiling and revert-on-regression are the real stops).
    #[arg(long, default_value = "256")]
    dl_cap: usize,
    /// Starting filter-worker count.
    #[arg(long, default_value = "4")]
    filter_start: usize,
    /// Upper bound on filter workers.
    #[arg(long, default_value = "128")]
    filter_cap: usize,
    /// Max trials before settling on the best.
    #[arg(long, default_value = "20")]
    max_trials: usize,
    /// Seconds to let a change reach steady state before measuring. Should be
    /// ≥ the objective window.
    #[arg(long, default_value = "35")]
    settle_secs: u64,
    /// Objective throughput window in seconds (10, 30, or 60).
    #[arg(long, default_value = "30")]
    window: u64,
    /// Stop climbing a knob once PSI cpu.pressure some avg10 exceeds this (%).
    #[arg(long, default_value = "40.0")]
    cpu_ceiling: f64,
    /// Stop climbing once PSI memory.pressure some avg10 exceeds this (%).
    #[arg(long, default_value = "20.0")]
    mem_ceiling: f64,
    /// Log proposals without applying them or settling on a best.
    #[arg(long)]
    dry_run: bool,
}

impl Command {
    /// The one-shot request for simple commands. `Autotune` is a loop, handled
    /// separately in `main`, so it has no single request.
    fn to_request(&self) -> Option<ControlRequest> {
        Some(match *self {
            Command::Status => ControlRequest::Status,
            Command::FilterWorkers { delta } => ControlRequest::AdjustFilterWorkers { delta },
            Command::DownloadTasks { delta } => ControlRequest::AdjustDownloadTasks { delta },
            Command::RangeConcurrency { delta } => ControlRequest::AdjustRangeConcurrency { delta },
            Command::PartSize { mb } => ControlRequest::SetPartSizeMb { mb },
            Command::LineBuffer { size } => ControlRequest::SetLineBufferSize { size },
            Command::Autotune(_) => return None,
        })
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // Autotune is a control loop, not a one-shot request.
    if let Command::Autotune(opts) = &cli.command {
        return match run_autotune(&cli.socket, opts) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("bsctl: {e:#}");
                ExitCode::FAILURE
            }
        };
    }

    let req = cli.command.to_request().expect("non-autotune command");
    match send(&cli.socket, &req) {
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

/// Send one request over a fresh connection and read one response.
fn send(socket: &Path, req: &ControlRequest) -> Result<ControlResponse> {
    let stream = UnixStream::connect(socket).with_context(|| {
        format!(
            "connecting to control socket {} (is the pipeline running with \
             --control-socket?)",
            socket.display()
        )
    })?;
    let mut writer = stream.try_clone().context("cloning socket handle")?;
    let mut line = encode_request(req);
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
        bail!("connection closed before a response was received");
    }
    serde_json::from_str(resp_line.trim()).context("parsing response")
}

fn status(socket: &Path) -> Result<StatusSnapshot> {
    match send(socket, &ControlRequest::Status)? {
        ControlResponse::Status(s) => Ok(s),
        other => bail!("expected status, got {other:?}"),
    }
}

/// Objective = smoothed download (compressed-input) MB/s over the chosen
/// window. In steady state the pipeline is backpressured end-to-end, so this
/// tracks overall progress.
fn objective(s: &StatusSnapshot, window: u64) -> f64 {
    match window {
        0..=10 => s.download_mbps_10s,
        11..=30 => s.download_mbps_30s,
        _ => s.download_mbps_60s,
    }
}

/// True when host pressure is over either ceiling — the cue to stop climbing.
/// When PSI is unavailable (`None`; cgroup v1 / non-Linux) the ceiling is inert
/// and only revert-on-regression guards the oversubscription cliff.
fn over_ceiling(s: &StatusSnapshot, cpu_ceiling: f64, mem_ceiling: f64) -> bool {
    s.cpu_pressure_avg10.is_some_and(|p| p > cpu_ceiling)
        || s.mem_pressure_avg10.is_some_and(|p| p > mem_ceiling)
}

/// Drive the live knobs to `target`. `download_concurrency` sets both the
/// file-semaphore (download tasks) and the download-semaphore (range
/// concurrency) to the same value — on the non-chunked path both gate actual
/// GETs, so moving them together is what changes real download parallelism.
fn apply_knobs(socket: &Path, target: Knobs, dry_run: bool) -> Result<()> {
    let s = status(socket)?;
    let dl_task_delta = target.download_concurrency as i64 - s.download_tasks_limit as i64;
    let range_delta = target.download_concurrency as i64 - s.range_concurrency_limit as i64;
    // Filter shrink is lazy: the server sizes adjustments against its *effective
    // target* (alive minus retirements already queued but not yet claimed), so
    // compute the delta against that too. Using raw `filter_workers_alive` would
    // double-count a pending shrink on a re-apply and overshoot toward the floor.
    let filter_current = s.filter_workers_alive as i64 - s.filter_retire_pending as i64;
    let filter_delta = target.filter_workers as i64 - filter_current;

    if dry_run {
        println!(
            "  would set download_concurrency→{} (download-tasks {dl_task_delta:+}, \
             range {range_delta:+}), filter_workers→{} ({filter_delta:+})",
            target.download_concurrency, target.filter_workers
        );
        return Ok(());
    }
    for (delta, req) in [
        (
            dl_task_delta,
            ControlRequest::AdjustDownloadTasks {
                delta: dl_task_delta,
            },
        ),
        (
            range_delta,
            ControlRequest::AdjustRangeConcurrency { delta: range_delta },
        ),
        (
            filter_delta,
            ControlRequest::AdjustFilterWorkers {
                delta: filter_delta,
            },
        ),
    ] {
        if delta != 0 {
            if let ControlResponse::Error(e) = send(socket, &req)? {
                bail!("applying knobs: {e}");
            }
        }
    }
    Ok(())
}

/// The classifier-guided live control loop (`bsctl autotune`).
fn run_autotune(socket: &Path, o: &AutotuneOpts) -> Result<()> {
    // Confirm the plane is reachable before we start perturbing the run.
    let start = status(socket)?;
    let baseline = Knobs {
        download_concurrency: o.dl_start,
        filter_workers: o.filter_start,
    };
    let cfg = PolicyConfig {
        download_concurrency_cap: o.dl_cap,
        filter_workers_cap: o.filter_cap,
        improve_threshold: 0.03,
    };
    let mut policy = HillClimbPolicy::new(baseline, cfg, o.max_trials);
    let mut history: Vec<Trial> = Vec::new();

    println!(
        "autotune{}: start dl={} filter={} (current dl_tasks={} range={} filter={}), \
         caps dl={} filter={}, settle={}s window={}s, ceilings cpu={:.0}% mem={:.0}%",
        if o.dry_run { " [DRY-RUN]" } else { "" },
        o.dl_start,
        o.filter_start,
        start.download_tasks_limit,
        start.range_concurrency_limit,
        start.filter_workers_alive,
        o.dl_cap,
        o.filter_cap,
        o.settle_secs,
        o.window,
        o.cpu_ceiling,
        o.mem_ceiling,
    );

    loop {
        match policy.propose(&history) {
            Proposal::Done => break,
            Proposal::Evaluate(knobs) => {
                apply_knobs(socket, knobs, o.dry_run)?;
                std::thread::sleep(Duration::from_secs(o.settle_secs));
                let s = status(socket)?;
                let obj = objective(&s, o.window);
                let over = over_ceiling(&s, o.cpu_ceiling, o.mem_ceiling);
                println!(
                    "  trial {:>2}: dl={:<3} filter={:<3} → {:>7.1} MB/s  bottleneck={:<14} \
                     cpu%={} psi_cpu={} psi_mem={}{}",
                    history.len() + 1,
                    knobs.download_concurrency,
                    knobs.filter_workers,
                    obj,
                    s.bottleneck,
                    fmt_opt(s.cpu_percent),
                    fmt_opt(s.cpu_pressure_avg10),
                    fmt_opt(s.mem_pressure_avg10),
                    if over { "  [OVER CEILING]" } else { "" },
                );
                history.push(Trial {
                    knobs,
                    objective: obj,
                    dominant: s.bottleneck,
                    over_pressure: over,
                });
            }
        }
    }

    match policy.best_knobs() {
        Some(best) => {
            if o.dry_run {
                println!(
                    "autotune [DRY-RUN] done: best dl={} filter={} over {} trials (not applied)",
                    best.download_concurrency,
                    best.filter_workers,
                    history.len()
                );
            } else {
                apply_knobs(socket, best, false)?;
                println!(
                    "autotune done: settled on dl={} filter={} over {} trials (applied)",
                    best.download_concurrency,
                    best.filter_workers,
                    history.len()
                );
            }
            Ok(())
        }
        None => Err(anyhow!("autotune completed no trials")),
    }
}

fn fmt_opt(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.1}")).unwrap_or_else(|| "n/a".into())
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
    let opt = |v: Option<f64>| v.map(|x| format!("{x:.1}")).unwrap_or_else(|| "n/a".into());
    println!("cpu_percent             {}", opt(s.cpu_percent));
    println!(
        "cpu_pressure            {} / {}   (some avg10/avg60)",
        opt(s.cpu_pressure_avg10),
        opt(s.cpu_pressure_avg60)
    );
    println!(
        "mem_pressure            {}   (some avg10)",
        opt(s.mem_pressure_avg10)
    );
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
