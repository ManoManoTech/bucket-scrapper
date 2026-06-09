//! Offline auto-tuner (`--tune` mode).
//!
//! Runs the pipeline repeatedly against a fixed, deterministically-sampled
//! slice of the bucket (void sink — read-side only), reads the existing
//! bottleneck classifier to decide which knob to bump next, and emits the
//! best-found read-side settings as a ready-to-paste CLI flag string plus an
//! optional JSON sidecar.
//!
//! The search *strategy* lives in [`policy`] behind the [`TunePolicy`] trait;
//! this module owns only the *mechanics* — build a config, run a trial, score
//! it, hand the result to the policy. See `policy::HillClimbPolicy` for the v1
//! algorithm (hill-climb with backtracking).

pub mod policy;

use crate::matcher::LineMatcher;
use crate::pipeline::{StreamingDownloader, StreamingDownloaderConfig, VoidOutputSink};
use crate::progress::dominant_label;
use crate::s3::S3ObjectInfo;
use anyhow::Result;
use aws_sdk_s3::Client;
use policy::{HillClimbPolicy, Knobs, PolicyConfig, Proposal, Trial, TunePolicy};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::sync::Arc;
use tracing::info;

/// Which scalar to maximize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Objective {
    /// Compressed bytes/s pulled from S3 — proxy for wall-clock to scan.
    Download,
    /// Decompressed bytes/s entering the filter stage — reflects CPU work.
    Filter,
}

impl Objective {
    fn label(self) -> &'static str {
        match self {
            Objective::Download => "download_mbps",
            Objective::Filter => "filter_in_mbps",
        }
    }
}

/// User-facing tuner configuration.
#[derive(Debug, Clone)]
pub struct TuneConfig {
    pub objective: Objective,
    /// Cumulative compressed-byte cap for the sample (the "stop after X GB").
    pub sample_bytes: u64,
    pub seed: u64,
    pub max_trials: usize,
    /// Fractional improvement a probe must beat the best by to be accepted.
    pub improve_threshold: f64,
    /// Times to repeat each candidate; the median objective is scored. Damps
    /// inter-trial network variance. Default 1.
    pub repeats: usize,
    /// Hard cap on `max_parallel` the search may propose.
    pub max_parallel_cap: usize,
}

impl Default for TuneConfig {
    fn default() -> Self {
        Self {
            objective: Objective::Download,
            sample_bytes: 20 * 1_000_000_000, // 20 GB
            seed: 0,
            max_trials: 20,
            improve_threshold: 0.03,
            repeats: 1,
            max_parallel_cap: 512,
        }
    }
}

/// The result of a tuning run.
#[derive(Debug, Clone)]
pub struct TuneReport {
    pub best: Trial,
    pub history: Vec<Trial>,
    pub objective: Objective,
    pub available_parallelism: usize,
    pub sample_objects: usize,
    pub sample_bytes: u64,
}

/// Deterministically shuffle the object list with `seed`, then take objects
/// until cumulative compressed size reaches `cap_bytes` (the object that
/// crosses the cap is included). Same seed ⇒ identical sample, so every trial
/// sees the same work and comparisons are fair.
pub fn build_sample(objects: &[S3ObjectInfo], seed: u64, cap_bytes: u64) -> Vec<S3ObjectInfo> {
    let mut order: Vec<usize> = (0..objects.len()).collect();
    let mut rng = StdRng::seed_from_u64(seed);
    order.shuffle(&mut rng);

    let mut out = Vec::new();
    let mut acc: u64 = 0;
    for i in order {
        if acc >= cap_bytes {
            break;
        }
        acc += objects[i].size as u64;
        out.push(objects[i].clone());
    }
    out
}

/// Median of a slice of objectives (sorted copy; averages the two middles for
/// even counts). Empty → 0.0.
fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Run the auto-tuner. Builds a fresh downloader + void sink per trial, scores
/// each by the configured objective, and lets the policy drive the search.
pub async fn run_tune(
    client: Client,
    base_config: StreamingDownloaderConfig,
    objects: &[S3ObjectInfo],
    searcher: Arc<LineMatcher>,
    cfg: TuneConfig,
) -> Result<TuneReport> {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let sample = build_sample(objects, cfg.seed, cfg.sample_bytes);
    let sample_bytes: u64 = sample.iter().map(|o| o.size as u64).sum();
    if sample.is_empty() {
        anyhow::bail!("tuning sample is empty (no objects within the byte cap)");
    }
    info!(
        objective = cfg.objective.label(),
        sample_objects = sample.len(),
        sample_mb = sample_bytes / 1_000_000,
        seed = cfg.seed,
        max_trials = cfg.max_trials,
        repeats = cfg.repeats,
        "Auto-tune starting"
    );

    let baseline = Knobs {
        max_parallel: base_config.max_concurrent_downloads,
        filter_tasks: base_config.filter_tasks,
        line_buffer_size: base_config.line_buffer_size,
    };
    let policy_cfg = PolicyConfig {
        max_parallel_cap: cfg.max_parallel_cap.max(baseline.max_parallel),
        filter_tasks_cap: (4 * cpus).max(baseline.filter_tasks),
        line_buffer_cap: 65_536,
        improve_threshold: cfg.improve_threshold,
    };
    let mut policy = HillClimbPolicy::new(baseline, policy_cfg, cfg.max_trials);

    let repeats = cfg.repeats.max(1);
    let mut history: Vec<Trial> = Vec::new();

    loop {
        let knobs = match policy.propose(&history) {
            Proposal::Done => break,
            Proposal::Evaluate(k) => k,
        };

        let mut objectives = Vec::with_capacity(repeats);
        let mut last_dominant = "download";
        for rep in 0..repeats {
            let (objective, dominant) = run_trial(
                &client,
                &base_config,
                &knobs,
                &sample,
                searcher.clone(),
                cfg.objective,
            )
            .await?;
            objectives.push(objective);
            last_dominant = dominant;
            info!(
                trial = history.len() + 1,
                rep = rep + 1,
                max_parallel = knobs.max_parallel,
                filter_tasks = knobs.filter_tasks,
                line_buffer_size = knobs.line_buffer_size,
                mbps = format_args!("{:.1}", objective / 1_000_000.0),
                dominant,
                "Auto-tune trial"
            );
        }
        let objective = median(&objectives);
        history.push(Trial {
            knobs,
            objective,
            dominant: last_dominant,
        });
    }

    let best = history
        .iter()
        .cloned()
        .max_by(|a, b| a.objective.partial_cmp(&b.objective).unwrap())
        .expect("at least the seed trial ran");

    let report = TuneReport {
        best,
        history,
        objective: cfg.objective,
        available_parallelism: cpus,
        sample_objects: sample.len(),
        sample_bytes,
    };
    log_report(&report);
    Ok(report)
}

/// Run one trial (one repeat) and return `(objective_bytes_per_s, dominant)`.
async fn run_trial(
    client: &Client,
    base_config: &StreamingDownloaderConfig,
    knobs: &Knobs,
    sample: &[S3ObjectInfo],
    searcher: Arc<LineMatcher>,
    objective: Objective,
) -> Result<(f64, &'static str)> {
    let mut config = base_config.clone();
    config.max_concurrent_downloads = knobs.max_parallel;
    config.filter_tasks = knobs.filter_tasks;
    config.line_buffer_size = knobs.line_buffer_size;

    let downloader = StreamingDownloader::new(client.clone(), config);
    let sink = Arc::new(VoidOutputSink::new());
    let outcome = downloader.search_objects(sample, searcher, sink).await?;

    let secs = outcome.elapsed.as_secs_f64().max(1e-9);
    let bytes = match objective {
        Objective::Download => outcome.downloaded_bytes as f64,
        Objective::Filter => outcome.filter_bytes_in as f64,
    };
    let dominant = dominant_label(&outcome.bottleneck_tally).unwrap_or("download");
    Ok((bytes / secs, dominant))
}

impl TuneReport {
    /// Ready-to-paste CLI flags that reproduce the best-found read-side knobs.
    pub fn flag_string(&self) -> String {
        let k = self.best.knobs;
        format!(
            "--max-parallel {} --filter-tasks {} --line-buffer-size {}",
            k.max_parallel, k.filter_tasks, k.line_buffer_size
        )
    }

    /// Machine-readable profile: instance metadata, tuned knobs, achieved
    /// throughput, the winning dominant bottleneck, and the full trial history.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "objective": self.objective.label(),
            "available_parallelism": self.available_parallelism,
            "sample_objects": self.sample_objects,
            "sample_bytes": self.sample_bytes,
            "best": {
                "max_parallel": self.best.knobs.max_parallel,
                "filter_tasks": self.best.knobs.filter_tasks,
                "line_buffer_size": self.best.knobs.line_buffer_size,
                "mbps": self.best.objective / 1_000_000.0,
                "dominant": self.best.dominant,
            },
            "flags": self.flag_string(),
            "trials": self.history.iter().map(|t| serde_json::json!({
                "max_parallel": t.knobs.max_parallel,
                "filter_tasks": t.knobs.filter_tasks,
                "line_buffer_size": t.knobs.line_buffer_size,
                "mbps": t.objective / 1_000_000.0,
                "dominant": t.dominant,
            })).collect::<Vec<_>>(),
        })
    }
}

fn log_report(report: &TuneReport) {
    info!(
        objective = report.objective.label(),
        "Auto-tune complete — trial table:"
    );
    for (i, t) in report.history.iter().enumerate() {
        info!(
            trial = i + 1,
            max_parallel = t.knobs.max_parallel,
            filter_tasks = t.knobs.filter_tasks,
            line_buffer_size = t.knobs.line_buffer_size,
            mbps = format_args!("{:.1}", t.objective / 1_000_000.0),
            dominant = t.dominant,
            "trial",
        );
    }
    info!(
        max_parallel = report.best.knobs.max_parallel,
        filter_tasks = report.best.knobs.filter_tasks,
        line_buffer_size = report.best.knobs.line_buffer_size,
        mbps = format_args!("{:.1}", report.best.objective / 1_000_000.0),
        dominant = report.best.dominant,
        flags = %report.flag_string(),
        "Auto-tune best settings"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn obj(size: usize, key: &str) -> S3ObjectInfo {
        S3ObjectInfo {
            bucket: "b".to_string(),
            key: key.to_string(),
            size,
            last_modified: Utc::now(),
            prefix: "p".to_string(),
        }
    }

    fn corpus() -> Vec<S3ObjectInfo> {
        (0..100).map(|i| obj(1_000_000, &format!("k{i}"))).collect()
    }

    #[test]
    fn build_sample_is_deterministic_for_a_seed() {
        let c = corpus();
        let a = build_sample(&c, 42, 10_000_000);
        let b = build_sample(&c, 42, 10_000_000);
        let ak: Vec<_> = a.iter().map(|o| &o.key).collect();
        let bk: Vec<_> = b.iter().map(|o| &o.key).collect();
        assert_eq!(ak, bk, "same seed must produce identical sample");
    }

    #[test]
    fn build_sample_respects_cap() {
        let c = corpus(); // 100 × 1MB = 100MB total
        let cap = 10_000_000u64; // 10 MB
        let s = build_sample(&c, 7, cap);
        let total: u64 = s.iter().map(|o| o.size as u64).sum();
        // Cumulative reaches the cap (object crossing it is included)...
        assert!(total >= cap, "sample total {total} should reach cap {cap}");
        // ...but dropping the last object falls short of the cap (we stopped
        // adding as soon as we crossed it).
        let without_last: u64 = total - s.last().unwrap().size as u64;
        assert!(without_last < cap, "should stop right at the cap boundary");
    }

    #[test]
    fn build_sample_returns_all_when_under_cap() {
        let c = corpus();
        let s = build_sample(&c, 1, 1_000_000_000); // 1GB cap > 100MB total
        assert_eq!(s.len(), c.len());
    }

    #[test]
    fn median_basic() {
        assert_eq!(median(&[]), 0.0);
        assert_eq!(median(&[5.0]), 5.0);
        assert_eq!(median(&[1.0, 3.0, 2.0]), 2.0);
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
    }
}
