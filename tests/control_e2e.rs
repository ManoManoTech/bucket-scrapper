//! End-to-end runtime-tuning tests: a *real* pipeline (Garage S3 source →
//! void sink) driven by the control socket, asserting that concurrency
//! actually rises then falls when retuned mid-run.
//!
//! Two scenarios:
//!   * download concurrency — grow then shrink the downloader pool and watch
//!     `files_in_flight` climb past the original cap, then settle back under
//!     the lowered cap;
//!   * filter workers — grow then shrink the filter pool and watch
//!     `workers_alive` reach the grown count, then fall to the shrunk count.
//!
//! Both run in-process (so we hold the control socket directly) and observe
//! live state through the same `status` snapshot `bsctl` would read. The run
//! is aborted once observed — it never needs to finish.

mod e2e;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use aws_sdk_s3::primitives::ByteStream;
use bucket_scrapper::control::{encode_request, ControlRequest, ControlResponse, StatusSnapshot};
use bucket_scrapper::matcher::{LineMatcher, MatcherConfig};
use bucket_scrapper::pipeline::{
    OutputSink, StreamingDownloader, StreamingDownloaderConfig, VoidOutputSink,
};
use bucket_scrapper::s3::S3ObjectInfo;
use e2e::fixtures::{seed_bucket, Encoding, StagedObject};
use e2e::garage::start_garage;

const BUCKET: &str = "logs-bucket";
const PREFIX: &str = "logs/dt=20260101/hour=10";

// ── fixture ────────────────────────────────────────────────────────────────

/// Build `n` gzip objects of `lines_per` JSON lines each. Lines are padded so
/// each object carries real decode + scan work, keeping the pipeline busy long
/// enough to observe concurrency transitions.
fn big_fixture(n: usize, lines_per: usize) -> Vec<StagedObject> {
    (0..n)
        .map(|i| {
            let lines = (0..lines_per)
                .map(|j| {
                    let level = if j % 5 == 0 { "ERROR" } else { "INFO" };
                    format!(
                        r#"{{"service":"svc-{i:04}","seq":{j},"level":"{level}","msg":"{level} payload {j} {pad}"}}"#,
                        pad = "xyzover0123456789abcdefghijklmnopqrstuvwxyz0123456789",
                    )
                })
                .collect();
            StagedObject {
                key: format!("{PREFIX}/service-{i:04}.json.gz"),
                lines,
                encoding: Encoding::Gzip,
            }
        })
        .collect()
}

/// Many tiny plaintext objects. Used by the download-concurrency test, where
/// the run is gated by per-object S3 GET latency (not CPU): with N objects and
/// only a couple of download slots, the run lasts long enough to retune and
/// watch concurrency move, yet each object is trivially cheap.
fn tiny_fixture(n: usize) -> Vec<StagedObject> {
    (0..n)
        .map(|i| StagedObject {
            key: format!("{PREFIX}/service-{i:05}.json"),
            lines: vec![
                format!(r#"{{"service":"svc","seq":{i},"level":"ERROR","msg":"e{i}"}}"#),
                format!(r#"{{"service":"svc","seq":{i},"level":"INFO","msg":"i{i}"}}"#),
            ],
            encoding: Encoding::Plain,
        })
        .collect()
}

/// Seed objects concurrently (sequential PUTs are too slow for thousands of
/// tiny objects). Bucket must already exist.
async fn seed_parallel(s3: &aws_sdk_s3::Client, staged: &[StagedObject]) -> Result<()> {
    use tokio::sync::Semaphore;
    let sem = Arc::new(Semaphore::new(64));
    let mut set = tokio::task::JoinSet::new();
    for obj in staged {
        let permit = sem.clone().acquire_owned().await?;
        let s3 = s3.clone();
        let key = obj.key.clone();
        let body = obj.body()?;
        set.spawn(async move {
            let _permit = permit;
            s3.put_object()
                .bucket(BUCKET)
                .key(&key)
                .body(ByteStream::from(body))
                .send()
                .await
                .map(|_| ())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined??;
    }
    Ok(())
}

/// Build the `S3ObjectInfo` list the orchestrator consumes directly from the
/// staged set (size = stored/compressed length; one shared prefix).
fn object_infos(staged: &[StagedObject]) -> Result<Vec<S3ObjectInfo>> {
    staged
        .iter()
        .map(|o| {
            Ok(S3ObjectInfo {
                bucket: BUCKET.to_string(),
                key: o.key.clone(),
                size: o.body()?.len(),
                last_modified: Utc::now(),
                prefix: PREFIX.to_string(),
            })
        })
        .collect()
}

// ── control-socket client ───────────────────────────────────────────────────

async fn rpc(sock: &Path, req: ControlRequest) -> Result<ControlResponse> {
    let stream = UnixStream::connect(sock).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut line = encode_request(&req);
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;
    let mut lines = BufReader::new(read_half).lines();
    let resp = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow!("no response line"))?;
    Ok(serde_json::from_str(&resp)?)
}

async fn status(sock: &Path) -> Result<StatusSnapshot> {
    match rpc(sock, ControlRequest::Status).await? {
        ControlResponse::Status(s) => Ok(s),
        other => bail!("expected Status, got {other:?}"),
    }
}

async fn expect_applied(sock: &Path, req: ControlRequest) -> Result<(i64, i64)> {
    match rpc(sock, req).await? {
        ControlResponse::Applied { before, after, .. } => Ok((before, after)),
        other => bail!("expected Applied, got {other:?}"),
    }
}

/// Wait until the control socket answers `status` (i.e. the run is up).
async fn wait_for_socket(sock: &Path) -> Result<()> {
    for _ in 0..200 {
        if status(sock).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("control socket never came up at {}", sock.display())
}

/// Poll `status` for `dur`, returning the max value of `pick` observed.
async fn poll_max(sock: &Path, dur: Duration, pick: fn(&StatusSnapshot) -> usize) -> usize {
    let deadline = Instant::now() + dur;
    let mut max = 0usize;
    while Instant::now() < deadline {
        if let Ok(s) = status(sock).await {
            max = max.max(pick(&s));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    max
}

/// Poll until `pred(status)` holds (returns the snapshot) or `timeout` elapses.
async fn wait_until(
    sock: &Path,
    timeout: Duration,
    pred: fn(&StatusSnapshot) -> bool,
) -> Result<StatusSnapshot> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(s) = status(sock).await {
            if pred(&s) {
                return Ok(s);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("predicate not satisfied within {timeout:?}")
}

fn searcher() -> Arc<LineMatcher> {
    Arc::new(
        LineMatcher::new(MatcherConfig {
            pattern: Some("ERROR".to_string()),
            ignore_case: false,
        })
        .unwrap(),
    )
}

// ── tests ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_concurrency_grows_then_shrinks() {
    skip_unless_docker!();
    if let Err(e) = run_download_concurrency_test().await {
        panic!("download_concurrency_grows_then_shrinks failed: {e:#}");
    }
}

async fn run_download_concurrency_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    // Many tiny objects: the run is gated by GET latency, so a small download
    // pool keeps it alive long enough to observe while each object stays cheap.
    // Sized so the run outlives the observation window even after the grow
    // speeds it up (~12 s at 2 slots; observation is ~2 s).
    let staged = tiny_fixture(15000);
    seed_parallel(&garage.s3_client(), &staged).await?;
    let objects = object_infos(&staged)?;

    let tmp = tempfile::tempdir()?;
    let sock = tmp.path().join("ctl.sock");

    // Start with just 2 download slots. `files_in_flight` is hard-bounded by
    // the file-semaphore permits, so the starting cap is a strict ceiling of 2.
    let config = StreamingDownloaderConfig {
        max_concurrent_downloads: 16,
        filter_tasks: 2,
        line_buffer_size: 1000,
        file_slots: 2,
        ..Default::default()
    };
    let downloader = StreamingDownloader::new(garage.s3_client(), config)
        .with_control_socket(Some(sock.clone()));

    let run = tokio::spawn(async move {
        let sink: Arc<dyn OutputSink> = Arc::new(VoidOutputSink::new());
        downloader.search_objects(&objects, searcher(), sink).await
    });

    wait_for_socket(&sock).await?;

    // Baseline: the semaphore caps concurrency at the starting 2.
    let base_peak = poll_max(&sock, Duration::from_millis(300), |s| s.files_in_flight).await;
    assert!(
        base_peak <= 2,
        "baseline files_in_flight must not exceed the starting cap of 2, saw {base_peak}"
    );

    // Grow downloaders 2 → 8 and watch concurrency climb past the old cap.
    let (before, after) =
        expect_applied(&sock, ControlRequest::AdjustDownloadTasks { delta: 6 }).await?;
    assert_eq!((before, after), (2, 8));
    let grow_peak = poll_max(&sock, Duration::from_millis(1500), |s| s.files_in_flight).await;
    assert!(
        grow_peak > 2,
        "after growing to 8, expected files_in_flight to climb past the original cap of 2; \
         saw grow_peak={grow_peak}"
    );

    // Shrink downloaders 8 → 2 and watch concurrency settle back under the cap.
    let (before, after) =
        expect_applied(&sock, ControlRequest::AdjustDownloadTasks { delta: -6 }).await?;
    assert_eq!((before, after), (8, 2));
    let settled = wait_until(&sock, Duration::from_secs(10), |s| {
        s.download_tasks_limit == 2 && s.files_in_flight <= 2
    })
    .await?;
    assert_eq!(settled.download_tasks_limit, 2);
    assert!(settled.files_in_flight <= 2);

    run.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_workers_grow_then_shrink() {
    skip_unless_docker!();
    if let Err(e) = run_filter_workers_test().await {
        panic!("filter_workers_grow_then_shrink failed: {e:#}");
    }
}

async fn run_filter_workers_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let staged = big_fixture(30, 3000);
    seed_bucket(&garage.s3_client(), BUCKET, &staged).await?;
    let objects = object_infos(&staged)?;

    let tmp = tempfile::tempdir()?;
    let sock = tmp.path().join("ctl.sock");

    // A small download pool gates the run length (keeping it alive long enough
    // to observe), while plenty of lines flow so workers hit their retire-check
    // boundary promptly.
    let config = StreamingDownloaderConfig {
        max_concurrent_downloads: 8,
        filter_tasks: 2,
        line_buffer_size: 1000,
        file_slots: 3,
        ..Default::default()
    };
    let downloader = StreamingDownloader::new(garage.s3_client(), config)
        .with_control_socket(Some(sock.clone()));

    let run = tokio::spawn(async move {
        let sink: Arc<dyn OutputSink> = Arc::new(VoidOutputSink::new());
        downloader.search_objects(&objects, searcher(), sink).await
    });

    wait_for_socket(&sock).await?;

    // Baseline: the two startup workers are alive.
    let base = wait_until(&sock, Duration::from_secs(5), |s| {
        s.filter_workers_alive == 2
    })
    .await?;
    assert_eq!(base.filter_workers_alive, 2);

    // Grow 2 → 6.
    let (before, after) =
        expect_applied(&sock, ControlRequest::AdjustFilterWorkers { delta: 4 }).await?;
    assert_eq!((before, after), (2, 6));
    let grown = wait_until(&sock, Duration::from_secs(5), |s| {
        s.filter_workers_alive == 6
    })
    .await?;
    assert_eq!(grown.filter_workers_alive, 6);

    // Shrink 6 → 3 (workers retire at their next line boundary).
    let (before, after) =
        expect_applied(&sock, ControlRequest::AdjustFilterWorkers { delta: -3 }).await?;
    assert_eq!((before, after), (6, 3));
    let shrunk = wait_until(&sock, Duration::from_secs(20), |s| {
        s.filter_workers_alive == 3
    })
    .await?;
    assert_eq!(shrunk.filter_workers_alive, 3);

    run.abort();
    Ok(())
}
