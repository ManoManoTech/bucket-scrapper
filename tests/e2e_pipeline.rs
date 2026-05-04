//! End-to-end pipeline tests: real Garage S3 source + (file output | nginx HTTP receiver).
//!
//! These spin up Docker containers via `testcontainers`. They are skipped when no
//! Docker daemon is reachable, so `cargo test` stays green on CI hosts without Docker.

mod e2e;

use anyhow::Result;
use assert_cmd::Command;
use aws_sdk_s3::Client as S3Client;
use e2e::fixtures::{build_fixture, expected_matches, seed_bucket};
use e2e::garage::start_garage;
use e2e::nginx::start_nginx;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;
use tempfile::TempDir;

const BUCKET: &str = "logs-bucket";
const RESULTS_BUCKET: &str = "results-bucket";
const DATE: &str = "20260101"; // 2026-01-01
const HOURS: &[&str] = &["10", "11"];
const PATTERN: &str = "ERROR";

fn write_config_yaml(path: &Path, output_dir: Option<&Path>) -> Result<()> {
    let mut yaml = format!(
        r#"buckets:
  - bucket: {BUCKET}
    path:
      - static_path: logs
      - datefmt: "dt=20060102/hour=15"
    only_prefix_patterns:
      - 'service-.*\.(json|json\.gz|json\.zst)$'

region: garage
"#,
    );
    if let Some(dir) = output_dir {
        yaml.push_str(&format!(
            "outputs:\n  - type: file\n    dir: {}\n",
            dir.display()
        ));
    }
    std::fs::write(path, yaml)?;
    Ok(())
}

fn read_zst_file(path: &Path) -> Result<String> {
    let f = std::fs::File::open(path)?;
    let mut decoder = zstd::Decoder::new(f)?;
    let mut s = String::new();
    decoder.read_to_string(&mut s)?;
    Ok(s)
}

fn collect_zst_files(root: &Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("zst") {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_output_end_to_end() {
    skip_unless_docker!();
    if let Err(e) = run_file_test().await {
        panic!("file_output_end_to_end failed: {e:#}");
    }
}

async fn run_file_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();
    let staged = build_fixture(DATE, HOURS);
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let output_dir = workdir.path().join("out");
    std::fs::create_dir_all(&output_dir)?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, Some(&output_dir))?;

    let mut cmd = Command::cargo_bin("bucket-scrapper")?;
    for (k, v) in garage.env_for_scrapper() {
        cmd.env(k, v);
    }
    cmd.arg("--config")
        .arg(&config_path)
        .arg("--region")
        .arg("garage")
        .arg("--start")
        .arg("2026-01-01T10:00:00Z")
        .arg("--end")
        .arg("2026-01-01T11:00:00Z")
        .arg("--line-pattern-regex")
        .arg(PATTERN)
        .arg("--filter")
        .arg(r"service-.*\.(json|json\.gz|json\.zst)$")
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let zsts = collect_zst_files(&output_dir);
    assert!(
        !zsts.is_empty(),
        "no .zst output files written under {}",
        output_dir.display()
    );

    let mut received: Vec<String> = Vec::new();
    for f in &zsts {
        let s = read_zst_file(f)?;
        for line in s.lines() {
            if !line.is_empty() {
                received.push(line.to_string());
            }
        }
    }

    let expected = expected_matches(&staged, PATTERN);
    assert_multiset_eq(&received, &expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_output_end_to_end() {
    skip_unless_docker!();
    if let Err(e) = run_http_test().await {
        panic!("http_output_end_to_end failed: {e:#}");
    }
}

async fn run_http_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();
    let staged = build_fixture(DATE, HOURS);
    seed_bucket(&s3, BUCKET, &staged).await?;
    let dump_root = TempDir::new()?;
    let nginx = start_nginx(dump_root.path()).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, None)?;

    let mut cmd = Command::cargo_bin("bucket-scrapper")?;
    for (k, v) in garage.env_for_scrapper() {
        cmd.env(k, v);
    }
    cmd.arg("--config")
        .arg(&config_path)
        .arg("--region")
        .arg("garage")
        .arg("--start")
        .arg("2026-01-01T10:00:00Z")
        .arg("--end")
        .arg("2026-01-01T11:00:00Z")
        .arg("--line-pattern-regex")
        .arg(PATTERN)
        .arg("--filter")
        .arg(r"service-.*\.(json|json\.gz|json\.zst)$")
        .arg("--output")
        .arg("http")
        .arg("--http-url")
        .arg(&nginx.url)
        .arg("--http-bearer-auth")
        .arg("test-token-123")
        .arg("--http-batch-max-mb")
        .arg("1")
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let dumps = nginx.collect_dumps().await?;
    let mut received: Vec<String> = Vec::new();
    for body in &dumps {
        // Body is zstd-compressed NDJSON.
        let plain = zstd::stream::decode_all(body.as_slice())?;
        let s = String::from_utf8(plain)?;
        for line in s.lines() {
            if !line.is_empty() {
                received.push(line.to_string());
            }
        }
    }

    let expected = expected_matches(&staged, PATTERN);
    assert_multiset_eq(&received, &expected);

    drop(dump_root); // keep TempDir alive until end
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end() {
    skip_unless_docker!();
    if let Err(e) = run_s3_test().await {
        panic!("s3_output_end_to_end failed: {e:#}");
    }
}

async fn run_s3_test() -> Result<()> {
    // Single Garage instance, two buckets: one source, one destination.
    // Same credentials, so the scrapper's source S3 client also reaches the
    // destination — no per-output endpoint/credentials override needed.
    let garage = start_garage(BUCKET).await?;
    garage.create_bucket(RESULTS_BUCKET).await?;

    let s3 = garage.s3_client();
    let staged = build_fixture(DATE, HOURS);
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, None)?;

    let mut cmd = Command::cargo_bin("bucket-scrapper")?;
    for (k, v) in garage.env_for_scrapper() {
        cmd.env(k, v);
    }
    cmd.arg("--config")
        .arg(&config_path)
        .arg("--region")
        .arg("garage")
        .arg("--start")
        .arg("2026-01-01T10:00:00Z")
        .arg("--end")
        .arg("2026-01-01T11:00:00Z")
        .arg("--line-pattern-regex")
        .arg(PATTERN)
        .arg("--filter")
        .arg(r"service-.*\.(json|json\.gz|json\.zst)$")
        .arg("--output")
        .arg("s3")
        .arg("--s3-output-bucket")
        .arg(RESULTS_BUCKET)
        .arg("--s3-output-batch-max-mb")
        .arg("1")
        .arg("--s3-output-key-template")
        .arg("out/{prefix}/{run_id}-{seq}.ndjson.zst")
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let (received, keys) = list_and_decode_zst(&s3, RESULTS_BUCKET, "out/").await?;
    let expected = expected_matches(&staged, PATTERN);
    assert_multiset_eq(&received, &expected);

    // At least one returned object key matches the configured template shape:
    //   out/<prefix-with-slashes>/<8-hex-run-id>-<5-digit-seq>.ndjson.zst
    let template_re = regex::Regex::new(r"^out/.+/[0-9a-f]{8}-\d{5}\.ndjson\.zst$").unwrap();
    assert!(
        keys.iter().any(|k| template_re.is_match(k)),
        "no destination key matched template `out/{{prefix}}/{{run_id}}-{{seq}}.ndjson.zst`; got: {keys:?}"
    );
    Ok(())
}

/// List every object under `prefix` in `bucket`, fetch each, zstd-decode it,
/// and return the concatenated NDJSON lines along with the object keys.
async fn list_and_decode_zst(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut keys = Vec::new();
    let mut continuation = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(token) = continuation.take() {
            req = req.continuation_token(token);
        }
        let resp = req.send().await?;
        if let Some(contents) = resp.contents {
            for obj in contents {
                if let Some(k) = obj.key {
                    keys.push(k);
                }
            }
        }
        if resp.is_truncated.unwrap_or(false) {
            continuation = resp.next_continuation_token;
        } else {
            break;
        }
    }

    let mut lines = Vec::new();
    for key in &keys {
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        let body = resp.body.collect().await?.to_vec();
        let plain = zstd::stream::decode_all(body.as_slice())?;
        for line in String::from_utf8(plain)?.lines() {
            if !line.is_empty() {
                lines.push(line.to_string());
            }
        }
    }
    Ok((lines, keys))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progress_reports_filter_volume_metrics() {
    skip_unless_docker!();
    if let Err(e) = run_progress_metrics_test().await {
        panic!("progress_reports_filter_volume_metrics failed: {e:#}");
    }
}

async fn run_progress_metrics_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();
    let staged = build_fixture(DATE, HOURS);
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let output_dir = workdir.path().join("out");
    std::fs::create_dir_all(&output_dir)?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, Some(&output_dir))?;

    let mut cmd = Command::cargo_bin("bucket-scrapper")?;
    for (k, v) in garage.env_for_scrapper() {
        cmd.env(k, v);
    }
    let output = cmd
        .arg("--config")
        .arg(&config_path)
        .arg("--region")
        .arg("garage")
        .arg("--start")
        .arg("2026-01-01T10:00:00Z")
        .arg("--end")
        .arg("2026-01-01T11:00:00Z")
        .arg("--line-pattern-regex")
        .arg(PATTERN)
        .arg("--filter")
        .arg(r"service-.*\.(json|json\.gz|json\.zst)$")
        .arg("--max-parallel")
        .arg("4")
        .arg("--progress-interval")
        .arg("0.1")
        .arg("--log-format")
        .arg("json")
        .timeout(std::time::Duration::from_secs(120))
        .output()?;
    assert!(
        output.status.success(),
        "scrapper exited non-zero ({:?}); stderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    // tracing's JSON subscriber writes to stdout by default.
    let logs = String::from_utf8(output.stdout)?;
    let progress_records: Vec<serde_json::Value> = logs
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| {
            v.get("fields")
                .and_then(|f| f.get("message"))
                .and_then(|m| m.as_str())
                == Some("Search progress")
        })
        .collect();
    assert!(
        !progress_records.is_empty(),
        "no Search progress records found in logs:\n{logs}"
    );

    // The expected fixture has 16 included lines per hour × 2 hours = 32 lines,
    // exactly half of which match the ERROR pattern (alternating INFO/ERROR).
    let expected = expected_matches(&staged, PATTERN);
    assert_eq!(expected.len(), 16, "fixture sanity check");

    let final_record = progress_records.last().unwrap();
    let fields = final_record.get("fields").expect("fields object");

    for key in [
        "filter_lines_in",
        "filter_in_mb",
        "filter_in_mbps",
        "match_mb",
        "matched_ratio_lines",
        "matched_ratio_bytes",
        "matches",
    ] {
        assert!(
            fields.get(key).is_some(),
            "missing progress field `{key}` in record: {final_record}"
        );
    }

    let filter_lines_in = fields["filter_lines_in"].as_u64().unwrap();
    let matches = fields["matches"].as_u64().unwrap();
    assert_eq!(
        filter_lines_in, 32,
        "filter_lines_in should equal total included lines"
    );
    assert_eq!(matches, 16, "should match 16 ERROR lines");

    let parse_ratio = |k: &str| -> f64 {
        fields[k]
            .as_str()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0)
    };
    let ratio_lines = parse_ratio("matched_ratio_lines");
    let ratio_bytes = parse_ratio("matched_ratio_bytes");
    assert!(
        (ratio_lines - 0.5).abs() < 1e-3,
        "matched_ratio_lines should be 0.5, got {ratio_lines}"
    );
    // ERROR lines are slightly longer than INFO lines, so byte ratio > line ratio.
    assert!(
        ratio_bytes > ratio_lines,
        "byte ratio ({ratio_bytes}) should exceed line ratio ({ratio_lines}) given ERROR lines are longer"
    );
    assert!(
        ratio_bytes < 1.0,
        "matched_ratio_bytes should be < 1, got {ratio_bytes}"
    );

    Ok(())
}

#[track_caller]
fn assert_multiset_eq(got: &[String], expected: &[String]) {
    let g: BTreeSet<&String> = got.iter().collect();
    let e: BTreeSet<&String> = expected.iter().collect();
    assert_eq!(
        got.len(),
        expected.len(),
        "line counts differ: got {} expected {}",
        got.len(),
        expected.len()
    );
    let missing: Vec<_> = e.difference(&g).collect();
    let extra: Vec<_> = g.difference(&e).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "set mismatch — missing: {missing:?} | extra: {extra:?}"
    );
}
