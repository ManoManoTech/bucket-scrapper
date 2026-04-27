//! End-to-end pipeline tests: real Garage S3 source + (file output | nginx HTTP receiver).
//!
//! These spin up Docker containers via `testcontainers`. They are skipped when no
//! Docker daemon is reachable, so `cargo test` stays green on CI hosts without Docker.

mod e2e;

use anyhow::Result;
use assert_cmd::Command;
use e2e::fixtures::{build_fixture, expected_matches, seed_bucket};
use e2e::garage::start_garage;
use e2e::nginx::start_nginx;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;
use tempfile::TempDir;

const BUCKET: &str = "logs-bucket";
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
        yaml.push_str(&format!("output_dir: {}\n", dir.display()));
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
        .arg("--http-output")
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
