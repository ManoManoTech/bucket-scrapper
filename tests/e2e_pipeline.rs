//! End-to-end pipeline tests: real Garage S3 source + (file output | nginx HTTP receiver).
//!
//! These spin up Docker containers via `testcontainers`. They are skipped when no
//! Docker daemon is reachable, so `cargo test` stays green on CI hosts without Docker.

mod e2e;

use anyhow::{anyhow, Result};
use assert_cmd::Command;
use aws_sdk_s3::Client as S3Client;
use e2e::fixtures::{build_fixture, expected_matches, seed_bucket, Encoding, StagedObject};
use e2e::garage::start_garage;
use e2e::nginx::start_nginx;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;
use std::sync::LazyLock;
use tempfile::TempDir;

const BUCKET: &str = "logs-bucket";
const RESULTS_BUCKET: &str = "results-bucket";
const DATE: &str = "20260101"; // 2026-01-01
const HOURS: &[&str] = &["10", "11"];
const PATTERN: &str = "ERROR";

/// Output codec under test in an e2e run. Mirrors the production
/// `Codec`/`CodecFormat` axis without depending on the lib's tagged
/// enum (which would force the test to import private fields).
#[derive(Clone, Copy, Debug)]
enum TestCodec {
    Zstd,
    Gzip,
    None,
}

impl TestCodec {
    fn cli_format(self) -> &'static str {
        match self {
            TestCodec::Zstd => "zstd",
            TestCodec::Gzip => "gzip",
            TestCodec::None => "none",
        }
    }

    /// File extension the codec produces (no leading dot). Empty for plaintext.
    fn extension(self) -> &'static str {
        match self {
            TestCodec::Zstd => "zst",
            TestCodec::Gzip => "gz",
            TestCodec::None => "",
        }
    }

    /// Wire `Content-Encoding` value, or `None` for plaintext.
    fn content_encoding(self) -> Option<&'static str> {
        match self {
            TestCodec::Zstd => Some("zstd"),
            TestCodec::Gzip => Some("gzip"),
            TestCodec::None => None,
        }
    }

    fn decode(self, body: &[u8]) -> Result<Vec<u8>> {
        match self {
            TestCodec::Zstd => Ok(zstd::stream::decode_all(body)?),
            TestCodec::Gzip => {
                let mut decoder = flate2::read::GzDecoder::new(body);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            TestCodec::None => Ok(body.to_vec()),
        }
    }
}

fn write_config_yaml(path: &Path, output_dir: Option<&Path>) -> Result<()> {
    write_config_yaml_with_codec(path, output_dir, None)
}

/// Like [`write_config_yaml`] but bakes the compression block into the
/// config-file `outputs:` entry. Needed for the file e2e tests, where
/// the YAML carries `outputs:` and CLI codec flags would be rejected by
/// the resolver (config-driven mode forbids per-output CLI flags).
fn write_config_yaml_with_codec(
    path: &Path,
    output_dir: Option<&Path>,
    codec: Option<TestCodec>,
) -> Result<()> {
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
        if let Some(c) = codec {
            yaml.push_str(&format!(
                "    compression:\n      format: {}\n",
                c.cli_format()
            ));
        }
    }
    std::fs::write(path, yaml)?;
    Ok(())
}

fn decode_file(path: &Path, codec: TestCodec) -> Result<String> {
    let bytes = std::fs::read(path)?;
    let plain = codec.decode(&bytes)?;
    Ok(String::from_utf8(plain)?)
}

/// Walk `root` and collect every file whose name ends with the codec's
/// extension. For `TestCodec::None` (no extension) we accept any
/// extension-less file, which works for the test fixture's templates
/// (`{prefix}.{ext}` collapses to `{prefix}`).
fn collect_outputs(root: &Path, codec: TestCodec) -> Vec<std::path::PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    let ext = codec.extension();
    let mut all = Vec::new();
    walk(root, &mut all);
    all.into_iter()
        .filter(|p| {
            let actual = p.extension().and_then(|s| s.to_str()).unwrap_or("");
            actual == ext
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_output_end_to_end_zstd() {
    skip_unless_docker!();
    if let Err(e) = run_file_test(TestCodec::Zstd).await {
        panic!("file_output_end_to_end_zstd failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_output_end_to_end_gzip() {
    skip_unless_docker!();
    if let Err(e) = run_file_test(TestCodec::Gzip).await {
        panic!("file_output_end_to_end_gzip failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_output_end_to_end_plaintext() {
    skip_unless_docker!();
    if let Err(e) = run_file_test(TestCodec::None).await {
        panic!("file_output_end_to_end_plaintext failed: {e:#}");
    }
}

async fn run_file_test(codec: TestCodec) -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();
    let staged = build_fixture(DATE, HOURS);
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let output_dir = workdir.path().join("out");
    std::fs::create_dir_all(&output_dir)?;
    let config_path = workdir.path().join("config.yaml");
    // The codec must live in the YAML, not on the CLI: config-driven
    // mode (an `outputs:` block is present) forbids per-output CLI flags.
    write_config_yaml_with_codec(&config_path, Some(&output_dir), Some(codec))?;

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

    let outputs = collect_outputs(&output_dir, codec);
    assert!(
        !outputs.is_empty(),
        "no outputs (ext={:?}) written under {}",
        codec.extension(),
        output_dir.display()
    );

    let mut received: Vec<String> = Vec::new();
    for f in &outputs {
        let s = decode_file(f, codec)?;
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
async fn chunked_download_end_to_end() {
    skip_unless_docker!();
    if let Err(e) = run_chunked_test().await {
        panic!("chunked_download_end_to_end failed: {e:#}");
    }
}

/// Seeds one multi-MB object and runs with `--download-chunk-size-mb 1`, so the
/// object is fetched as several concurrent byte-ranges and reassembled in order.
/// Asserts the matched output equals the expected ERROR lines — i.e. ranged
/// fetch + in-order reassembly reconstructs the object byte-exactly.
async fn run_chunked_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();

    // ~4 MB plaintext object with high-entropy filler (so it stays large) and
    // alternating INFO/ERROR lines. Plain encoding ⇒ on-disk size ≈ plaintext,
    // well above the 1 MB chunk size ⇒ multiple ranges.
    let mut lines = Vec::new();
    for i in 0..25_000usize {
        let level = if i % 2 == 0 { "INFO" } else { "ERROR" };
        let filler: String = (0..120)
            .map(|j| char::from(b'a' + (((i * 7 + j * 13) % 26) as u8)))
            .collect();
        lines.push(format!(
            r#"{{"seq":{i},"level":"{level}","f":"{filler}","msg":"{level} #{i}"}}"#
        ));
    }
    let staged = vec![StagedObject {
        key: format!("logs/dt={DATE}/hour=10/service-big-001.json"),
        lines,
        encoding: Encoding::Plain,
    }];
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let output_dir = workdir.path().join("out");
    std::fs::create_dir_all(&output_dir)?;
    let config_path = workdir.path().join("config.yaml");
    // Plain output codec so we can read matches back verbatim.
    write_config_yaml_with_codec(&config_path, Some(&output_dir), Some(TestCodec::None))?;

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
        .arg("--download-chunk-size-mb")
        .arg("1")
        .arg("--max-input-buffer-memory-mb")
        .arg("16")
        .arg("--decode-input-buffer-mb")
        .arg("2")
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let outputs = collect_outputs(&output_dir, TestCodec::None);
    assert!(!outputs.is_empty(), "no outputs written");
    let mut received: Vec<String> = Vec::new();
    for f in &outputs {
        for line in decode_file(f, TestCodec::None)?.lines() {
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
async fn file_output_end_to_end_sharded() {
    skip_unless_docker!();
    if let Err(e) = run_sharded_test().await {
        panic!("file_output_end_to_end_sharded failed: {e:#}");
    }
}

/// Three independent scrapper invocations against the same Garage fixture,
/// each owning one of three shards. The union of their outputs must equal the
/// no-shard expected set (coverage), and total line count across shards must
/// equal the expected set's size (disjointness — no line was processed twice).
async fn run_sharded_test() -> Result<()> {
    const SHARD_COUNT: usize = 3;
    let codec = TestCodec::Zstd;

    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();
    let staged = build_fixture(DATE, HOURS);
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");

    let mut per_shard_lines: Vec<Vec<String>> = Vec::with_capacity(SHARD_COUNT);

    for shard_number in 0..SHARD_COUNT {
        let output_dir = workdir.path().join(format!("out-{shard_number}"));
        std::fs::create_dir_all(&output_dir)?;
        write_config_yaml_with_codec(&config_path, Some(&output_dir), Some(codec))?;

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
            .arg("--shard-count")
            .arg(SHARD_COUNT.to_string())
            .arg("--shard-number")
            .arg(shard_number.to_string())
            .timeout(std::time::Duration::from_secs(120))
            .assert()
            .success();

        let outputs = collect_outputs(&output_dir, codec);
        let mut lines = Vec::new();
        for f in &outputs {
            for line in decode_file(f, codec)?.lines() {
                if !line.is_empty() {
                    lines.push(line.to_string());
                }
            }
        }
        per_shard_lines.push(lines);
    }

    let expected = expected_matches(&staged, PATTERN);

    let union: Vec<String> = per_shard_lines.iter().flatten().cloned().collect();
    assert_multiset_eq(&union, &expected);

    let total: usize = per_shard_lines.iter().map(|v| v.len()).sum();
    assert_eq!(
        total,
        expected.len(),
        "shards overlap: total lines across shards = {}, expected = {}",
        total,
        expected.len()
    );

    // The fixture has 6 partition-eligible objects (2 hours × 3 included
    // services) → 2 objects per shard with SHARD_COUNT=3. A zero-length shard
    // would mean the modulo math is off or the shard filter ran before the
    // listing was assembled.
    for (i, lines) in per_shard_lines.iter().enumerate() {
        assert!(!lines.is_empty(), "shard {i} produced no lines");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_output_end_to_end_zstd() {
    skip_unless_docker!();
    if let Err(e) = run_http_test(TestCodec::Zstd).await {
        panic!("http_output_end_to_end_zstd failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_output_end_to_end_gzip() {
    skip_unless_docker!();
    if let Err(e) = run_http_test(TestCodec::Gzip).await {
        panic!("http_output_end_to_end_gzip failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_output_end_to_end_plaintext() {
    skip_unless_docker!();
    if let Err(e) = run_http_test(TestCodec::None).await {
        panic!("http_output_end_to_end_plaintext failed: {e:#}");
    }
}

async fn run_http_test(codec: TestCodec) -> Result<()> {
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
        .arg("--compression-format")
        .arg(codec.cli_format())
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let dumps = nginx.collect_dumps().await?;
    let mut received: Vec<String> = Vec::new();
    for body in &dumps {
        let plain = codec.decode(body)?;
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

/// E2E proof that the HTTP sink under `--output-format json_array` produces
/// **one valid JSON array per uploaded batch** — not a single concatenated
/// blob, not NDJSON, not partial fragments. The bulky fixture + tiny
/// batch_max_mb force multiple batches so the across-batch boundary is
/// covered (the unit tests only see a single batch).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_output_end_to_end_json_array() {
    skip_unless_docker!();
    if let Err(e) = run_http_json_array_test().await {
        panic!("http_output_end_to_end_json_array failed: {e:#}");
    }
}

async fn run_http_json_array_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    let s3 = garage.s3_client();
    // Bulky fixture: lots of matching JSON lines per hour, so the
    // sub-MB batch threshold rolls over several times.
    let mut staged = build_fixture(DATE, HOURS);
    for hour in HOURS {
        staged.push(bulk_match_object(DATE, hour, 5_000));
    }
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
        .arg("--http-batch-max-mb")
        // ~50 KB — small enough to force several rollovers given ~10k
        // matching lines × ~140 B each = ~1.4 MB plaintext.
        .arg("0.05")
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        // Plaintext on the wire: keep the dumped bodies directly parseable
        // as JSON without first decompressing. The framing layer is the
        // axis under test, not compression.
        .arg("--compression-format")
        .arg("none")
        .arg("--output-format")
        .arg("json_array")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let dumps = nginx.collect_dumps().await?;
    assert!(
        dumps.len() >= 2,
        "expected ≥2 batches to cover the multi-array case; got {}",
        dumps.len()
    );

    let mut received: Vec<String> = Vec::new();
    for (idx, body) in dumps.iter().enumerate() {
        // Every body must be its own standalone valid JSON array.
        let arr: Vec<serde_json::Value> = serde_json::from_slice(body).map_err(|e| {
            anyhow!(
                "batch {idx} body is not a valid JSON array: {e}; first 200 bytes: {:?}",
                String::from_utf8_lossy(&body[..body.len().min(200)])
            )
        })?;
        assert!(
            !arr.is_empty(),
            "batch {idx} should not be an empty array (the sink must not emit `[]`)"
        );
        // Canonicalize via `serde_json::to_string` so the multiset
        // comparison ignores key ordering inside each object (the
        // framing strips a trailing `\n` but otherwise passes bytes
        // through unchanged — only the parse/reserialize step on our
        // side reorders).
        for item in arr {
            received.push(serde_json::to_string(&item)?);
        }
    }

    let expected: Vec<String> = expected_matches(&staged, PATTERN)
        .into_iter()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!("expected line not parseable as JSON: {e}; line={line}")
            });
            serde_json::to_string(&v).unwrap()
        })
        .collect();
    assert_multiset_eq(&received, &expected);

    drop(dump_root); // keep TempDir alive until end
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end_no_batching() {
    skip_unless_docker!();
    // Default mode: --s3-output-batch-max-mb omitted. Each source prefix
    // (one per hour in the fixture) collapses to exactly one output object.
    if let Err(e) = run_s3_test(TestCodec::Zstd, None, HOURS.len()).await {
        panic!("s3_output_end_to_end_no_batching failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end_batched() {
    skip_unless_docker!();
    // Tiny rollover threshold: each prefix splits into multiple sequence
    // numbers, so we expect strictly more output objects than source prefixes.
    if let Err(e) = run_s3_test(TestCodec::Zstd, Some("0.00001"), HOURS.len() + 1).await {
        panic!("s3_output_end_to_end_batched failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end_gzip() {
    skip_unless_docker!();
    if let Err(e) = run_s3_test(TestCodec::Gzip, None, HOURS.len()).await {
        panic!("s3_output_end_to_end_gzip failed: {e:#}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end_plaintext() {
    skip_unless_docker!();
    if let Err(e) = run_s3_test(TestCodec::None, None, HOURS.len()).await {
        panic!("s3_output_end_to_end_plaintext failed: {e:#}");
    }
}

/// E2E proof that the S3 sink under `--output-format json_array` writes
/// **one valid JSON array per uploaded object** — not a single concatenated
/// blob across rollovers. Forces rollover via `--s3-output-batch-max-mb`
/// so the across-object boundary is covered (the unit tests only see a
/// single batch).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end_json_array() {
    skip_unless_docker!();
    if let Err(e) = run_s3_json_array_test().await {
        panic!("s3_output_end_to_end_json_array failed: {e:#}");
    }
}

async fn run_s3_json_array_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    garage.create_bucket(RESULTS_BUCKET).await?;

    let s3 = garage.s3_client();
    // Bulky fixture to feed enough plaintext per prefix that the size-based
    // rollover actually fires (zstd's internal block buffer would otherwise
    // hide all the bytes until end-of-run; plaintext codec sidesteps that).
    let mut staged = build_fixture(DATE, HOURS);
    for hour in HOURS {
        staged.push(bulk_match_object(DATE, hour, 5_000));
    }
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, None)?;

    // Use `.json` instead of `.ndjson` in the literal portion to reflect
    // the new framing; the codec extension still drives the trailing
    // `.{ext}` (empty for plaintext, so the key ends at `.json`).
    let key_template = "out/{prefix}/{run_id}-{seq}.json";

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
        .arg("--s3-output-key-template")
        .arg(key_template)
        // Plaintext: predictable rollover and direct JSON parsing of object bodies.
        .arg("--compression-format")
        .arg("none")
        // Aggressive rollover so several objects per prefix are produced.
        .arg("--s3-output-batch-max-mb")
        .arg("0.05")
        .arg("--output-format")
        .arg("json_array")
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    // List every uploaded object; each body must be its own JSON array.
    let mut keys = Vec::new();
    let mut continuation = None;
    loop {
        let mut req = s3.list_objects_v2().bucket(RESULTS_BUCKET).prefix("out/");
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

    assert!(
        keys.len() > HOURS.len(),
        "expected rollover to produce more objects than source prefixes ({}); got {} keys: {keys:?}",
        HOURS.len(),
        keys.len()
    );
    assert!(
        keys.iter().any(|k| !k.contains("-00000.")),
        "expected at least one rolled-over object (seq > 0); got: {keys:?}"
    );

    let mut received: Vec<String> = Vec::new();
    for key in &keys {
        let resp = s3
            .get_object()
            .bucket(RESULTS_BUCKET)
            .key(key)
            .send()
            .await?;
        let body = resp.body.collect().await?.to_vec();
        let arr: Vec<serde_json::Value> = serde_json::from_slice(&body).map_err(|e| {
            anyhow!(
                "object {key} body is not a valid JSON array: {e}; first 200 bytes: {:?}",
                String::from_utf8_lossy(&body[..body.len().min(200)])
            )
        })?;
        assert!(
            !arr.is_empty(),
            "object {key} should not be an empty array (the sink must not upload `[]`)"
        );
        for item in arr {
            received.push(serde_json::to_string(&item)?);
        }
    }

    // Canonicalize the expected lines through the same parse/reserialize
    // path so key ordering in the comparison is consistent.
    let expected: Vec<String> = expected_matches(&staged, PATTERN)
        .into_iter()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!("expected line not parseable as JSON: {e}; line={line}")
            });
            serde_json::to_string(&v).unwrap()
        })
        .collect();
    assert_multiset_eq(&received, &expected);

    Ok(())
}

/// Exercises the multipart upload path through the AWS transfer manager.
///
/// Strategy: feed enough plaintext per prefix that each per-prefix batch
/// crosses the 5 MiB multipart threshold. With `compression.format=none`
/// the encoder's output buffer grows 1:1 with input, so we can hit the
/// threshold predictably without depending on zstd block-flush timing.
///
/// We can't directly observe Garage's wire calls from the test, but a
/// broken multipart implementation would either fail the upload, reorder
/// parts, or corrupt the resulting object. Round-tripping the body and
/// asserting the line set is therefore a sufficient end-to-end check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_end_to_end_multipart() {
    skip_unless_docker!();
    if let Err(e) = run_multipart_test().await {
        panic!("s3_output_end_to_end_multipart failed: {e:#}");
    }
}

async fn run_multipart_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    garage.create_bucket(RESULTS_BUCKET).await?;

    let s3 = garage.s3_client();
    // Each line is ~140 bytes; 50_000 ERROR lines per hour ≈ 7 MB plaintext.
    // With format=none the per-prefix encoder buffer is plaintext bytes,
    // so each prefix's single batch sits comfortably above the 5 MiB
    // multipart threshold and the upload goes through TM's multipart path.
    let mut staged = build_fixture(DATE, HOURS);
    for hour in HOURS {
        staged.push(bulk_match_object(DATE, hour, 50_000));
    }
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, None)?;

    let key_template = "out/{prefix}/{run_id}-{seq}.ndjson.{ext}";

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
        .arg("--s3-output-key-template")
        .arg(key_template)
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        // Plaintext: encoder buffer == ingested bytes, so threshold checks
        // are predictable rather than gated on zstd's internal block timing.
        .arg("--compression-format")
        .arg("none")
        // Defaults are already 5/5 but pin them explicitly so the test
        // doesn't drift if defaults change later.
        .arg("--s3-output-multipart-threshold-mb")
        .arg("5")
        .arg("--s3-output-multipart-part-mb")
        .arg("5")
        .arg("--s3-output-multipart-concurrency")
        .arg("4")
        .timeout(std::time::Duration::from_secs(180))
        .assert()
        .success();

    let (received, keys) = list_and_decode(&s3, RESULTS_BUCKET, "out/", TestCodec::None).await?;
    let expected = expected_matches(&staged, PATTERN);
    assert_multiset_eq(&received, &expected);

    // Sanity: at least one object should be ≥ 5 MiB, proving the test
    // actually crossed the multipart threshold (rather than silently
    // falling back to single PutObject).
    assert!(
        !keys.is_empty(),
        "no objects produced — fixture or run is broken"
    );
    let mut max_size = 0u64;
    for key in &keys {
        let head = s3
            .head_object()
            .bucket(RESULTS_BUCKET)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("head_object {key}: {e}"))?;
        let len = head.content_length().unwrap_or(0) as u64;
        if len > max_size {
            max_size = len;
        }
    }
    let five_mib = 5 * 1024 * 1024;
    assert!(
        max_size >= five_mib,
        "expected at least one batch ≥ 5 MiB (multipart threshold); got max {max_size} bytes — \
         multipart path was not exercised"
    );

    Ok(())
}

/// Regression guard for the OOM mode: in default S3-sink mode (no
/// `--s3-output-batch-max-mb`), the sink must **not** buffer the entire
/// run's matches in memory per prefix. Asserts that `peak_inflight_bytes`
/// (sum across per-prefix encoder buffers, sampled on every ingest) stays
/// below a small cap regardless of how much plaintext per prefix is
/// matched. Pre-streaming code accumulates ~28 MB/prefix → 56 MB total
/// resident and fails this. Streaming code ships bytes to TM as they're
/// produced and stays ≤ part_size × in-flight uploads + slack.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_does_not_buffer_whole_run_in_memory() {
    skip_unless_docker!();
    if let Err(e) = run_s3_oom_regression_test().await {
        panic!("s3_output_does_not_buffer_whole_run_in_memory failed: {e:#}");
    }
}

async fn run_s3_oom_regression_test() -> Result<()> {
    let garage = start_garage(BUCKET).await?;
    garage.create_bucket(RESULTS_BUCKET).await?;

    let s3 = garage.s3_client();
    // ~200K matching lines × ~140 B per line ≈ 28 MB plaintext per prefix,
    // 2 prefixes → ~56 MB total matched plaintext. Plaintext codec so the
    // encoder's output buffer ≈ ingested bytes (no zstd block-buffer
    // ambiguity), making the buffering question crisp.
    let mut staged = Vec::new();
    for hour in HOURS {
        staged.push(bulk_match_object(DATE, hour, 200_000));
    }
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, None)?;

    let key_template = "out/{prefix}/{run_id}-{seq}.ndjson";

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
        .arg("--output")
        .arg("s3")
        .arg("--s3-output-bucket")
        .arg(RESULTS_BUCKET)
        .arg("--s3-output-key-template")
        .arg(key_template)
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        // Plaintext codec: buffered_len == ingested bytes, so the buffer
        // measurement reflects actual matched plaintext rather than
        // compression-block timing.
        .arg("--compression-format")
        .arg("none")
        // NB: deliberately omit --s3-output-batch-max-mb — this is the
        // default mode that today buffers the whole run per prefix.
        .timeout(std::time::Duration::from_secs(240))
        .output()?;
    assert!(
        output.status.success(),
        "scrapper exited non-zero ({:?}); stderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let logs = String::from_utf8(output.stdout)?;
    let completion = logs
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v.get("message").and_then(|m| m.as_str()) == Some("Search completed"))
        .expect("Search completed record not found in JSON logs");

    let extras_str = completion
        .get("extras")
        .and_then(|v| v.as_str())
        .expect("extras field missing or not a string");
    let extras: serde_json::Value =
        serde_json::from_str(extras_str).expect("extras field is not valid JSON");

    let peak = extras
        .get("peak_inflight_bytes")
        .and_then(|v| v.as_u64())
        .expect("peak_inflight_bytes missing from extras");

    // Cap: 2 prefixes × `multipart_part_mb` (5 MiB) + slack ≈ 12 MB.
    // Streaming code stays well under this; pre-streaming code with ~28 MB
    // per prefix accumulated will blow past it by ~5×.
    let cap_bytes: u64 = 12 * 1_000_000;
    assert!(
        peak <= cap_bytes,
        "S3 sink buffered too much: peak_inflight_bytes={peak} > {cap_bytes} \
         (per-prefix matched plaintext × num prefixes was ~56 MB; the sink \
         should stream rather than accumulate)"
    );

    // Sanity: confirm the run actually moved real data — guards against
    // the test passing because nothing matched.
    let lines_recorded = completion
        .get("lines_recorded")
        .and_then(|v| v.as_u64())
        .expect("lines_recorded missing");
    assert_eq!(
        lines_recorded, 400_000,
        "expected 200_000 matches × 2 prefixes = 400_000 lines"
    );

    Ok(())
}

async fn run_s3_test(
    codec: TestCodec,
    batch_max_mb: Option<&str>,
    min_expected_objects: usize,
) -> Result<()> {
    // Single Garage instance, two buckets: one source, one destination.
    // Same credentials, so the scrapper's source S3 client also reaches the
    // destination — no per-output endpoint/credentials override needed.
    let garage = start_garage(BUCKET).await?;
    garage.create_bucket(RESULTS_BUCKET).await?;

    let s3 = garage.s3_client();
    let mut staged = build_fixture(DATE, HOURS);
    if batch_max_mb.is_some() {
        // The default fixture is too small to push any plaintext past zstd's
        // internal block buffer, so the rollover threshold (which inspects
        // the encoder's *output* Vec) never sees any bytes. Add one bulky
        // matching object per hour to force several block flushes per
        // prefix and exercise rollover.
        for hour in HOURS {
            staged.push(bulk_match_object(DATE, hour, 5_000));
        }
    }
    seed_bucket(&s3, BUCKET, &staged).await?;

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_config_yaml(&config_path, None)?;

    // Use {ext} so the same template adapts to whatever codec the test uses.
    let key_template = "out/{prefix}/{run_id}-{seq}.ndjson.{ext}";

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
        .arg("--s3-output-key-template")
        .arg(key_template)
        .arg("--max-parallel")
        .arg("4")
        .arg("--log-format")
        .arg("json")
        .arg("--compression-format")
        .arg(codec.cli_format());
    if let Some(mb) = batch_max_mb {
        cmd.arg("--s3-output-batch-max-mb").arg(mb);
    }
    cmd.timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let (received, keys) = list_and_decode(&s3, RESULTS_BUCKET, "out/", codec).await?;
    let expected = expected_matches(&staged, PATTERN);
    assert_multiset_eq(&received, &expected);

    // Each returned key should match the rendered template. With codec
    // `none` the trailing `.{ext}` collapses, so the suffix is `.ndjson`.
    let suffix_re = match codec {
        TestCodec::None => regex::Regex::new(r"^out/.+/[0-9a-f]{8}-\d{5}\.ndjson$").unwrap(),
        TestCodec::Zstd => regex::Regex::new(r"^out/.+/[0-9a-f]{8}-\d{5}\.ndjson\.zst$").unwrap(),
        TestCodec::Gzip => regex::Regex::new(r"^out/.+/[0-9a-f]{8}-\d{5}\.ndjson\.gz$").unwrap(),
    };
    for key in &keys {
        assert!(
            suffix_re.is_match(key),
            "destination key did not match template (codec={:?}): {key}",
            codec
        );
    }

    // Verify the wire `Content-Encoding` reflects the codec. For plaintext
    // it must be absent (not `identity`).
    for key in &keys {
        let head = s3
            .head_object()
            .bucket(RESULTS_BUCKET)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow!("head_object {key}: {e}"))?;
        let actual = head.content_encoding();
        match codec.content_encoding() {
            Some(expected) => assert_eq!(
                actual,
                Some(expected),
                "object {key} should carry Content-Encoding: {expected}, got {actual:?}"
            ),
            None => assert!(
                actual.is_none() || actual == Some(""),
                "object {key} should have no Content-Encoding for plaintext codec, got {actual:?}"
            ),
        }
    }

    match batch_max_mb {
        // Without batching: exactly one object per source prefix, all
        // sequence-zero (no rollover ever happened).
        None => {
            assert_eq!(
                keys.len(),
                min_expected_objects,
                "no-batching mode should produce exactly one object per source prefix; got: {keys:?}"
            );
            for key in &keys {
                assert!(
                    key.contains("-00000."),
                    "no-batching mode must keep seq=00000 (no rollover); got: {key}"
                );
            }
        }
        // With aggressive rollover: strictly more objects than prefixes,
        // and at least one object with seq > 0.
        Some(_) => {
            assert!(
                keys.len() > HOURS.len(),
                "batched mode should produce more objects than source prefixes ({}); got {} keys: {:?}",
                HOURS.len(),
                keys.len(),
                keys
            );
            assert!(
                keys.iter().any(|k| !k.contains("-00000.")),
                "batched mode should produce at least one rolled-over object (seq > 0); got: {keys:?}"
            );
            let _ = min_expected_objects; // bound is implicit via the `>` check above
        }
    }
    Ok(())
}

/// One bulky `service-bulk-*.json` object containing only ERROR lines. Plain
/// text (uncompressed body) so the scrapper sees the lines verbatim. Keyed
/// under the standard `logs/dt=…/hour=…/` prefix, satisfying the e2e key
/// filter regex.
fn bulk_match_object(date: &str, hour: &str, n_lines: usize) -> StagedObject {
    bulk_match_object_n(date, hour, 0, n_lines)
}

/// Like [`bulk_match_object`] but creates the `obj_idx`-th object within
/// the prefix's hour. Used by the RSS regression tests to put multiple
/// objects under one prefix (production-realistic shape) so the
/// orchestrator's sort+FIFO download clustering can keep concurrent
/// open uploads small.
fn bulk_match_object_n(date: &str, hour: &str, obj_idx: usize, n_lines: usize) -> StagedObject {
    let prefix = format!("logs/dt={date}/hour={hour}");
    let lines: Vec<String> = (0..n_lines)
        .map(|i| {
            format!(
                r#"{{"service":"bulk-{obj_idx}","hour":"{hour}","seq":{i},"level":"ERROR","msg":"ERROR bulk-{obj_idx} row #{i} for rollover test, padding the line a bit so blocks fill faster"}}"#
            )
        })
        .collect();
    StagedObject {
        key: format!("{prefix}/service-bulk-{obj_idx:03}.json"),
        lines,
        encoding: Encoding::Plain,
    }
}

/// List every object under `prefix` in `bucket`, fetch each, decode it
/// with the given codec, and return the concatenated NDJSON lines along
/// with the object keys.
async fn list_and_decode(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    codec: TestCodec,
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
        let plain = codec.decode(&body)?;
        for line in String::from_utf8(plain)?.lines() {
            if !line.is_empty() {
                lines.push(line.to_string());
            }
        }
    }
    Ok((lines, keys))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn void_output_end_to_end() {
    skip_unless_docker!();
    if let Err(e) = run_void_test().await {
        panic!("void_output_end_to_end failed: {e:#}");
    }
}

async fn run_void_test() -> Result<()> {
    // Void output writes nothing; the only observable signal is the structured
    // "Search completed" log line. Capture stdout, parse it, assert counters.
    let garage = start_garage(BUCKET).await?;
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
        .arg("--output")
        .arg("void")
        .arg("--max-parallel")
        .arg("4")
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

    let logs = String::from_utf8(output.stdout)?;
    let completion = logs
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v.get("message").and_then(|m| m.as_str()) == Some("Search completed"))
        .expect("Search completed record not found in JSON logs");

    let fields = &completion;
    let expected = expected_matches(&staged, PATTERN);
    assert_eq!(expected.len(), 16, "fixture sanity check");

    assert_eq!(
        fields.get("output").and_then(|v| v.as_str()),
        Some("void"),
        "output should be `void` in completion record: {completion}"
    );
    assert_eq!(
        fields.get("matched_lines").and_then(|v| v.as_u64()),
        Some(16),
        "matched_lines should be 16: {completion}"
    );
    assert_eq!(
        fields.get("lines_recorded").and_then(|v| v.as_u64()),
        Some(16),
        "lines_recorded should be 16 (every match counted): {completion}"
    );
    assert_eq!(
        fields.get("lines_dropped").and_then(|v| v.as_u64()),
        Some(0),
        "lines_dropped should be 0 for void output: {completion}"
    );
    let wrote_mb = fields
        .get("wrote_compressed_mb")
        .and_then(|v| v.as_f64())
        .expect("wrote_compressed_mb missing");
    assert_eq!(
        wrote_mb, 0.0,
        "void output must not produce compressed bytes: {completion}"
    );
    Ok(())
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
        .filter(|v| v.get("message").and_then(|m| m.as_str()) == Some("Search progress"))
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
    let fields = final_record;

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

// =====================================================================
// High-concurrency RSS regression tests
// =====================================================================
//
// Production hit OOM on the S3 sink under a workload that produced lots
// of matched bytes across many source prefixes. The prior in-buffer test
// (`s3_output_does_not_buffer_whole_run_in_memory`) only proves *our own*
// streaming buffer is small — it can't see TM's internal part staging or
// codec/encoder per-upload state, both of which scale with the number of
// concurrent active uploads.
//
// These tests force that exact shape: many concurrent prefixes (~64),
// substantial matched bytes per prefix (~24 MB each, plaintext), and read
// the scrapper's own `rss_mb` field from its `Search progress` log
// records (which already exists — see `progress.rs`). Assertion is on
// peak process RSS, the only metric that actually reflects the OOM in
// production.

/// Serializes the two RSS variants. `cargo test` runs `#[tokio::test]`s on
/// parallel threads by default; without this lock both memory-heavy variants
/// (each: a Garage container + the scrapper subprocess) run at once and the
/// aggregate OOM-kills the 7 GB GitHub runner (SIGKILL/signal 9, empty stderr).
/// A `tokio::sync::Mutex` is held cleanly across `.await`s and serializes even
/// across the per-test runtimes, so only one variant is resident at a time.
static RSS_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

const RSS_TEST_NUM_PREFIXES: usize = 64;
/// Objects per prefix — production-shaped (multiple service logs per
/// hour). Sort+FIFO download means at most ~32 / OBJECTS_PER_PREFIX
/// prefixes have downloads in flight simultaneously, capping the
/// number of open S3 uploads at the sink.
const RSS_TEST_OBJECTS_PER_PREFIX: usize = 6;
/// ~28_000 × ~140 B per line ≈ 4 MB matched plaintext per object,
/// 24 MB per prefix. Total fixture ≈ 1.5 GB plaintext.
const RSS_TEST_LINES_PER_OBJECT: usize = 28_000;
/// Peak RSS cap. Measured ~545–580 MB in practice under this workload
/// with close-on-completion enabled; `multipart_concurrency` barely
/// moves it (TM staging is only ~36 MB of the total) so the bulk is
/// pipeline working set + allocator behavior (Bytes::copy_from_slice
/// per codec block doesn't immediately return pages to the OS).
///
/// The pre-streaming buffered code was ~1500 MB on the same workload;
/// the post-streaming-without-close-on-completion code was ~864 MB.
/// 800 MB catches a regression toward either of those baselines while
/// leaving headroom for the current ~580 MB working set. Critically,
/// the curve is *flat in `match_mb`* — verified by the test running
/// to 1.6 GB matched without RSS climbing further — which is the
/// architectural property the close-on-completion machinery provides.
const RSS_TEST_CAP_MB: u64 = 800;

/// Many concurrent prefixes + explicit `multipart_concurrency` cap. This
/// is the configuration we'd ship as a safe default — it should stay
/// well under the RSS cap. If this fails, the streaming sink has a leak
/// not attributable to TM's internal concurrency choice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_rss_bounded_under_high_concurrency_with_explicit_cap() {
    skip_unless_docker!();
    if let Err(e) = run_rss_high_concurrency_test(Some("32")).await {
        panic!("s3_output_rss_bounded_under_high_concurrency_with_explicit_cap failed: {e:#}");
    }
}

/// Same workload but with `multipart_concurrency` unset (default `Auto`).
/// This is the *current* production configuration and the prime suspect
/// for the unresolved OOM. If this fails while the explicit-cap variant
/// passes, the fix is to add a sane default cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_output_rss_bounded_under_high_concurrency_with_auto_concurrency() {
    skip_unless_docker!();
    if let Err(e) = run_rss_high_concurrency_test(None).await {
        panic!("s3_output_rss_bounded_under_high_concurrency_with_auto_concurrency failed: {e:#}");
    }
}

async fn run_rss_high_concurrency_test(multipart_concurrency: Option<&str>) -> Result<()> {
    // Serialize the two variants so only one Garage container + scrapper is
    // resident at a time (see RSS_TEST_LOCK). Held for the whole run.
    let _serial = RSS_TEST_LOCK.lock().await;

    let garage = start_garage(BUCKET).await?;
    garage.create_bucket(RESULTS_BUCKET).await?;
    let s3 = garage.s3_client();

    // 64 unique (date, hour) prefixes spanning ~3 days × OBJECTS_PER_PREFIX
    // objects each — production shape, where each hour's prefix holds
    // several service-X logs (service-a, service-b, …). Sort+FIFO download
    // exploits this clustering to keep concurrent active uploads small.
    let prefixes = generate_prefixes(RSS_TEST_NUM_PREFIXES);
    let mut staged: Vec<StagedObject> = Vec::new();
    for (date, hour) in &prefixes {
        for obj_idx in 0..RSS_TEST_OBJECTS_PER_PREFIX {
            staged.push(bulk_match_object_n(
                date,
                hour,
                obj_idx,
                RSS_TEST_LINES_PER_OBJECT,
            ));
        }
    }
    seed_bucket(&s3, BUCKET, &staged).await?;
    // The staged fixture is ~10.75M owned Strings (~1.9 GB resident). It's only
    // needed for seeding; drop it before launching the scrapper so the test
    // process isn't holding it alongside the subprocess + Garage container.
    drop(staged);

    let workdir = TempDir::new()?;
    let config_path = workdir.path().join("config.yaml");
    write_rss_test_config(&config_path)?;

    let key_template = "out/{prefix}/{run_id}-{seq}.ndjson";
    let (start, end) = time_range_covering(&prefixes);

    let mut cmd = Command::cargo_bin("bucket-scrapper")?;
    for (k, v) in garage.env_for_scrapper() {
        cmd.env(k, v);
    }
    cmd.arg("--config")
        .arg(&config_path)
        .arg("--region")
        .arg("garage")
        .arg("--start")
        .arg(&start)
        .arg("--end")
        .arg(&end)
        .arg("--line-pattern-regex")
        .arg(PATTERN)
        .arg("--filter")
        .arg(r"service-.*\.(json|json\.gz|json\.zst)$")
        .arg("--output")
        .arg("s3")
        .arg("--s3-output-bucket")
        .arg(RESULTS_BUCKET)
        .arg("--s3-output-key-template")
        .arg(key_template)
        .arg("--max-parallel")
        .arg("32") // saturate the download semaphore
        .arg("--log-format")
        .arg("json")
        // Plaintext codec: encoder output bytes track ingested bytes
        // exactly, so the "leak vs match volume" relationship is crisp.
        .arg("--compression-format")
        .arg("none")
        // Frequent progress ticks so peak RSS is sampled densely enough
        // to catch transient spikes.
        .arg("--progress-interval")
        .arg("0.25");
    if let Some(c) = multipart_concurrency {
        cmd.arg("--s3-output-multipart-concurrency").arg(c);
    }

    let output = cmd.timeout(std::time::Duration::from_secs(300)).output()?;
    assert!(
        output.status.success(),
        "scrapper exited non-zero ({:?}); stderr tail:\n{}",
        output.status,
        last_lines(&String::from_utf8_lossy(&output.stderr), 60),
    );

    let logs = String::from_utf8(output.stdout)?;
    let rss_series = collect_rss_series(&logs);
    assert!(
        !rss_series.is_empty(),
        "no `Search progress` records with rss_mb found in logs"
    );

    let peak = rss_series.iter().map(|p| p.rss_mb).max().unwrap_or(0);
    let final_progress = rss_series.last().unwrap();
    let total_matched_mb = final_progress.match_mb;

    eprintln!(
        "rss test (multipart_concurrency={:?}): \
         samples={}, peak_rss_mb={}, final_rss_mb={}, \
         matched_mb={}, filter_lines_in={}",
        multipart_concurrency,
        rss_series.len(),
        peak,
        final_progress.rss_mb,
        total_matched_mb,
        final_progress.filter_lines_in,
    );

    // Sanity: the run must actually have moved real data, otherwise the
    // RSS bound is trivial.
    let expected_min_match_mb = (RSS_TEST_NUM_PREFIXES as u64
        * RSS_TEST_OBJECTS_PER_PREFIX as u64
        * RSS_TEST_LINES_PER_OBJECT as u64
        * 140 // approx line length in bytes
        / 1_000_000)
        / 2; // half-of-expected as a generous floor
    assert!(
        total_matched_mb >= expected_min_match_mb,
        "fixture moved less data than expected: matched_mb={total_matched_mb}, \
         floor={expected_min_match_mb}. RSS assertion would be meaningless."
    );

    assert!(
        peak <= RSS_TEST_CAP_MB,
        "S3 sink RSS exceeded cap under high concurrency: \
         peak={peak} MB > cap={RSS_TEST_CAP_MB} MB \
         (matched={total_matched_mb} MB, prefixes={}, \
          multipart_concurrency={:?}).\n\
         RSS series (last 10 samples): {:?}",
        RSS_TEST_NUM_PREFIXES,
        multipart_concurrency,
        rss_series.iter().rev().take(10).collect::<Vec<_>>(),
    );

    Ok(())
}

/// One parsed `Search progress` record's memory + matched-volume
/// snapshot. Only the fields the RSS test cares about.
#[derive(Debug, Clone)]
struct ProgressSample {
    rss_mb: u64,
    match_mb: u64,
    filter_lines_in: u64,
}

/// Walk JSON-lines stdout from a scrapper run, return one ProgressSample
/// per `Search progress` record in emission order.
fn collect_rss_series(logs: &str) -> Vec<ProgressSample> {
    logs.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("message").and_then(|m| m.as_str()) == Some("Search progress"))
        .map(|v| ProgressSample {
            rss_mb: v.get("rss_mb").and_then(|x| x.as_u64()).unwrap_or(0),
            match_mb: v.get("match_mb").and_then(|x| x.as_u64()).unwrap_or(0),
            filter_lines_in: v
                .get("filter_lines_in")
                .and_then(|x| x.as_u64())
                .unwrap_or(0),
        })
        .collect()
}

/// Generate `n` (date, hour) tuples spanning consecutive days starting
/// 2026-01-01. Hours wrap 00..24 per day; days roll forward.
fn generate_prefixes(n: usize) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let day = 1 + (i / 24);
        let hour = i % 24;
        out.push((format!("202601{day:02}"), format!("{hour:02}")));
    }
    out
}

fn time_range_covering(prefixes: &[(String, String)]) -> (String, String) {
    let (start_date, start_hour) = prefixes.first().expect("at least one prefix");
    let (end_date, end_hour) = prefixes.last().expect("at least one prefix");
    // `--end` is exclusive of the named hour, so add one hour by
    // bumping to the next hour slot. Padding with zeros on minute/sec.
    let next_hour: u32 = end_hour.parse::<u32>().unwrap() + 1;
    let (end_day, end_h) = if next_hour >= 24 {
        // Push to 00:00 of the next day.
        let next_day = end_date[6..].parse::<u32>().unwrap() + 1;
        (format!("202601{next_day:02}"), "00".to_string())
    } else {
        (end_date.clone(), format!("{next_hour:02}"))
    };
    let start = format!(
        "{}-{}-{}T{}:00:00Z",
        &start_date[..4],
        &start_date[4..6],
        &start_date[6..],
        start_hour
    );
    let end = format!(
        "{}-{}-{}T{}:00:00Z",
        &end_day[..4],
        &end_day[4..6],
        &end_day[6..],
        end_h
    );
    (start, end)
}

fn write_rss_test_config(path: &Path) -> Result<()> {
    let yaml = format!(
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
    std::fs::write(path, yaml)?;
    Ok(())
}

fn last_lines(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}
