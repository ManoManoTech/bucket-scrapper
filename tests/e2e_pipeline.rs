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
    let prefix = format!("logs/dt={date}/hour={hour}");
    let lines: Vec<String> = (0..n_lines)
        .map(|i| {
            format!(
                r#"{{"service":"bulk","hour":"{hour}","seq":{i},"level":"ERROR","msg":"ERROR bulk row #{i} for rollover test, padding the line a bit so blocks fill faster"}}"#
            )
        })
        .collect();
    StagedObject {
        key: format!("{prefix}/service-bulk-001.json"),
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
        .find(|v| {
            v.get("fields")
                .and_then(|f| f.get("message"))
                .and_then(|m| m.as_str())
                == Some("Search completed")
        })
        .expect("Search completed record not found in JSON logs");

    let fields = completion.get("fields").expect("fields object");
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
