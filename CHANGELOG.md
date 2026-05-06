# Changelog

All notable changes since 1.0.0. Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.1] — 2026-05-05

### Added
- **S3 sink: multipart uploads via `aws-sdk-s3-transfer-manager`.** Finalized batches above `multipart_threshold_mb` (default 5 MiB) are auto-multiparted with parts of `multipart_part_mb` (default 5 MiB); sub-threshold batches still go through a single `PutObject`. The TM crate shares our `aws_sdk_s3::Client` via `Config::Builder::client(...)`, so credentials, endpoint URL, and the cached DNS resolver carry through unchanged.
- New config `multipart_concurrency: Option<usize>` bounding parts in flight across all in-flight batches (`None` → `ConcurrencyMode::Auto`). Distinct from `upload_tasks`, which bounds whole-batch concurrency. CLI: `--s3-output-multipart-concurrency`.
- Validation: `multipart_part_mb` / `multipart_threshold_mb` / `multipart_concurrency` now do real work; sub-5 / over-5000 values are rejected at startup.
- E2E: `s3_output_end_to_end_multipart` asserts each batch ≥ 5 MiB (proving multipart actually engaged) and bodies round-trip.

### Known issues
- `aws-sdk-s3-transfer-manager` 0.1.3 (developer preview) transitively activates `aws-smithy-http-client` default features, reintroducing the legacy hyper-rustls 0.24 / rustls 0.21 / rustls-webpki 0.101 path (RUSTSEC-2026-0098 / -0099 / -0104). Documented in `CLAUDE.md` "Known Issues"; revisit when TM disables defaults on its `aws-*` deps upstream.

## [2.0.0] — 2026-05-05

### Added
- **Pluggable output sinks with tagged-enum config** (`file` / `http` / `s3` / `void`). Single `Arc<dyn OutputSink>` dispatch replaces the file-or-HTTP split.
  - New `s3` sink: per-prefix batches via `PutObject`.
  - New `void` sink: no-op for benchmarking.
  - String fields in `outputs:` support `${VAR}` / `${VAR:-default}` env interpolation so secrets stay out of YAML.
- **`Codec` abstraction** unifying compression across file / http / s3 sinks. Owns format (zstd / gzip / none), level, file extension, and wire `Content-Encoding`. Replaces three per-sink `compression_level` fields with one shared `compression: { format, level }` block.
- **Templated output paths**: `path_template` / `key_template` support `{prefix}` / `{prefix_hash}` / `{seq}` / `{run_id}` / `{ext}` (codec-derived). Templates without `{prefix}` or `{prefix_hash}` (and `{seq}` when `batch_max_mb` is set on s3) are rejected at startup; a runtime `CollisionTracker` catches residual collisions — fatal for file, warn for s3.
- **File-level sampling.** Coarse work-shedding that drops a fraction of S3 objects after key filtering and before the download/decompress pipeline. Per-bucket `sample_files` with a global `--sample-files` CLI fallback. Optional `sampling_seed` for reproducibility.
- E2E coverage for the s3 and void sinks (Garage second-bucket; structured-log assertions).

### Changed
- **BREAKING:** Legacy `output_dir:` and `http_output:` config keys are removed; configs must use `outputs: [{ type: …, … }]`.
- **BREAKING:** `--http-output` boolean flag is replaced by `--output <type>`.
- **BREAKING:** All per-output CLI flags become `Option<T>`; mixing them with a config `outputs:` block is now a hard error (no per-field merge).
- **BREAKING:** Bare `compression_level` in YAML and `--s3-output-compression-level` on the CLI are removed; use the new `compression` block / `--compression-format` + `--compression-level` instead.
- **BREAKING:** `batch_max_mb` is now `Option<f64>` with no default. When unset, each source prefix collapses to exactly one output object (matching file-sink N:1 semantics). When set, the existing size-based rollover behavior applies. Configs that previously relied on the implicit 16 MB rollover must set `batch_max_mb` explicitly.
- `SharedFileWriter::finish(self)` → `finalize(&self)` so the writer can live behind `Arc`.
- `HttpOutputSink` holds a `WeakSender` so `finish` actually closes the line channel instead of deadlocking.
- The s3 batching docs are rewritten to describe the actual lifecycle (per-prefix encoder, threshold-driven mid-run flush, end-of-run flush) and the buffering/concurrency caveats.
- README refreshed for tagged-enum outputs; default config path renamed to `sample-config.yaml`.

## [1.1.1] — 2026-04-29

### Security
- Resolved all open RUSTSEC advisories surfaced by `cargo audit`:
  - `aws-sdk-s3`: disabled default features and selected the modern `aws-lc-rs` HTTPS path (`sigv4a`, `http-1x`, `default-https-client`, `rt-tokio`). The default feature set silently enabled a legacy `rustls` feature that pulled rustls 0.21 / hyper-rustls 0.24 — source of RUSTSEC-2026-0098, -0099, -0104.
  - `testcontainers`: 0.23 → 0.27 to drop vulnerable `tokio-tar` 0.3.1 (RUSTSEC-2025-0111) and unmaintained `rustls-pemfile` (RUSTSEC-2025-0134) from the dev-dep tree.
  - `cargo update`: ~200 transitive patch bumps.
- Pinned `cargo-audit` in `mise.toml` so contributors run the same advisory scanner CI does.

### Changed
- Tooling: pinned `lefthook` in `mise.toml`; added an `AGENTS.md` rule that "if mise can manage it, mise must manage it."

## [1.1.0] — 2026-04-27

### Added
- **Filter input volume + matched ratios in periodic progress.** Three new shared atomics (`filter_lines_in`, `filter_bytes_in`, `match_bytes`) surface as `filter_in_mb` / `filter_in_mbps` plus two distinct selectivity ratios (`matched_ratio_lines` and `matched_ratio_bytes`). A final `progress.report()` after pipeline completion ensures the last log line carries accurate end-of-run totals.
- **Garage + nginx end-to-end pipeline tests** (`tests/e2e_pipeline.rs`) covering both file and HTTP output modes against a real S3-compatible backend.
- Tier-1 mock-based unit tests for `ChunkReader`, `progress` (extracted `classify_bottleneck_{http,file}`), `PipelineObserver`, and `dns_cache`. 64 → 81 tests.
- Pipeline observability: `workers_alive` and `open_fds` metrics.
- CI: `lefthook` pre-commit (`fmt`) / pre-push (`clippy`) hooks mirroring CI checks.

### Fixed
- **Worker-death deadlock and channel-lifetime bug.** `ChannelObserver::from_receiver` cloned the receiver and kept the line channel alive forever, defeating close-on-coordinator-exit. Added `from_sender` as the correct mirror, deprecated `from_receiver`, and switched the orchestrator to it.
- **Early filter-worker death** no longer hangs the pipeline. Workers now run in a `JoinSet` and a `select!` loop joins them concurrently with the download coordinator; a worker error aborts the coordinator instead of blocking on a full line channel.
- `src/s3/client.rs`: honor `AWS_S3_FORCE_PATH_STYLE` and set `RequestChecksumCalculation::WhenRequired`, both needed to talk to non-AWS S3 backends (Garage, MinIO, …).
- `src/pipeline/observer.rs`: `ChannelObserver::from_sender` switched to `WeakSender` so the file pipeline no longer hangs after all files are processed.
- Pre-existing fmt drift in `observer.rs` and the e2e test harness.

## [1.0.2] — 2026-03-12

### Changed
- Dependency bumps and adaptations:
  - `flume` 0.11 → 0.12
  - `reqwest` 0.12 → 0.13 (`rustls-tls` feature renamed to `rustls`)
  - `hickory-resolver` 0.24 → 0.25 (`TokioAsyncResolver` → `TokioResolver`, builder API)
  - Patch updates for `libc`, `socket2`, `tempfile`, `uuid`, `zerocopy`, etc.

### Fixed
- Lower download progress reporting latency.

## [1.0.1] — 2026-03-05

### Fixed
- CI: lowercase Docker image name for GHCR. `github.repository` returns `ManoManoTech/...` but Docker tags must be all lowercase.

## [1.0.0] — 2026-03-05

Initial tagged release.

[2.0.1]: https://github.com/ManoManoTech/bucket-scrapper/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/ManoManoTech/bucket-scrapper/compare/v1.1.1...v2.0.0
[1.1.1]: https://github.com/ManoManoTech/bucket-scrapper/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/ManoManoTech/bucket-scrapper/compare/v1.0.2...v1.1.0
[1.0.2]: https://github.com/ManoManoTech/bucket-scrapper/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/ManoManoTech/bucket-scrapper/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/ManoManoTech/bucket-scrapper/releases/tag/v1.0.0
