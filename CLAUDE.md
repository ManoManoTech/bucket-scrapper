# bucket-scrapper

S3 bucket content searcher. Downloads compressed objects from S3, stream-decompresses, filters lines by regex, and outputs to either local zstd files or an HTTP API (HTTP).

## Architecture

Streaming pipeline with decoupled stages connected by bounded channels:

```
S3 download (semaphore-bounded)
  → spawn_blocking: stream-decompress (.gz/.zst) line-by-line
  → line_ch (flume bounded)
  → filter_worker pool (spawn_blocking, regex via grep-matcher)
  → FilterOutput::Http → compressor pool → uploader pool → HTTP POST
  → FilterOutput::File → SharedFileWriter (per-prefix zstd files)
```

Key modules:
- `src/s3/streaming_downloader.rs` — pipeline orchestrator (despite living in `s3/`)
- `src/pipeline/http_writer.rs` — HTTP output: compressor pool + uploader pool + AIMD throttle
- `src/pipeline/observer.rs` — type-erased channel fill-level observer for progress reporting
- `src/pipeline/streaming_writer.rs` — file output: `SharedFileWriter` with per-prefix `Mutex<ZstdEncoder>`
- `src/matcher.rs` — `LineMatcher`: stateless regex wrapper around `grep-matcher`
- `src/progress.rs` — periodic structured-log progress reports with bottleneck detection

## Tech Stack

- Rust, Tokio async runtime
- `aws-sdk-s3` for S3 operations
- `flume` for bounded MPMC channels
- `grep-matcher` / `grep-regex` for line matching
- `zstd` / `flate2` for compression
- `tracing` for structured logging (text or JSON via `--log-format`)
- `clap` for CLI argument parsing
- `wiremock` for HTTP mock tests

## Testing

```bash
cargo test          # 61 tests (unit + integration + doctests)
cargo clippy        # zero warnings expected
```

## Known Issues

- `unused manifest key: profile.profiling.force-frame-pointers` — Cargo bug with custom profiles, not a code issue
