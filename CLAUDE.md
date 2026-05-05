# bucket-scrapper

S3 bucket content searcher. Downloads compressed objects from S3, stream-decompresses, filters lines by regex, and routes matches to one of four pluggable output sinks: local files, an HTTP API (NDJSON), an S3 bucket, or `/dev/null` (void). The file/http/s3 sinks share a single `Codec` abstraction (zstd / gzip / none) so the on-disk extension, the wire `Content-Encoding`, and the encoder bytes stay in lockstep.

## Architecture

Streaming pipeline with decoupled stages connected by bounded channels:

```
S3 GetObject stream (semaphore-bounded, range-based resume on retries)
  → async chunk loop → flume::bounded(4) → ChunkReader (impl Read)
  → spawn_blocking: stream-decompress (.gz/.zst) line-by-line
  → line_ch (flume bounded)
  → filter_worker pool (spawn_blocking, regex via grep-matcher)
  → Arc<dyn OutputSink>::ingest(prefix, line)
       ├─ FileOutputSink   → SharedFileWriter (per-prefix Codec-encoded files)
       ├─ HttpOutputSink   → compressor pool → uploader pool → HTTP POST (AIMD)
       ├─ S3OutputSink     → per-prefix Codec-encoded batches → uploader pool → PutObject
       └─ VoidOutputSink   → atomic counters only (benchmarking)
```

Key modules:
- `src/pipeline/orchestrator.rs` — pipeline orchestrator: download → decompress → filter → sink
- `src/pipeline/output.rs` — `OutputSink` trait + `OutputStats`
- `src/pipeline/codec.rs` — output codec (zstd / gzip / none) + `CompressionConfig` + `CodecEncoder<W>`
- `src/pipeline/path_template.rs` — `{prefix}` / `{prefix_hash}` / `{seq}` / `{run_id}` / `{ext}` template renderer + `CollisionTracker`, shared by file and s3 sinks
- `src/pipeline/http_writer.rs` / `http_sink.rs` — HTTP output internals + sink adapter
- `src/pipeline/streaming_writer.rs` / `file_sink.rs` — file output internals + sink adapter
- `src/pipeline/s3_writer.rs` — S3 output sink with per-prefix batching
- `src/pipeline/void_writer.rs` — no-op sink with atomic counters
- `src/pipeline/observer.rs` — observer primitives: `PipelineObserver`, `ChannelObserver`, `DownloadObserver`
- `src/config/output.rs` — `OutputConfig` tagged enum + `${ENV}` interpolation + template/codec validation
- `src/config/resolve.rs` — selects config-driven vs CLI-driven mode (mixing is a hard error)
- `src/matcher.rs` — `LineMatcher`: stateless regex wrapper around `grep-matcher`
- `src/progress.rs` — periodic structured-log progress reports with bottleneck detection
- `src/config/path_formatter.rs` — date/hour prefix formatting from `BucketConfig` path schemas

## Tech Stack

- Rust, Tokio async runtime
- `aws-sdk-s3` for S3 operations (downloads + S3 output uploads)
- `flume` for bounded MPMC channels
- `grep-matcher` / `grep-regex` for line matching
- `zstd` / `flate2` for compression
- `tracing` for structured logging (text or JSON via `--log-format`)
- `clap` for CLI argument parsing
- `wiremock` for HTTP mock tests

## Testing

```bash
cargo test          # unit + integration + doctests
cargo clippy        # zero warnings expected
```

## Known Issues

- `unused manifest key: profile.profiling.force-frame-pointers` — Cargo bug with custom profiles, not a code issue
