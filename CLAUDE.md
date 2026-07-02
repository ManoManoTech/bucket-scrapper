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
- `src/control/mod.rs` — runtime control plane: wire protocol (`ControlRequest`/`ControlResponse`/`StatusSnapshot`) + `RuntimeControls` (live-tunable shared state)
- `src/control/server.rs` — Unix-domain-socket control server (`serve`, `ControlContext`, `StatusHandles`)
- `src/bin/bsctl.rs` — control client binary (`bsctl`)
- `src/config/output.rs` — `OutputConfig` tagged enum + `${ENV}` interpolation + template/codec validation
- `src/config/resolve.rs` — selects config-driven vs CLI-driven mode (mixing is a hard error)
- `src/matcher.rs` — `LineMatcher`: stateless regex wrapper around `grep-matcher`
- `src/progress.rs` — periodic structured-log progress reports with bottleneck detection
- `src/config/path_formatter.rs` — date/hour prefix formatting from `BucketConfig` path schemas

## Runtime control plane (`bsctl`)

The pipeline exposes a Unix-domain-socket control plane by default at `bs.sock`
in its working directory; `bsctl <cmd>` (defaulting to the same path) retunes a
*running* sweep without a restart. `--control-socket <PATH>` / `bsctl --socket
<PATH>` relocate it; `--no-socket` disables the plane entirely. The default is
relative, so in the image it lands in the `/app` WORKDIR (which the Dockerfile
`chown`s to the runtime user so the bind succeeds) and `docker exec <container>
./bsctl status` reaches it. A bind failure is non-fatal — logged as a warning,
the sweep continues without a plane — so on a read-only-rootfs pod either mount
a writable dir and point `--control-socket` at it, or accept no control plane.
Knobs are live because the pipeline shares them via `Arc`:

- `bsctl … download-tasks ±N` — concurrent downloaders (the file semaphore; the
  coordinator spawns/retires download tasks as permits are added/forgotten).
- `bsctl … range-concurrency ±N` — range-GET concurrency within chunked
  downloads (the download semaphore).
- `bsctl … filter-workers ±N` — grow spawns into the filter `JoinSet`; shrink
  posts to a retire-counter workers honor at their next line boundary.
- `bsctl … part-size <MB>` — chunked-download part size (0 disables); applies to
  objects dispatched after the change.
- `bsctl … status` — live snapshot (effective knobs + gauges).
- `bsctl … line-buffer <N>` — **unsupported in v1** (flume bounded channels can't
  resize in place); the daemon returns an `unsupported` notice.

With `--no-socket`, the server never starts (zero overhead). A socket bind
failure is non-fatal — it's logged and the sweep continues without a control
plane.

## Tech Stack

- Rust, Tokio async runtime
- `aws-sdk-s3` for S3 operations (downloads + S3 output uploads)
- `aws-sdk-s3-transfer-manager` (developer preview, pinned `=0.1.3`) drives multipart uploads on the s3 sink, sharing our pre-built `aws_sdk_s3::Client` via `Config::Builder::client(...)`
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
- `aws-sdk-s3-transfer-manager` is a developer preview (no production-stability guarantee), pinned at `=0.1.3`. It declares its `aws-sdk-s3` and `aws-config` deps **without** `default-features = false`, so `aws-sdk-s3`'s default `"rustls"` feature is unconditionally on; that activates `aws-smithy-runtime/tls-rustls` → `aws-smithy-http-client/legacy-rustls-ring`, pulling in the legacy hyper-rustls 0.24 / rustls 0.21 / rustls-webpki 0.101 path. That re-introduces RUSTSEC-2026-0098/0099/0104 (name-constraint and CRL-parsing issues in rustls-webpki). Our own direct `aws-sdk-s3` dep already passes `default-features = false` + `default-https-client`, but Cargo's additive feature unification means TM's defaults still win. Tracked upstream as [awslabs/aws-s3-transfer-manager-rs#138](https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/138) (open, no movement). Accepted as a deliberate tradeoff for AWS-maintained multipart code; revisit when that issue lands a fix and a new TM release ships.


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:6cd5cc61 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->
