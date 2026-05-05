# bucket-scrapper

Search through compressed S3 objects at scale. Stream-decompress, filter lines by regex, and route matches to one of four pluggable output sinks (local zstd files, HTTP API, S3, or `/dev/null`) — without ever buffering a full object in memory.

## Installation

```bash
cargo build --release
# Binary: target/release/bucket-scrapper
```

## How it works

Three decisions drive every run:

1. **Which files to read from S3?** — Buckets, prefix paths, date ranges, key filters, optional file-level sampling
2. **Which lines to keep?** — All lines, or only those matching a regex
3. **Where to send them?** — One of four sinks: `file`, `http`, `s3`, `void`

Everything streams: downloads decompress on the fly, lines are filtered as they appear, and results are written continuously.

## 1. Selecting S3 files

### Config file

A config file (`sample-config.yaml` by default; override with `--config`) defines which buckets and prefix paths to scan, plus the active output:

```yaml
buckets:
  - bucket: my-log-bucket
    path:
      - static_path: "log-archives/"
      - datefmt: "dt=20060102/hour=15"
    only_prefix_patterns:          # optional: regexes matched against the key suffix
      - "service-a"

region: eu-west-3

outputs:
  - type: file
    dir: ./scrapper-output
```

Path components are either literal strings (`static_path`) or date patterns (`datefmt`). Two date conventions are recognized: Hive-style `dt=20060102/hour=15` or Go reference-time `2006/01/02/15`. Each bucket must have at least one `datefmt` component to avoid listing the entire bucket.

The `outputs:` list takes exactly one entry today (multi-output fan-out is reserved). When `outputs:` is present, the CLI per-output flags (`--output`, `--output-dir`, `--http-*`, `--s3-output-*`) must NOT be passed — mixing config and CLI is a hard error. Drop `outputs:` to drive the output entirely from CLI flags. See [`sample-config.yaml`](sample-config.yaml) for fully commented examples of each sink.

### Date range (required)

Every run needs at least a start time. The tool generates S3 prefixes for each hour in the range:

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z -e 2024-01-15T12:00:00Z
```

### Key filter

Further narrow which S3 objects to process with a regex on the object key:

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z -f "service-a.*\.json\.zst$"
```

### File-level sampling

Drop a fraction of input files *after* key filtering with a float in `(0.0, 1.0]`:

```bash
# Keep ~10% of files, reproducibly
bucket-scrapper -s 2024-01-15T10:00:00Z --sample-files 0.1 --sampling-seed 42
```

Or per-bucket in the config (overrides the global CLI flag for that bucket):

```yaml
buckets:
  - bucket: my-log-bucket
    path: [...]
    sample_files: 0.1     # keep 10% of this bucket's files
sampling_seed: 42         # optional, top-level; omit for fresh entropy each run
```

Sampling is the **coarsest** of the work-shedding mechanisms: it sheds whole files, so for sources with high per-file size variance the resulting line-volume sample can be noisy. It is also the cheapest — files that aren't kept are never downloaded, decompressed, or scanned.

`0.0`, negative values, and `>1.0` are rejected at startup. Omit the field to disable sampling.

## 2. Filtering lines

By default, all lines from matching objects are forwarded. Add a regex to keep only what you need:

```bash
# Lines containing "ERROR" followed by "timeout"
bucket-scrapper -s 2024-01-15T10:00:00Z --line-pattern-regex "ERROR.*timeout"

# Case insensitive
bucket-scrapper -s 2024-01-15T10:00:00Z --line-pattern-regex "failed" -i
```

Omit `--line-pattern-regex` to extract everything (useful for bulk re-export).

## 3. Choosing output

Pick exactly one sink, either via the config `outputs:` block or via CLI flags. Mixing the two is a hard error.

### File sink

Per-prefix files under `dir`. Default codec is zstd:3, default filename is `{prefix}.{ext}`:

```yaml
outputs:
  - type: file
    dir: ./scrapper-output
    # path_template: "{prefix}.{ext}"
    # compression: { format: zstd, level: 3 }
```

Or via CLI:

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z --output file --output-dir ./scrapper-output
```

### HTTP sink

NDJSON POSTs to an HTTP endpoint with adaptive (AIMD) throttling and 429 back-off. `Content-Encoding` is set automatically from the codec (`zstd` / `gzip`) or omitted entirely when `compression.format = none`:

```yaml
outputs:
  - type: http
    url: https://logs.example.com/api/v1/logs
    bearer_auth: ${HTTP_BEARER_AUTH}    # ${ENV} interpolation supported
    timeout_secs: 30
    batch_max_mb: 2
    # compression: { format: zstd, level: 3 }
```

Or via CLI (URL and token can also come from `HTTP_URL` / `HTTP_BEARER_AUTH`):

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z \
  --output http \
  --http-url "https://logs.example.com/api/v1/logs" \
  --http-bearer-auth "your-token"
```

### S3 sink

Per-prefix objects written to a destination S3 bucket. Works with non-AWS backends (Garage, MinIO) via `endpoint_url`.

```yaml
outputs:
  - type: s3
    bucket: my-results-bucket
    key_template: "results/{prefix}/{run_id}-{seq}.ndjson.{ext}"
    # batch_max_mb: 16   # opt into batched uploads — see below
    # compression: { format: zstd, level: 3 }
```

#### Batching model

The s3 sink uploads **batches**. One batch is one `PutObject` call. Every batch carries lines from a single source prefix; there is no cross-prefix mixing or consolidation. The configurable axis is *how many batches per source prefix*.

**Per-prefix encoder lifecycle.** When the first matched line for a source prefix arrives, the sink lazily creates an in-memory encoder for that prefix (zstd / gzip / identity, per `compression.format`). Subsequent matched lines for the same prefix are written into that encoder. A prefix that produces no matches has no encoder and emits no object. Different prefixes have independent encoders and never block each other.

**Default mode (`batch_max_mb` unset).** Each prefix's encoder is finalized exactly once, at end-of-run. The finalized buffer is enqueued for upload. Result: one output object per source prefix, with `{seq}` always rendering to `00000`. This is the same N:1 shape as the file sink.

**Batched mode (`batch_max_mb` set).** After every line ingested for a prefix, the sink reads the **compressed bytes already emitted into that prefix's encoder output buffer** and compares it to `batch_max_mb`. When the buffer crosses the threshold:

1. The encoder is finalized in place, producing a finished compressed frame.
2. That frame is rendered into a destination key (`{seq}` substituted, then `{seq}` is incremented for the next batch in this prefix) and pushed onto the bounded upload queue.
3. A fresh encoder is constructed for the same prefix; subsequent lines start filling it.

End-of-run runs an unconditional flush over every prefix that still has a non-empty encoder, producing a final batch (the trailing partial). Each prefix's `{seq}` therefore yields a contiguous `00000`, `00001`, … sequence whose count depends only on how many threshold crossings happened plus one closing flush.

**Why upload size can exceed `batch_max_mb`.** The threshold check sees only the compressed bytes the encoder has already flushed to its output buffer — it doesn't include lines still buffered inside the codec, nor does it preemptively split a line that pushes the buffer over the line. In practice each batch lands a little above the threshold rather than at it.

**Why a small `batch_max_mb` may not produce extra batches.** zstd and gzip both buffer internally and only emit compressed bytes when they have enough data to encode efficiently. A trickle of small, highly-compressible lines can keep the encoder's *output* buffer at zero for a long time even though many lines have been ingested — so no threshold crossing fires, and end-of-run produces a single batch. To exercise batched mode you need either enough input volume per prefix to force several internal block flushes, or `compression.format: none` (where the output buffer grows monotonically with input size).

**Concurrency and ordering.** Batches go onto a bounded queue drained by N uploader tasks (`upload_tasks`, defaults to ~`cpu/4`). Uploaders run concurrently, so batches within a prefix can land out of order on S3. `{seq}` reflects the order in which the sink *finalized* the batch, not the order S3 finished receiving it. End-of-run waits for the queue to drain before reporting completion.

**Failure model.** Recoverable upload errors (transient 5xx, throttling) surface as `error!` logs but don't stop the pipeline; the run continues with the next batch. Non-recoverable errors (malformed request, auth failure) flip the sink's fatal flag, the download coordinator stops feeding new work, and the run aborts. Lost batches are counted into `OutputStats.lines_dropped`. Multipart uploads are not yet implemented — every batch is a single `PutObject`, so a batch must fit AWS's 5 GB single-PUT cap (the `multipart_threshold_mb` / `multipart_part_mb` config fields are accepted but currently informational, with a startup warning if you set them away from defaults).

**`{seq}` is required when `batch_max_mb` is set.** Without it, every batch within a prefix would render to the same key and silently overwrite the previous one. Startup rejects such configs.

**`{seq}` and reruns.** `{seq}` is an in-memory counter — it resets to 0 on every process start. Two runs that hit the same prefix and template produce overlapping `{seq}` values; rely on `{run_id}` (unique per process invocation) to disambiguate them in the destination key.

#### Output mapping summary

| Sink | Default | With `batch_max_mb` set |
|------|---------|-------------------------|
| `file` | one file per source prefix (always) | n/a — file sink has no batched uploads |
| `s3` | one object per source prefix, `{seq}=00000` | one or more objects per prefix; new batch finalized whenever the encoder's compressed output buffer crosses `batch_max_mb`, plus one trailing batch at end-of-run |

Cross-prefix consolidation (e.g. one daily file across all hours) is not currently supported.

### Void sink

Drops every match (benchmarking only):

```yaml
outputs:
  - type: void
```

### Codecs

The `file`, `http`, and `s3` sinks share a single compression block:

```yaml
compression:
  format: zstd | gzip | none   # default: zstd
  level: <int>                 # codec-specific; omit for the codec default
```

| Format | Levels  | Default | File extension | Wire `Content-Encoding` |
|--------|---------|---------|----------------|-------------------------|
| `zstd` | 1–22    | 3       | `.zst`         | `zstd`                  |
| `gzip` | 0–9     | 6       | `.gz`          | `gzip`                  |
| `none` | n/a     | n/a     | (none)         | (header omitted)        |

Setting `level` when `format: none` is rejected at startup. So is an out-of-range level for the chosen format.

### Path templates and collisions

Both `file` (`path_template`) and `s3` (`key_template`) render the per-prefix output destination from a string with these placeholders:

| Placeholder      | Meaning                                                                          |
|------------------|----------------------------------------------------------------------------------|
| `{prefix}`       | Source S3 prefix verbatim (e.g. `logs/dt=20240315/hour=09`)                      |
| `{prefix_hash}`  | 8-char hex hash of the prefix — useful when the raw prefix has awkward characters |
| `{run_id}`       | 8-char hex unique to this process invocation                                     |
| `{seq}`          | Per-prefix zero-padded sequence number — **s3 only**, incremented on each batch finalize when `batch_max_mb` is set |
| `{ext}`          | Codec extension (`zst`, `gz`, or empty); the leading `.` is dropped when empty   |

The template **must contain `{prefix}` or `{prefix_hash}`** — without one, every source prefix renders to the same destination. For the file sink this is fatal (two encoders writing to one file would corrupt the output) and is rejected at startup. For the s3 sink it's also rejected at startup, with a runtime warn-and-overwrite as defence in depth against `{prefix_hash}` collisions.

When `batch_max_mb` is set on the s3 sink, the template must additionally contain `{seq}`; otherwise every batch within a prefix would render to the same key and overwrite the previous — also a startup error.

Anti-pattern (rejected): `path_template: "results.{ext}"` — same path for every prefix.

## AWS Authentication

Standard AWS SDK credential chain: environment variables, `~/.aws/credentials`, IAM role, or `aws sso login`. Custom CA bundles via `AWS_CA_BUNDLE`.

## CLI Reference

```
Usage: bucket-scrapper [OPTIONS] --start <START>
```

### General

| Flag | Default | Description |
|------|---------|-------------|
| `-s, --start <START>` | *required* | Start date (ISO 8601) |
| `-e, --end <END>` | now | End date (ISO 8601) |
| `--config <CONFIG>` | `sample-config.yaml` | Config file path |
| `-r, --region <REGION>` | `eu-west-3` | AWS region |
| `-v, --log-level <LOG_LEVEL>` | `info` | trace, debug, info, warn, error |
| `--log-format <LOG_FORMAT>` | `text` | `text` or `json` |

### File selection & filtering

| Flag | Default | Description |
|------|---------|-------------|
| `-f, --filter <FILTER>` | | Regex on S3 object keys |
| `--line-pattern-regex <REGEX>` | (all lines) | Regex to filter lines |
| `-i, --ignore-case` | false | Case insensitive matching |
| `--sample-files <RATE>` | (disabled) | File-level sample rate in (0,1]. Per-bucket `sample_files` overrides this. |
| `--sampling-seed <SEED>` | (entropy) | Seed for the sampling RNG (reproducibility) |

### Output selection

| Flag | Default | Description |
|------|---------|-------------|
| `--output <KIND>` | (from config) | `file`, `http`, `s3`, or `void` |
| `--output-dir <DIR>` | | Directory for `file` output |
| `--http-url` | `HTTP_URL` env | Endpoint URL (http output) |
| `--http-bearer-auth` | `HTTP_BEARER_AUTH` env | Bearer token |
| `--http-batch-max-mb` | 2 | Max batch size (MB) |
| `--http-timeout` | 30 | Request timeout (seconds) |
| `--s3-output-bucket` | | Destination bucket (s3 output) |
| `--s3-output-key-template` | | Key template; supports `{prefix}`, `{prefix_hash}`, `{seq}`, `{run_id}`, `{ext}` |
| `--output-path-template` | `{prefix}.{ext}` | File-sink path template; same placeholders minus `{seq}` |
| `--compression-format` | `zstd` | `zstd`, `gzip`, or `none` |
| `--compression-level` | codec default | zstd 1–22 / gzip 0–9; unset for `none` |

### AIMD throttle

| Flag | Default | Description |
|------|---------|-------------|
| `--max-submission-time` | 3.0 | Batch time threshold in seconds (0 = disable) |
| `--max-upload-rate` | 0 | Rate limit in MB/s (0 = unlimited) |
| `--http-aimd-decrease-factor` | 0.15 | Multiplicative decrease on congestion |
| `--http-aimd-increase` | 1.0 | Additive increase per healthy batch (MB/s) |

### Performance tuning

| Flag | Default | Description |
|------|---------|-------------|
| `--max-parallel` | 32 | Concurrent S3 downloads |
| `--filter-tasks` | cpu/2 | Regex filter workers |
| `--line-buffer-size` | 1000 | Line channel capacity |
| `--max-retries` | 10 | Download retry attempts |
| `--retry-delay` | 2 | Initial retry delay (seconds) |
| `--progress-interval` | 3 | Progress report interval (seconds) |
| `--memory-limit-gb` | 0 | Memory limit via setrlimit (0 = none) |
| `--client-max-age` | 60 | S3 client max age (minutes) |
| `--http-line-channel-size` | 1000 | Line channel before compressors |
| `--http-compressor-tasks` | cpu/8 | Zstd compressor tasks |
| `--http-upload-tasks` | 4x compressors | Concurrent upload tasks |
| `--http-upload-channel-size` | 4 | Batch channel size |

## Profiling

**CPU**: Use samply with the profiling build profile.

**Memory**: `cargo build --profile profiling --features dhat-heap`, then submit the generated `profiler.json` to [dh_view](https://nnethercote.github.io/dh_view/dh_view.html).
