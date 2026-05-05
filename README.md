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

Per-prefix zstd-compressed files under `dir`:

```yaml
outputs:
  - type: file
    dir: ./scrapper-output
    # compression_level: 3
```

Or via CLI:

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z --output file --output-dir ./scrapper-output
```

### HTTP sink

NDJSON-zstd POSTs to an HTTP endpoint, with adaptive (AIMD) throttling and 429 back-off:

```yaml
outputs:
  - type: http
    url: https://logs.example.com/api/v1/logs
    bearer_auth: ${HTTP_BEARER_AUTH}    # ${ENV} interpolation supported
    timeout_secs: 30
    batch_max_mb: 2
```

Or via CLI (URL and token can also come from `HTTP_URL` / `HTTP_BEARER_AUTH`):

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z \
  --output http \
  --http-url "https://logs.example.com/api/v1/logs" \
  --http-bearer-auth "your-token"
```

### S3 sink

Per-prefix zstd objects written to a destination S3 bucket (works with non-AWS backends — Garage, MinIO — via `endpoint_url`). By default each source prefix collapses to exactly one output object (N:1, same shape as the file sink). Set `batch_max_mb` to opt into size-based rollover within a prefix:

```yaml
outputs:
  - type: s3
    bucket: my-results-bucket
    key_template: "results/{prefix}/{run_id}-{seq}.ndjson.zst"
    # batch_max_mb: 16   # optional; omit for one object per source prefix
```

Output mapping summary (file and S3 sinks):

| Sink | Default | With `batch_max_mb` set |
|------|---------|-------------------------|
| `file` | one `.zst` file per source prefix (always) | n/a — file sink has no rollover |
| `s3` | one object per source prefix | one or more objects per prefix, rolling over at the threshold |

Cross-prefix consolidation (e.g. one daily file across all hours) is not currently supported.

### Void sink

Drops every match (benchmarking only):

```yaml
outputs:
  - type: void
```

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
| `--s3-output-key-template` | | Key template; supports `{prefix}`, `{prefix_hash}`, `{seq}`, `{run_id}` |

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
| `--compression-level` | 3 | Zstd level (1-22) |
| `--memory-limit-gb` | 0 | Memory limit via setrlimit (0 = none) |
| `--client-max-age` | 60 | S3 client max age (minutes) |
| `--http-line-channel-size` | 1000 | Line channel before compressors |
| `--http-compressor-tasks` | cpu/8 | Zstd compressor tasks |
| `--http-upload-tasks` | 4x compressors | Concurrent upload tasks |
| `--http-upload-channel-size` | 4 | Batch channel size |

## Profiling

**CPU**: Use samply with the profiling build profile.

**Memory**: `cargo build --profile profiling --features dhat-heap`, then submit the generated `profiler.json` to [dh_view](https://nnethercote.github.io/dh_view/dh_view.html).
