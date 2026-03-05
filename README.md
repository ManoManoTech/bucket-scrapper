# bucket-scrapper

Search through compressed S3 objects at scale. Stream-decompress, filter lines by regex, and forward results to local files or an HTTP API -- without ever buffering a full object in memory.

## Installation

```bash
cargo build --release
# Binary: target/release/bucket-scrapper
```

## How it works

Three decisions drive every run:

1. **Which files to read from S3?** -- Buckets, prefix paths, date ranges, key filters
2. **Which lines to keep?** -- All lines, or only those matching a regex
3. **Where to send them?** -- Local zstd files, or an HTTP endpoint

Everything streams: downloads decompress on the fly, lines are filtered as they appear, and results are written continuously.

## 1. Selecting S3 files

### Config file

A config file (`config-scrapper.yml` by default) defines which buckets and prefix paths to scan:

```yaml
buckets:
  - bucket: my-log-bucket
    path:
      - static_path: "log-archives/"
      - datefmt: "dt=%Y%m%d/hour=%H"
    only_prefix_patterns:          # optional: only scan prefixes matching these regexes
      - "service-a"

region: eu-west-3
output_dir: ./scrapper-output
```

Path components are either literal strings (`static_path`) or date patterns (`datefmt`). Supported date formats: `dt=%Y%m%d/hour=%H` (strftime) or `2006/01/02/15` (Go reference time). Each bucket must have at least one `datefmt` component to avoid listing the entire bucket.

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

### File output (default)

Results are written as zstd-compressed files in `output_dir`, grouped by date/hour prefix:

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z --line-pattern-regex "ERROR"
```

### HTTP output

Sends matched lines as zstd-compressed NDJSON batches to an HTTP endpoint, with adaptive (AIMD) throttling and 429 back-off:

```bash
bucket-scrapper -s 2024-01-15T10:00:00Z \
  --line-pattern-regex "ERROR" \
  --http-output \
  --http-url "https://logs.example.com/api/v1/logs" \
  --http-bearer-auth "your-token"
```

URL and token can also come from environment variables (`HTTP_URL`, `HTTP_BEARER_AUTH`).

HTTP defaults can be set in the config file:

```yaml
http_output:
  url: https://logs.example.com/api/v1/logs
  api_key: your-api-key
  timeout_secs: 30
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
| `--config <CONFIG>` | `config-scrapper.yml` | Config file path |
| `-r, --region <REGION>` | `eu-west-3` | AWS region |
| `-v, --log-level <LOG_LEVEL>` | `info` | trace, debug, info, warn, error |
| `--log-format <LOG_FORMAT>` | `text` | `text` or `json` |

### File selection & filtering

| Flag | Default | Description |
|------|---------|-------------|
| `-f, --filter <FILTER>` | | Regex on S3 object keys |
| `--line-pattern-regex <REGEX>` | (all lines) | Regex to filter lines |
| `-i, --ignore-case` | false | Case insensitive matching |

### HTTP output

| Flag | Default | Description |
|------|---------|-------------|
| `--http-output` | false | Enable HTTP output mode |
| `--http-url` | `HTTP_URL` env | Endpoint URL |
| `--http-bearer-auth` | `HTTP_BEARER_AUTH` env | Bearer token |
| `--http-batch-max-mb` | 2 | Max batch size (MB) |
| `--http-timeout` | 30 | Request timeout (seconds) |

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
