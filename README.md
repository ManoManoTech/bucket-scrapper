# bucket-scrapper

High-performance S3 bucket content searcher. Downloads compressed objects, stream-decompresses line-by-line, filters by regex, and outputs to local zstd files or an HTTP API.

## Features

- **Streaming pipeline** -- no full decompressed object ever held in memory
- **Regex filtering** -- ripgrep-based line matching (or omit pattern to extract all lines)
- **Dual output** -- local zstd files grouped by date/hour, or HTTP streaming (HTTP/NDJSON)
- **Parallel downloads** -- semaphore-bounded concurrent S3 fetches
- **AIMD upload throttle** -- adaptive rate control for HTTP output with 429 back-off
- **Compression support** -- reads `.gz` and `.zst` objects automatically
- **Structured logging** -- text or JSON via `--log-format`, with bottleneck detection

## Installation

```bash
cargo build --release
# Binary: target/release/bucket-scrapper
```

## Configuration

Optional config file (`config-scrapper.yml` by default):

```yaml
buckets:
  - bucket: my-log-bucket
    path:
      - static_path: "log-archives/"
      - datefmt: "dt=%Y%m%d/hour=%H"
    only_prefix_patterns:          # optional: regex filters on S3 key prefixes
      - "service-a"

region: eu-west-3
output_dir: ./scrapper-output

# Optional: HTTP output defaults
http_output:
  url: https://intake.handy-mango.http.com/api/v1/logs
  api_key: your-api-key
  timeout_secs: 30
```

Path components use two types:
- `static_path` -- literal prefix string
- `datefmt` -- date pattern: `dt=%Y%m%d/hour=%H` (common format) or `2006/01/02/15` (Go reference time)

Each bucket must have at least one `datefmt` component to avoid listing the entire bucket.

## Usage

### File output (default)

Results are written as zstd-compressed files in the output directory, grouped by date/hour prefix.

```bash
# Extract all lines from a date range
bucket-scrapper -s 2024-01-15T10:00:00Z -e 2024-01-15T12:00:00Z

# Filter lines by regex pattern
bucket-scrapper -s 2024-01-15T10:00:00Z --line-pattern-regex "ERROR.*timeout"

# Case insensitive
bucket-scrapper -s 2024-01-15T10:00:00Z --line-pattern-regex "failed" -i

# Filter S3 object keys (only process matching files)
bucket-scrapper -s 2024-01-15T10:00:00Z -f "service-a.*\.json\.zst$"
```

### HTTP output (HTTP API)

Sends matched lines as zstd-compressed NDJSON batches with AIMD-based adaptive throttling.

```bash
# Send to HTTP API
bucket-scrapper -s 2024-01-15T10:00:00Z \
  --line-pattern-regex "ERROR" \
  --http-output \
  --http-url "https://intake.handy-mango.http.com/api/v1/logs" \
  --http-api-key "your-api-key"

# Using environment variables
export HTTP_URL="https://intake.handy-mango.http.com/api/v1/logs"
export HTTP_BEARER_AUTH="your-api-key"
bucket-scrapper -s 2024-01-15T10:00:00Z --http-output --line-pattern-regex "ERROR"
```

## CLI Reference

Generated from `--help`:

```
Search through S3 bucket contents using ripgrep patterns

Usage: bucket-scrapper [OPTIONS] --start <START>
```

### Required

| Flag | Description |
|------|-------------|
| `-s, --start <START>` | Start date in ISO 8601 format |

### General

| Flag | Default | Description |
|------|---------|-------------|
| `--config <CONFIG>` | `config-scrapper.yml` | Config file path |
| `-r, --region <REGION>` | `eu-west-3` | AWS region |
| `-v, --log-level <LOG_LEVEL>` | `info` | Log level: trace, debug, info, warn, error |
| `--log-format <LOG_FORMAT>` | `text` | Log format: `text` (human-readable) or `json` (structured) |
| `-e, --end <END>` | now | End date in ISO 8601 format |

### Search

| Flag | Default | Description |
|------|---------|-------------|
| `--line-pattern-regex <REGEX>` | (all lines) | Regex pattern to filter lines |
| `-f, --filter <FILTER>` | | Regex filter on S3 object keys |
| `-i, --ignore-case` | false | Case insensitive matching |

### Performance Tuning

| Flag | Default | Description |
|------|---------|-------------|
| `--max-parallel` | 32 | Max concurrent S3 downloads |
| `--filter-tasks` | cpu/2 | Number of regex filter worker tasks |
| `--line-buffer-size` | 1000 | Line channel capacity between decompress and filter |
| `--max-retries` | 10 | Max retry attempts for failed downloads |
| `--retry-delay` | 2 | Initial retry delay in seconds |
| `--progress-interval` | 3 | Progress report interval in seconds (supports fractional) |
| `--compression-level` | 3 | Zstd compression level (1-22) |
| `--memory-limit-gb` | 0 | Memory limit via setrlimit (0 = no limit) |
| `--client-max-age` | 60 | S3 client max age in minutes (longer = fewer DNS lookups) |

### HTTP Output

| Flag | Default | Description |
|------|---------|-------------|
| `--http-output` | false | Enable HTTP output mode |
| `--http-url` | `HTTP_URL` env | HTTP API URL |
| `--http-api-key` | `HTTP_BEARER_AUTH` env | API key for authentication |
| `--http-batch-max-mb` | 2 | Max batch size in MB |
| `--http-timeout` | 30 | HTTP request timeout in seconds |
| `--http-line-channel-size` | 1000 | Line channel capacity before compressors |
| `--http-compressor-tasks` | cpu/8 | Number of zstd compressor tasks |
| `--http-upload-tasks` | 4x compressors | Number of concurrent upload tasks |
| `--http-upload-channel-size` | 4 | Batch channel between compressors and uploaders |

### AIMD Throttle

| Flag | Default | Description |
|------|---------|-------------|
| `--max-submission-time` | 3.0 | Per-batch time threshold in seconds (0 = disable AIMD) |
| `--max-upload-rate` | 0 | Global upload rate limit in MB/s (0 = unlimited) |
| `--http-aimd-decrease-factor` | 0.15 | Multiplicative decrease factor on congestion |
| `--http-aimd-increase` | 1.0 | Additive increase in MB/s per healthy batch |

## Architecture

```
S3 download tasks (--max-parallel, semaphore-bounded)
  |
  | spawn_blocking: stream-decompress (.gz/.zst) line-by-line
  v
line channel (--line-buffer-size)
  |
  | filter_worker pool (--filter-tasks, spawn_blocking + regex)
  v
FilterOutput enum
  |
  +-- Http --> compressor pool (--http-compressor-tasks, zstd)
  |              --> batch channel (--http-upload-channel-size)
  |                    --> uploader pool (--http-upload-tasks, HTTP POST)
  |
  +-- File --> SharedFileWriter (per-prefix Mutex<ZstdEncoder<File>>)
```

Every stage boundary is observable. The progress reporter detects bottlenecks by checking channel fill levels (>80% = that stage is the bottleneck).

## AWS Authentication

Uses standard AWS SDK credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (when running on AWS infrastructure)
4. AWS SSO (`aws sso login`)

Custom CA bundles are supported via `AWS_CA_BUNDLE` environment variable.
