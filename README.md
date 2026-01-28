# Bucket Scrapper

A high-performance tool for searching through S3 bucket contents using ripgrep patterns.

## Features

- **Fast regex searching** - Uses ripgrep's powerful search engine
- **S3 native** - Direct integration with AWS S3
- **Compression support** - Handles .gz and .zst files automatically
- **Parallel processing** - Configurable concurrent downloads
- **Memory efficient** - Streaming architecture with backpressure control
- **Organized output** - Results saved to compressed files by date/hour
- **Clean archive format** - One `.gz` file per date/hour prefix

## Installation

```bash
# Build from source
cargo build --release

# The binary will be at target/release/bucket-scrapper
```

## Configuration

The tool looks for `config-scrapper.yml` by default:

```yaml
buckets:
  - bucket: support-infra-log-consolidator-archives
    path:
      - datefmt: "log-archives/dt=%Y%m%d/hour=%H"

region: eu-west-3
output_dir: ./scrapper-output
```

## Usage

### Search for patterns in S3 objects

Results are automatically saved to compressed files in the output directory, organized by date/hour.

```bash
# Search using config file buckets
bucket-scrapper search -p "ERROR.*timeout"
# Creates: ./scrapper-output/20240115H10.gz, ./scrapper-output/20240115H11.gz, etc.

# Search specific date range
bucket-scrapper search -p "exception" \
  --start 2024-01-15T10:00:00Z \
  --end 2024-01-15T12:00:00Z
# Creates: ./scrapper-output/20240115H10.gz, ./scrapper-output/20240115H11.gz

# Case insensitive search
bucket-scrapper search -p "failed" -i

# Count matches only (still saves to files)
bucket-scrapper search -p "WARNING" --count

# JSON summary output
bucket-scrapper search -p "error" --output json
```

### Output Format

Each output file (e.g., `20240115H10.gz`) contains JSON lines with matching entries:

```json
{"file":"support-infra-log-consolidator-archives/log-archives/dt=20240115/hour=10/app.json.zst","line":142,"content":"ERROR: Connection timeout to database"}
{"file":"support-infra-log-consolidator-archives/log-archives/dt=20240115/hour=10/api.json.zst","line":89,"content":"ERROR: Authentication failed for user"}
```

### List objects in buckets

```bash
# List all objects
bucket-scrapper list -b my-logs

# Filter by pattern
bucket-scrapper list -b my-logs -f "*.gz"

# List with date range
bucket-scrapper list -b my-archive \
  --start 2024-01-01T00:00:00Z \
  --end 2024-01-31T23:59:59Z
```

## Command Line Options

### Global Options
- `-c, --config` - Config file path (default: config-scrapper.yml)
- `-r, --region` - AWS region (default: eu-west-3)
- `-v, --log-level` - Log level: trace, debug, info, warn, error

### Search Options
- `-p, --pattern` - Regex pattern to search for (required)
- `-b, --buckets` - S3 buckets to search (comma-separated)
- `-f, --filter` - Object key filter pattern
- `-s, --start` - Start date (ISO 8601 format)
- `-e, --end` - End date (defaults to now)
- `-C, --context` - Lines of context around matches
- `-i, --ignore-case` - Case insensitive search
- `-c, --count` - Only show match counts
- `--output` - Output format: text, json, quiet

### Performance Tuning
- `--max-parallel` - Max parallel downloads (default: 32)
- `--buffer-size-kb` - Stream buffer size (default: 64)
- `--channel-buffer` - Channel buffer size (default: 100)
- `--max-retries` - Max retry attempts (default: 10)
- `--retry-delay` - Initial retry delay in seconds (default: 2)

## AWS Authentication

The tool uses standard AWS authentication methods:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (when running on AWS infrastructure)
4. AWS SSO (`aws sso login`)

## Examples

### Finding errors in application logs
```bash
bucket-scrapper search -p "ERROR|FATAL" -b app-logs --ignore-case
```

### Searching for specific user activity
```bash
bucket-scrapper search -p "userId=12345" -b api-logs,app-logs -C 5
```

### Analyzing log patterns over time
```bash
bucket-scrapper search -p "timeout|deadline" -b prod-logs \
  --start 2024-01-01T00:00:00Z \
  --count \
  --output json
```

### Finding large response payloads
```bash
bucket-scrapper search -p '"size":\s*[0-9]{7,}' -b api-responses
```

## Performance Considerations

- The tool currently downloads files to memory before searching (streaming improvements planned)
- Use `--max-parallel` to control memory usage on large datasets
- Use `--filter` to reduce the number of objects to process
- Date ranges significantly improve performance for time-partitioned data

## License

See LICENSE file in the repository.