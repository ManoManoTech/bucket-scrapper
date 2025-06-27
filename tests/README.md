# Testing Guide

## Overview

This document describes the testing infrastructure for the **log-consolidator-checker** (a Rust tool that verifies the log-consolidator's work), including how to use the MockDataPopulator for realistic end-to-end testing of the checker's validation capabilities.

## Understanding the System

- **log-consolidator** (Golang) = Recompresses logs from .gz to .zstd and groups by env/service
- **log-consolidator-checker** (Rust) = Verifies that consolidation was performed correctly 
- **These tests** = Test the checker's ability to validate consolidation results

## Test Structure

### Unit Tests
- **Location**: Embedded in source files (e.g., `src/utils/date.rs`)
- **Purpose**: Test individual functions and components in isolation
- **Run with**: `cargo test --lib`

### Integration Tests
- **Location**: `tests/` directory
- **Purpose**: Test component interactions and S3 operations
- **Run with**: `cargo test --test integration_*`

### End-to-End Tests
- **Location**: `tests/integration_end_to_end.rs`
- **Purpose**: Test complete workflows (list, check commands)
- **Run with**: `cargo test --test integration_end_to_end`

## MockDataPopulator Usage

### Consolidation Test Scenarios

The MockDataPopulator creates realistic test scenarios that simulate the entire consolidation pipeline:

1. **Input buckets** - Contains original .gz files (what went INTO the consolidator)
2. **Consolidated bucket** - Contains .zstd files grouped by env-service (what came OUT of the consolidator)  
3. **Checker verification** - Tests that the checker can validate the consolidation was successful

### Environment Setup

The MockDataPopulator supports two modes:

#### 1. Real Data Mode (Recommended for Local Development)
```bash
export MOCK_DATA_FOLDER="/path/to/your/test/data"
mkdir -p "$MOCK_DATA_FOLDER/inputs"
# Place compressed log files (.json.gz, .json.zst, .gz) in inputs/ folder
```

#### 2. Synthetic Data Mode (Default for CI/CD)
```bash
# No environment setup needed
# Tests automatically fall back to synthetic data generation
```

### Data Structure Requirements

For real data mode, organize files as:
```
$MOCK_DATA_FOLDER/
└── inputs/
    ├── app-service-001.json.gz
    ├── web-server-logs.json.zst
    ├── api-gateway.gz
    └── database-queries.json.gz
```

### Supported File Types
- `.json.gz` - Gzipped JSON log files
- `.json.zst` - Zstd compressed JSON log files
- `.gz` - Generic gzipped files

## Test Environment Architecture

### Standard 6-Bucket Environment
- **Input buckets**: `input-bucket-1`, `input-bucket-2`
- **Archive buckets**: `archive-bucket-1`, `archive-bucket-2` (legacy support)
- **Consolidated bucket**: `consolidated-bucket`
- **Results bucket**: `results-bucket`

### S3 Key Structure
Generated keys follow the pattern:
```
raw-logs/dt=YYYYMMDD/hour=HH/service-name.ext
```

Example:
```
raw-logs/dt=20231225/hour=14/app-service-001.json.gz
```

## Running Tests

### Local Development
```bash
# With real mock data
export MOCK_DATA_FOLDER="/path/to/test/data"
cargo test --test integration_end_to_end

# With synthetic data only
unset MOCK_DATA_FOLDER
cargo test --test integration_end_to_end
```

### CI/CD Environment
```bash
# Tests automatically use synthetic data
cargo test --test integration_end_to_end
```

### Specific Test Cases
```bash
# Test list command workflow
cargo test test_end_to_end_list_command_workflow

# Test check command workflow
cargo test test_end_to_end_check_command_workflow

# Test multi-bucket scenarios
cargo test test_end_to_end_multiple_date_hours

# Test error handling
cargo test test_end_to_end_error_scenarios
```

## Test Data Characteristics

### Synthetic Data Features
- **Realistic consolidation pipeline**: Creates both input (.gz) and consolidated (.zstd) files from same base data
- **Proper compression**: Input files use gzip, consolidated files use zstd (matching real consolidator behavior)  
- **Environment/service grouping**: Consolidated files are grouped by env-service combinations
- **Character count consistency**: Same log content ensures character counts match between input and consolidated
- **Multiple environments**: production, staging
- **Multiple services**: app, web, api, db

### File Patterns Generated

**Input buckets** (original logs):
- `app-0.log.gz` - Application logs from input-0 
- `web-1.log.gz` - Web server logs from input-1
- `api-0.log.gz` - API logs from input-0
- `db-1.log.gz` - Database logs from input-1

**Consolidated bucket** (processed logs):
- `production-app-consolidated.json.zst` - All production app logs consolidated
- `staging-web-consolidated.json.zst` - All staging web logs consolidated  
- `production-api-consolidated.json.zst` - All production API logs consolidated

## Troubleshooting

### Common Issues

1. **No files found in bucket**
   - Check `$MOCK_DATA_FOLDER` environment variable
   - Verify `inputs/` subfolder exists
   - Ensure files have supported extensions

2. **MinIO container startup failures**
   - Check Docker is running
   - Verify available ports (9000)
   - Increase timeout if needed

3. **Test timeouts**
   - Increase file count gradually
   - Check memory limits for large datasets
   - Consider parallel processing limits

### Debug Output
Tests include debug output:
```
Using real mock data from $MOCK_DATA_FOLDER
Populated 12 files across 2 buckets
Found 8 total files across 2 archived buckets
Check result with MockDataPopulator: ok=true, message=...
```

## Best Practices

1. **Use real data when available** - Provides more realistic testing scenarios
2. **Start with small datasets** - Easier to debug and understand
3. **Test both success and failure cases** - Include error scenarios
4. **Verify compression** - Ensure files are properly compressed
5. **Monitor memory usage** - Large datasets may require memory tuning

## Integration with MockDataPopulator

### Key Classes
- `MockDataPopulator` - Main data population logic
- `PopulationUtils` - Utility functions and standard mappings
- `PopulationSummary` - Results and statistics

### Example Usage in Tests
```rust
// Setup
let populator = match MockDataPopulator::from_env() {
    Ok(populator) => populator,
    Err(_) => MockDataPopulator::new("/tmp"), // Synthetic mode
};

// Populate
let summary = populator
    .populate_with_synthetic_data(&clients, &bucket_mappings, 8)
    .await?;

// Verify
println!("Populated {} files", summary.total_files());
```

This testing infrastructure ensures reliable, realistic testing across both development and CI/CD environments.