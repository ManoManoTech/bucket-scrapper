/// Static test data and fixtures for integration tests
/// This module contains predefined test data that can be reused across tests

/// Sample JSON log entries for testing
pub struct JsonFixtures;

impl JsonFixtures {
    /// Simple debug JSON objects with env, service, and basic fields
    pub fn debug_objects() -> Vec<&'static str> {
        vec![
            r#"{"env": "production", "service": "frontend", "a": 1, "b": 2, "c": 3}"#,
            r#"{"env": "staging", "service": "api", "a": 1, "b": 2, "c": 3}"#,
            r#"{"env": "development", "service": "database", "a": 1, "b": 2, "c": 3}"#,
        ]
    }

    /// Realistic log entries with different environments and services
    #[allow(dead_code)]
    pub fn realistic_logs() -> Vec<&'static str> {
        vec![
            r#"{"env": "production", "service": "frontend", "message": "User login successful", "timestamp": "2023-12-25T14:30:00Z", "user_id": "12345"}"#,
            r#"{"env": "production", "service": "frontend", "error": "Connection timeout", "severity": "warning", "retry_count": 3}"#,
            r#"{"env": "staging", "service": "api", "endpoint": "/users", "method": "GET", "status": 200, "duration_ms": 150}"#,
            r#"{"env": "staging", "service": "api", "request_id": "req-12345", "duration_ms": 150, "cache_hit": true}"#,
            r#"{"env": "development", "service": "database", "query": "SELECT * FROM users", "duration": "5ms", "rows_returned": 42}"#,
            r#"{"env": "development", "service": "database", "connection_pool": "active", "count": 25, "max_connections": 100}"#,
        ]
    }

    /// Mixed environment and service combinations for comprehensive testing
    #[allow(dead_code)]
    pub fn mixed_environments() -> Vec<&'static str> {
        vec![
            r#"{"env": "production", "service": "frontend", "level": "info", "msg": "App started"}"#,
            r#"{"env": "production", "service": "backend", "level": "error", "msg": "Database connection failed"}"#,
            r#"{"env": "staging", "service": "frontend", "level": "debug", "msg": "User interaction logged"}"#,
            r#"{"env": "staging", "service": "cache", "level": "info", "msg": "Cache miss", "key": "user:12345"}"#,
            r#"{"env": "development", "service": "auth", "level": "trace", "msg": "Token validation", "valid": true}"#,
            r#"{"env": "test", "service": "mock", "level": "info", "msg": "Mock response generated"}"#,
        ]
    }

    /// Error and edge case JSON entries
    #[allow(dead_code)]
    pub fn error_cases() -> Vec<&'static str> {
        vec![
            r#"{"env": "production", "service": "frontend", "error": "Null pointer exception", "stack_trace": "..."}"#,
            r#"{"env": "staging", "service": "api", "timeout": true, "endpoint": "/slow-query", "duration_ms": 30000}"#,
            r#"{"env": "development", "service": "database", "connection_lost": true, "retry_attempts": 5}"#,
        ]
    }
}

/// Sample file structures for S3 tests
#[allow(dead_code)]
pub struct FileFixtures;

impl FileFixtures {
    /// Standard log file patterns with different extensions
    #[allow(dead_code)]
    pub fn log_files() -> Vec<(&'static str, &'static str)> {
        vec![
            ("logs/2023/12/25/14/app.log.gz", "Compressed log file"),
            ("logs/2023/12/25/14/data.json.zst", "Compressed JSON file"),
            (
                "logs/2023/12/25/14/metrics.json.gz",
                "Compressed metrics file",
            ),
            (
                "logs/2023/12/25/14/config.txt",
                "Plain text file - should be filtered out",
            ),
            (
                "logs/2023/12/25/14/readme.md",
                "Markdown file - should be filtered out",
            ),
        ]
    }
}

/// Test bucket names to avoid conflicts
#[allow(dead_code)]
pub struct BucketNames;

impl BucketNames {
    // Legacy bucket names (for compatibility)
    pub const INPUT_LOGS: &'static str = "test-input-logs";
    pub const ARCHIVED_LOGS: &'static str = "test-archived-logs";

    // Input bucket names (bucketsToConsolidate)
    pub const INPUT_BUCKET_1: &'static str = "input-bucket-1";
    pub const INPUT_BUCKET_2: &'static str = "input-bucket-2";

    // Archive bucket names (for legacy tests)
    pub const ARCHIVE_BUCKET_1: &'static str = "archive-bucket-1";
    pub const ARCHIVE_BUCKET_2: &'static str = "archive-bucket-2";

    // Consolidated bucket names (bucketsConsolidated)
    pub const CONSOLIDATED_BUCKET_1: &'static str = "consolidated-bucket-1";
    pub const CONSOLIDATED_BUCKET_2: &'static str = "consolidated-bucket-2";

    // Results bucket names (bucketsCheckerResults)
    pub const RESULTS_BUCKET_1: &'static str = "results-bucket-1";
    pub const RESULTS_BUCKET_2: &'static str = "results-bucket-2";

    /// Get all standard bucket names as a vector
    pub fn all_standard_buckets() -> Vec<&'static str> {
        vec![
            Self::INPUT_BUCKET_1,
            Self::INPUT_BUCKET_2,
            Self::CONSOLIDATED_BUCKET_1,
            Self::CONSOLIDATED_BUCKET_2,
            Self::RESULTS_BUCKET_1,
            Self::RESULTS_BUCKET_2,
        ]
    }

    /// Get input bucket names
    pub fn input_buckets() -> Vec<&'static str> {
        vec![Self::INPUT_BUCKET_1, Self::INPUT_BUCKET_2]
    }

    /// Get consolidated bucket names
    pub fn consolidated_buckets() -> Vec<&'static str> {
        vec![Self::CONSOLIDATED_BUCKET_1, Self::CONSOLIDATED_BUCKET_2]
    }

    /// Get results bucket names
    pub fn results_buckets() -> Vec<&'static str> {
        vec![Self::RESULTS_BUCKET_1, Self::RESULTS_BUCKET_2]
    }

    /// Get archive bucket names (legacy)
    pub fn archive_buckets() -> Vec<&'static str> {
        vec![Self::ARCHIVE_BUCKET_1, Self::ARCHIVE_BUCKET_2]
    }
}

/// Common test configurations as YAML strings
#[allow(dead_code)]
pub struct ConfigFixtures;

impl ConfigFixtures {
    /// Standard test configuration with multiple bucket types
    pub fn standard_config() -> &'static str {
        r#"
bucketsToConsolidate:
  - bucket: "integration-test-archived"
    path:
      - static_path: "raw-logs"
      - datefmt: "dt=placeholder/hour=99"
    only_prefix_patterns:
      - "^app-.*"
      - "^web-.*"

bucketsConsolidated:
  - bucket: "integration-test-consolidated"
    path:
      - static_path: "processed"
      - datefmt: "2006/01/02/15"

bucketsCheckerResults:
  - bucket: "integration-test-results"
    path:
      - static_path: "results"
"#
    }
}
