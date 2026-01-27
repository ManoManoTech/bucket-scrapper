use bucket_scrapper::search::{SearchConfig, SearchResultCollector, StreamSearcher};
use std::io::Cursor;

#[test]
fn test_regex_matcher_cached() {
    // Create searcher with a pattern
    let config = SearchConfig {
        pattern: "ERROR".to_string(),
        ignore_case: false,
        count_only: false,
    };

    let searcher = StreamSearcher::new(config).expect("Failed to create searcher");

    // Test searching multiple times (regex should be reused)
    let test_data1 = b"Line 1\nERROR: Something went wrong\nLine 3";
    let test_data2 = b"Another ERROR here\nNo problems\nERROR again";

    let mut collector1 = SearchResultCollector::new();
    searcher
        .search_stream("bucket1", "key1", Cursor::new(test_data1), &mut collector1)
        .expect("Failed to search stream 1");

    let mut collector2 = SearchResultCollector::new();
    searcher
        .search_stream("bucket2", "key2", Cursor::new(test_data2), &mut collector2)
        .expect("Failed to search stream 2");

    let result1 = collector1.into_result();
    let result2 = collector2.into_result();

    assert_eq!(
        result1.total_matches, 1,
        "Should find 1 match in first stream"
    );
    assert_eq!(
        result2.total_matches, 2,
        "Should find 2 matches in second stream"
    );
}

#[test]
fn test_no_line_numbers() {
    // Create searcher
    let config = SearchConfig {
        pattern: "test".to_string(),
        ignore_case: true,
        count_only: false,
    };

    let searcher = StreamSearcher::new(config).expect("Failed to create searcher");

    let test_data = b"This is a TEST line\nAnother test here\nNo match";
    let mut collector = SearchResultCollector::new();

    searcher
        .search_stream("bucket", "key", Cursor::new(test_data), &mut collector)
        .expect("Failed to search");

    let result = collector.into_result();

    // We don't care about line numbers anymore - they're kept for API compatibility
    // but we're not using context lines which was the main performance issue

    assert_eq!(result.total_matches, 2, "Should find 2 matches");
}

#[test]
fn test_count_only_mode() {
    // Create searcher in count-only mode
    let config = SearchConfig {
        pattern: "\\d+".to_string(), // Match numbers
        ignore_case: false,
        count_only: true,
    };

    let searcher = StreamSearcher::new(config).expect("Failed to create searcher");

    let test_data = b"123 errors\n456 warnings\n789 info messages";
    let mut collector = SearchResultCollector::new();

    searcher
        .search_stream("bucket", "key", Cursor::new(test_data), &mut collector)
        .expect("Failed to search");

    let result = collector.into_result();

    // In count-only mode, we don't store actual matches
    assert_eq!(
        result.matches.len(),
        0,
        "Should not store matches in count-only mode"
    );
    assert_eq!(
        result.file_counts.get("bucket/key"),
        Some(&3),
        "Should count 3 matches"
    );
}
