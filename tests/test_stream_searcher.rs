use bucket_scrapper::search::{SearchConfig, SearchResultCollector, StreamSearcher};
use std::io::Cursor;

#[test]
fn test_regex_matcher_cached() {
    // Create searcher with a pattern
    let config = SearchConfig {
        pattern: Some("ERROR".to_string()),
        ignore_case: false,
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
        pattern: Some("test".to_string()),
        ignore_case: true,
    };

    let searcher = StreamSearcher::new(config).expect("Failed to create searcher");

    let test_data = b"This is a TEST line\nAnother test here\nNo match";
    let mut collector = SearchResultCollector::new();

    searcher
        .search_stream("bucket", "key", Cursor::new(test_data), &mut collector)
        .expect("Failed to search");

    let result = collector.into_result();

    assert_eq!(result.total_matches, 2, "Should find 2 matches");
}

#[test]
fn test_all_lines_mode() {
    // No pattern = all lines
    let config = SearchConfig {
        pattern: None,
        ignore_case: false,
    };

    let searcher = StreamSearcher::new(config).expect("Failed to create searcher");

    let test_data = b"Line 1\nLine 2\nLine 3";
    let mut collector = SearchResultCollector::new();

    searcher
        .search_stream("bucket", "key", Cursor::new(test_data), &mut collector)
        .expect("Failed to search");

    let result = collector.into_result();

    assert_eq!(result.total_matches, 3, "Should yield all 3 lines");
    assert_eq!(result.matches.len(), 3, "Should store all 3 matches");
}
