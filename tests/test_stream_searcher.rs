use bucket_scrapper::pipeline::{LineMatcher, MatcherConfig};

#[test]
fn test_regex_matcher_cached() {
    let config = MatcherConfig {
        pattern: Some("ERROR".to_string()),
        ignore_case: false,
    };
    let searcher = LineMatcher::new(config).unwrap();

    assert!(searcher.matches_line(b"ERROR: bad"));
    assert!(!searcher.matches_line(b"Line 1"));
    assert!(!searcher.matches_line(b"Line 3"));

    // Second "file" — same searcher instance
    assert!(searcher.matches_line(b"Another ERROR"));
    assert!(!searcher.matches_line(b"Ok"));
    assert!(searcher.matches_line(b"ERROR again"));
}

#[test]
fn test_case_insensitive() {
    let config = MatcherConfig {
        pattern: Some("test".to_string()),
        ignore_case: true,
    };
    let searcher = LineMatcher::new(config).unwrap();

    assert!(searcher.matches_line(b"This is a TEST line"));
    assert!(searcher.matches_line(b"Another test here"));
    assert!(!searcher.matches_line(b"No match"));
}

#[test]
fn test_all_lines_mode() {
    let config = MatcherConfig {
        pattern: None,
        ignore_case: false,
    };
    let searcher = LineMatcher::new(config).unwrap();

    assert!(searcher.matches_line(b"Line 1"));
    assert!(searcher.matches_line(b"Line 2"));
    assert!(searcher.matches_line(b"Line 3"));
    assert!(searcher.matches_line(b""));
}

#[test]
fn test_invalid_regex_returns_error() {
    let config = MatcherConfig {
        pattern: Some("[invalid".to_string()),
        ignore_case: false,
    };
    let result = LineMatcher::new(config);
    let err = result.err().expect("should be Err for invalid regex");
    let msg = format!("{err:#}");
    assert!(msg.contains("Invalid regex pattern"), "got: {msg}");
}

#[test]
fn test_empty_pattern_matches_all() {
    let config = MatcherConfig {
        pattern: None,
        ignore_case: false,
    };
    let searcher = LineMatcher::new(config).unwrap();
    assert!(searcher.matches_line(b"anything"));
    assert!(searcher.matches_line(b""));
    assert!(searcher.matches_line(b"\xff\xfe binary"));
}
