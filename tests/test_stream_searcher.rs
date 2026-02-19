use bucket_scrapper::pipeline::{SearchExporter, SearchConfig, StreamSearcher};
use std::io::Cursor;

/// Minimal exporter for tests — just counts matches.
struct CountingExporter(usize);

impl SearchExporter for CountingExporter {
    fn add_match(&mut self, _: &str) -> anyhow::Result<()> {
        self.0 += 1;
        Ok(())
    }
    fn match_count(&self) -> usize {
        self.0
    }
}

#[test]
fn test_regex_matcher_cached() {
    let config = SearchConfig {
        pattern: Some("ERROR".to_string()),
        ignore_case: false,
    };
    let searcher = StreamSearcher::new(config).unwrap();

    let mut c1 = CountingExporter(0);
    searcher
        .search_stream("b", "k1", Cursor::new(b"Line 1\nERROR: bad\nLine 3"), &mut c1)
        .unwrap();

    let mut c2 = CountingExporter(0);
    searcher
        .search_stream("b", "k2", Cursor::new(b"Another ERROR\nOk\nERROR again"), &mut c2)
        .unwrap();

    assert_eq!(c1.match_count(), 1);
    assert_eq!(c2.match_count(), 2);
}

#[test]
fn test_no_line_numbers() {
    let config = SearchConfig {
        pattern: Some("test".to_string()),
        ignore_case: true,
    };
    let searcher = StreamSearcher::new(config).unwrap();

    let mut c = CountingExporter(0);
    searcher
        .search_stream("b", "k", Cursor::new(b"This is a TEST line\nAnother test here\nNo match"), &mut c)
        .unwrap();

    assert_eq!(c.match_count(), 2);
}

#[test]
fn test_all_lines_mode() {
    let config = SearchConfig {
        pattern: None,
        ignore_case: false,
    };
    let searcher = StreamSearcher::new(config).unwrap();

    let mut c = CountingExporter(0);
    searcher
        .search_stream("b", "k", Cursor::new(b"Line 1\nLine 2\nLine 3"), &mut c)
        .unwrap();

    assert_eq!(c.match_count(), 3);
}
