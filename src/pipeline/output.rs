//! Trait abstraction for pipeline outputs.
//!
//! Filter workers call [`OutputSink::ingest`] for every matched line. The
//! trait is `dyn`-friendly (boxed-future return on `finish`) so the orchestrator
//! holds a single `Arc<dyn OutputSink>` regardless of which output type is
//! configured (file / http / s3 / void).

use crate::pipeline::observer::PipelineObserver;
use anyhow::Result;
use serde_json::{Map, Value};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Boxed future used by [`OutputSink::finish`] to make the trait `dyn`-safe
/// while still allowing `async` work in the implementation.
pub type BoxFinishFuture<'a> = Pin<Box<dyn Future<Output = Result<OutputStats>> + Send + 'a>>;

/// Aggregate statistics emitted by a sink at end-of-run.
///
/// Common counters live as named fields; sink-specific extras (e.g. the HTTP
/// throttle's lines_dropped, the S3 batch count) ride along in `extras` so
/// the structured-log reporter can iterate them generically without growing
/// a per-sink branch.
#[derive(Debug, Default, Clone)]
pub struct OutputStats {
    pub matched_lines: u64,
    pub plaintext_bytes: u64,
    pub compressed_bytes: u64,
    pub lines_dropped: u64,
    pub extras: Map<String, Value>,
}

impl OutputStats {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Where matched lines flow.
///
/// Implementations must be cheap and non-blocking inside `ingest` — long
/// work (network IO, disk flush) is buffered and handed off to background
/// tasks, drained by `finish`.
pub trait OutputSink: Send + Sync {
    /// Called for every matched line by the filter workers. `prefix` is the
    /// source S3 prefix (used by sinks that partition output by date/hour).
    fn ingest(&self, prefix: &str, line: &[u8]) -> Result<()>;

    /// Drain in-flight buffers and return totals. Called once at end of run.
    /// Implementations that consume per-call resources (channels, encoders)
    /// should take them via interior mutability so this can run on `&self`.
    fn finish<'a>(&'a self) -> BoxFinishFuture<'a>;

    /// Optional channel-fill observer surfaced to progress reporting.
    /// Returning `Some` engages the HTTP-style progress format.
    fn observer(&self) -> Option<PipelineObserver> {
        None
    }

    /// Optional fatal-error flag: when the sink hits a non-retryable failure
    /// (e.g. HTTP 4xx) it sets this to true so the download coordinator and
    /// filter workers can bail out instead of blocking on full channels.
    fn fatal_error_flag(&self) -> Option<Arc<AtomicBool>> {
        None
    }

    /// One-line label describing the sink for end-of-run logging.
    fn type_name(&self) -> &'static str;
}
