//! `OutputSink` adapter for [`HttpResultWriter`].
//!
//! Holds the HTTP writer behind a `Mutex<Option<...>>` so [`OutputSink::finish`]
//! can run on `&self` and consume the inner writer (whose `finish` consumes
//! `self` because it joins compressor + uploader task handles).

use super::http_writer::HttpResultWriter;
use super::observer::PipelineObserver;
use super::output::{BoxFinishFuture, OutputSink, OutputStats};
use anyhow::{anyhow, Result};
use serde_json::json;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

pub struct HttpOutputSink {
    /// Weak handle to the line sender. `ingest` upgrades, sends, drops the
    /// upgrade; this avoids keeping the channel open after `finish` drops the
    /// writer's strong sender (which would make compressor tasks block on
    /// `recv_async` forever).
    sender: flume::WeakSender<Vec<u8>>,
    /// Owned writer, taken by `finish`.
    inner: Mutex<Option<HttpResultWriter>>,
    fatal_error: Arc<AtomicBool>,
    observer: Mutex<Option<PipelineObserver>>,
    url: String,
}

impl HttpOutputSink {
    pub fn new(writer: HttpResultWriter) -> Self {
        let sender = writer.get_sender().downgrade();
        let observer = writer.observer();
        let fatal_error = writer.fatal_error_flag();
        let url = writer.url().to_string();
        Self {
            sender,
            inner: Mutex::new(Some(writer)),
            fatal_error,
            observer: Mutex::new(Some(observer)),
            url,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

impl OutputSink for HttpOutputSink {
    fn ingest(&self, _prefix: &str, line: &[u8]) -> Result<()> {
        let sender = self
            .sender
            .upgrade()
            .ok_or_else(|| anyhow!("HTTP consumer gone, channel closed"))?;
        sender
            .send(line.to_vec())
            .map_err(|_| anyhow!("HTTP consumer gone, channel closed"))
    }

    fn finish<'a>(&'a self) -> BoxFinishFuture<'a> {
        Box::pin(async move {
            let writer = self
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .ok_or_else(|| anyhow!("HTTP sink already finished"))?;

            let stats = writer.finish().await?;
            Ok(OutputStats {
                matched_lines: stats.lines_sent as u64,
                plaintext_bytes: stats.plaintext_bytes_sent as u64,
                compressed_bytes: stats.compressed_bytes_sent as u64,
                lines_dropped: stats.lines_dropped as u64,
                extras: serde_json::Map::from_iter([("url".to_string(), json!(self.url))]),
            })
        })
    }

    fn observer(&self) -> Option<PipelineObserver> {
        self.observer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    fn fatal_error_flag(&self) -> Option<Arc<AtomicBool>> {
        Some(self.fatal_error.clone())
    }

    fn type_name(&self) -> &'static str {
        "http"
    }
}
