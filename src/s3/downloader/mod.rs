// src/s3/downloader/mod.rs
mod downloader;
mod download_orchestrator;
mod processor;
mod types;

pub use download_orchestrator::DownloadOrchestrator;
