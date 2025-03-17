// src/s3/mod.rs
pub mod checker;
pub mod client;
pub mod downloader;

// For backward compatibility, re-export the S3Downloader
// from its new location
pub use downloader::S3Downloader;