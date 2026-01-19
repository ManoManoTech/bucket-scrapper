// src/s3/downloader/types.rs
use crate::utils::memory_limited_allocator::LimitedVec;

/// Enum representing different compression types
#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
}

/// Structure to pass data between download and processing stages
pub struct RawObjectData {
    pub bucket: String,
    pub key: String,
    pub data: LimitedVec,
    pub compression_type: CompressionType,
}
