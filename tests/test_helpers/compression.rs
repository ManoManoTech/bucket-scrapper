/// Compression and decompression utilities for tests
/// Provides helpers for zstd and gzip operations commonly used in tests

use std::io::Write;

/// Compress data using zstd
pub fn compress_with_zstd(data: &[u8]) -> Vec<u8> {
    let mut encoder = zstd::Encoder::new(Vec::new(), 3).unwrap();
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// Decompress zstd compressed data
pub fn decompress_zstd(compressed_data: &[u8]) -> Vec<u8> {
    let mut decoder = zstd::Decoder::new(compressed_data).unwrap();
    let mut decompressed = Vec::new();
    std::io::copy(&mut decoder, &mut decompressed).unwrap();
    decompressed
}

/// Compress data using gzip
#[allow(dead_code)]
pub fn compress_with_gzip(data: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// Decompress gzip compressed data
#[allow(dead_code)]
pub fn decompress_gzip(compressed_data: &[u8]) -> Vec<u8> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(compressed_data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();
    decompressed
}

/// Compression utilities for JSON data specifically
#[allow(dead_code)]
pub struct JsonCompression;

impl JsonCompression {
    /// Compress JSON string with zstd and return compressed bytes
    pub fn compress_json_zstd(json_str: &str) -> Vec<u8> {
        compress_with_zstd(json_str.as_bytes())
    }

    /// Compress JSON string with gzip and return compressed bytes
    pub fn compress_json_gzip(json_str: &str) -> Vec<u8> {
        compress_with_gzip(json_str.as_bytes())
    }

    /// Decompress zstd data and return as JSON string
    pub fn decompress_json_zstd(compressed_data: &[u8]) -> Result<String, std::string::FromUtf8Error> {
        let decompressed = decompress_zstd(compressed_data);
        String::from_utf8(decompressed)
    }

    /// Decompress gzip data and return as JSON string
    pub fn decompress_json_gzip(compressed_data: &[u8]) -> Result<String, std::string::FromUtf8Error> {
        let decompressed = decompress_gzip(compressed_data);
        String::from_utf8(decompressed)
    }

    /// Compress multiple JSON lines into a single zstd file
    pub fn compress_json_lines_zstd(json_lines: Vec<String>) -> Vec<u8> {
        let combined = json_lines.join("\n");
        compress_with_zstd(combined.as_bytes())
    }

    /// Decompress zstd data and split into JSON lines
    pub fn decompress_to_json_lines_zstd(compressed_data: &[u8]) -> Result<Vec<String>, std::string::FromUtf8Error> {
        let decompressed_str = Self::decompress_json_zstd(compressed_data)?;
        Ok(decompressed_str.lines().map(|s| s.to_string()).collect())
    }
}

/// Test compression roundtrip functionality
#[allow(dead_code)]
pub struct CompressionTester;

impl CompressionTester {

    pub fn test_json_zstd_roundtrip(json_str: &str) -> bool {
        match JsonCompression::decompress_json_zstd(
            &JsonCompression::compress_json_zstd(json_str)
        ) {
            Ok(decompressed) => json_str == decompressed,
            Err(_) => false,
        }
    }
}
