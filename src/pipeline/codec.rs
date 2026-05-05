//! Output compression abstraction shared by file/http/s3 sinks.
//!
//! Each sink that compresses output goes through this module instead of
//! constructing its own `zstd::Encoder` and hardcoding `"zstd"` / `.zst`.
//! That keeps the file extension, the wire `Content-Encoding`, and the
//! actual encoder bytes in lockstep — change the codec and everything
//! follows.

use anyhow::{anyhow, Context, Result};
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use std::io::Write;

/// User-facing compression config block. Lives on each sink config that
/// compresses (`file`, `http`, `s3`) so per-sink overrides are possible.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompressionConfig {
    #[serde(default)]
    pub format: CodecFormat,
    /// Codec-specific compression level. `None` → format default
    /// (`zstd: 3`, `gzip: 6`). Must be `None` when format is `none`.
    #[serde(default)]
    pub level: Option<i32>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodecFormat {
    #[default]
    Zstd,
    Gzip,
    None,
}

/// Resolved codec + level. Built from [`CompressionConfig`] via
/// [`Codec::from_config`], which also performs range validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Zstd { level: i32 },
    Gzip { level: u32 },
    None,
}

impl Codec {
    /// Resolve a [`CompressionConfig`] into a concrete codec, validating
    /// that the level is in range for the chosen format. Returns a
    /// field-named error on out-of-range levels — surfaces at startup,
    /// not at first write.
    pub fn from_config(cfg: &CompressionConfig) -> Result<Self> {
        match cfg.format {
            CodecFormat::Zstd => {
                let level = cfg.level.unwrap_or(3);
                if !(1..=22).contains(&level) {
                    return Err(anyhow!(
                        "compression.level {level} is out of range for zstd (1..=22)"
                    ));
                }
                Ok(Codec::Zstd { level })
            }
            CodecFormat::Gzip => {
                let level = cfg.level.unwrap_or(6);
                if !(0..=9).contains(&level) {
                    return Err(anyhow!(
                        "compression.level {level} is out of range for gzip (0..=9)"
                    ));
                }
                Ok(Codec::Gzip {
                    level: level as u32,
                })
            }
            CodecFormat::None => {
                if let Some(l) = cfg.level {
                    return Err(anyhow!(
                        "compression.level {l} is not applicable when compression.format = none"
                    ));
                }
                Ok(Codec::None)
            }
        }
    }

    /// File extension without the leading dot. `""` for the plaintext codec.
    pub fn extension(&self) -> &'static str {
        match self {
            Codec::Zstd { .. } => "zst",
            Codec::Gzip { .. } => "gz",
            Codec::None => "",
        }
    }

    /// Wire identifier used for HTTP `Content-Encoding` and S3
    /// `content_encoding`. `None` for plaintext — caller must skip the
    /// header rather than send `Content-Encoding: identity`.
    pub fn content_encoding(&self) -> Option<&'static str> {
        match self {
            Codec::Zstd { .. } => Some("zstd"),
            Codec::Gzip { .. } => Some("gzip"),
            Codec::None => None,
        }
    }

    /// Wrap a writer in the configured encoder.
    pub fn encoder<W: Write>(&self, w: W) -> Result<CodecEncoder<W>> {
        match *self {
            Codec::Zstd { level } => Ok(CodecEncoder::Zstd(
                zstd::Encoder::new(w, level).context("create zstd encoder")?,
            )),
            Codec::Gzip { level } => Ok(CodecEncoder::Gzip(GzEncoder::new(
                w,
                flate2::Compression::new(level),
            ))),
            Codec::None => Ok(CodecEncoder::None(w)),
        }
    }
}

/// Generic encoder wrapper. Generic over `W` so a sink can use
/// `CodecEncoder<File>` (file sink) or `CodecEncoder<Vec<u8>>` (http/s3
/// sinks) without dyn dispatch.
pub enum CodecEncoder<W: Write> {
    Zstd(zstd::Encoder<'static, W>),
    Gzip(GzEncoder<W>),
    None(W),
}

impl<W: Write> Write for CodecEncoder<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            CodecEncoder::Zstd(e) => e.write(buf),
            CodecEncoder::Gzip(e) => e.write(buf),
            CodecEncoder::None(w) => w.write(buf),
        }
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            CodecEncoder::Zstd(e) => e.write_all(buf),
            CodecEncoder::Gzip(e) => e.write_all(buf),
            CodecEncoder::None(w) => w.write_all(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            CodecEncoder::Zstd(e) => e.flush(),
            CodecEncoder::Gzip(e) => e.flush(),
            CodecEncoder::None(w) => w.flush(),
        }
    }
}

impl<W: Write> CodecEncoder<W> {
    /// Finalize the encoder frame and return the underlying writer.
    pub fn finish(self) -> Result<W> {
        match self {
            CodecEncoder::Zstd(e) => e.finish().context("finalize zstd encoder"),
            CodecEncoder::Gzip(e) => e.finish().context("finalize gzip encoder"),
            CodecEncoder::None(w) => Ok(w),
        }
    }
}

impl CodecEncoder<Vec<u8>> {
    /// Number of bytes already written to the inner buffer. Used by sinks
    /// that roll over batches based on compressed size (s3 with
    /// `batch_max_mb`, http with `batch_max_bytes`).
    pub fn buffered_len(&self) -> usize {
        match self {
            CodecEncoder::Zstd(e) => e.get_ref().len(),
            CodecEncoder::Gzip(e) => e.get_ref().len(),
            CodecEncoder::None(v) => v.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn roundtrip(codec: Codec, payload: &[u8]) -> Vec<u8> {
        let mut enc = codec.encoder(Vec::new()).unwrap();
        enc.write_all(payload).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn zstd_roundtrip() {
        let codec = Codec::Zstd { level: 3 };
        let body = roundtrip(codec, b"hello zstd world\n");
        let decoded = zstd::stream::decode_all(body.as_slice()).unwrap();
        assert_eq!(decoded, b"hello zstd world\n");
    }

    #[test]
    fn gzip_roundtrip() {
        let codec = Codec::Gzip { level: 6 };
        let body = roundtrip(codec, b"hello gzip world\n");
        let mut decoder = flate2::read::GzDecoder::new(body.as_slice());
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello gzip world\n");
    }

    #[test]
    fn none_is_identity() {
        let codec = Codec::None;
        let body = roundtrip(codec, b"plaintext payload\n");
        assert_eq!(body, b"plaintext payload\n");
    }

    #[test]
    fn extensions_match_format() {
        assert_eq!(Codec::Zstd { level: 3 }.extension(), "zst");
        assert_eq!(Codec::Gzip { level: 6 }.extension(), "gz");
        assert_eq!(Codec::None.extension(), "");
    }

    #[test]
    fn content_encoding_matches_format() {
        assert_eq!(Codec::Zstd { level: 3 }.content_encoding(), Some("zstd"));
        assert_eq!(Codec::Gzip { level: 6 }.content_encoding(), Some("gzip"));
        assert_eq!(Codec::None.content_encoding(), None);
    }

    #[test]
    fn from_config_rejects_zstd_out_of_range() {
        let cfg = CompressionConfig {
            format: CodecFormat::Zstd,
            level: Some(99),
        };
        let err = Codec::from_config(&cfg).unwrap_err();
        assert!(format!("{err}").contains("zstd"));
    }

    #[test]
    fn from_config_rejects_gzip_out_of_range() {
        let cfg = CompressionConfig {
            format: CodecFormat::Gzip,
            level: Some(11),
        };
        let err = Codec::from_config(&cfg).unwrap_err();
        assert!(format!("{err}").contains("gzip"));
    }

    #[test]
    fn from_config_rejects_level_with_none_format() {
        let cfg = CompressionConfig {
            format: CodecFormat::None,
            level: Some(3),
        };
        let err = Codec::from_config(&cfg).unwrap_err();
        assert!(format!("{err}").contains("none"));
    }

    #[test]
    fn from_config_defaults() {
        let zstd = Codec::from_config(&CompressionConfig {
            format: CodecFormat::Zstd,
            level: None,
        })
        .unwrap();
        assert!(matches!(zstd, Codec::Zstd { level: 3 }));
        let gzip = Codec::from_config(&CompressionConfig {
            format: CodecFormat::Gzip,
            level: None,
        })
        .unwrap();
        assert!(matches!(gzip, Codec::Gzip { level: 6 }));
    }

    #[test]
    fn buffered_len_tracks_writes() {
        let codec = Codec::None;
        let mut enc: CodecEncoder<Vec<u8>> = codec.encoder(Vec::new()).unwrap();
        enc.write_all(b"abcdef").unwrap();
        assert_eq!(enc.buffered_len(), 6);
    }
}
