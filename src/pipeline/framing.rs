//! Output framing layered above the codec encoder.
//!
//! All sinks receive matched lines as raw bytes terminated with `\n`. Two
//! framings are supported:
//!
//! - [`OutputFormat::JsonLines`] (default) — pass-through; the on-disk /
//!   on-the-wire layout is NDJSON.
//! - [`OutputFormat::JsonArray`] — wrap items in a single JSON array
//!   (`[obj1,obj2,...]`) so the payload is one valid JSON document.
//!   Input lines are assumed to already be valid JSON values — we never
//!   parse them; we only inject the inter-item delimiters.
//!
//! The wrapper sits *above* `CodecEncoder<W>` so compression is unaware of
//! the framing choice. Sinks construct
//! `FramedEncoder<CodecEncoder<W>>` and call [`FramedEncoder::write_item`]
//! once per matched line.

use serde::{Deserialize, Serialize};
use std::io::{self, Write};

/// User-facing framing config block. Lives on each sink config that emits
/// matched lines (`file`, `http`, `s3`).
///
/// In YAML this serializes as a tagged union:
///
/// ```yaml
/// format:
///   kind: json_lines              # default; equivalent to omitting `format:`
/// format:
///   kind: json_array
///   pretty: false                 # default
/// ```
///
/// When the format is `json_array`, the on-disk extension and (for the HTTP
/// sink) the request `Content-Type` change accordingly — see the HTTP sink
/// for the latter. For the file and s3 sinks the file extension is still
/// driven by the codec; users opting into `json_array` can override
/// `path_template` / `key_template` if they want `.json` instead of
/// `.ndjson` in the literal portion.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputFormat {
    #[default]
    JsonLines,
    JsonArray {
        #[serde(default)]
        pretty: bool,
    },
}

impl OutputFormat {
    /// `true` when the framing wraps items in a JSON array.
    pub fn is_json_array(&self) -> bool {
        matches!(self, OutputFormat::JsonArray { .. })
    }
}

/// Writer wrapper that inserts JSON-array framing (`[`, `,`, `]`) around
/// items when configured. Pass-through in `JsonLines` mode.
///
/// Generic over the underlying writer so a sink can use
/// `FramedEncoder<CodecEncoder<File>>` (file sink) or
/// `FramedEncoder<CodecEncoder<Vec<u8>>>` (http/s3 sinks) without dyn dispatch.
pub struct FramedEncoder<W: Write> {
    inner: W,
    format: OutputFormat,
    item_count: usize,
}

impl<W: Write> FramedEncoder<W> {
    pub fn new(inner: W, format: OutputFormat) -> Self {
        Self {
            inner,
            format,
            item_count: 0,
        }
    }

    /// Write one matched item. The trailing `\n` that the orchestrator adds
    /// to every line is stripped in `JsonArray` mode and preserved in
    /// `JsonLines` mode.
    pub fn write_item(&mut self, item: &[u8]) -> io::Result<()> {
        match self.format {
            OutputFormat::JsonLines => {
                self.inner.write_all(item)?;
            }
            OutputFormat::JsonArray { pretty } => {
                let sep: &[u8] = match (self.item_count, pretty) {
                    (0, false) => b"[",
                    (0, true) => b"[\n",
                    (_, false) => b",",
                    (_, true) => b",\n",
                };
                self.inner.write_all(sep)?;
                let payload = item.strip_suffix(b"\n").unwrap_or(item);
                self.inner.write_all(payload)?;
            }
        }
        self.item_count += 1;
        Ok(())
    }

    /// Close the array (when applicable) and return the underlying writer.
    /// An empty `JsonArray` batch writes nothing — callers that treat an
    /// empty body as "skip" (s3 sink) get the same behavior they had under
    /// `JsonLines`.
    pub fn finish(mut self) -> io::Result<W> {
        if let OutputFormat::JsonArray { pretty } = self.format {
            if self.item_count > 0 {
                let tail: &[u8] = if pretty { b"\n]" } else { b"]" };
                self.inner.write_all(tail)?;
            }
        }
        Ok(self.inner)
    }

    /// Borrow the underlying writer. Used by sinks that batch by
    /// compressed size and need to call `buffered_len()` on the codec
    /// encoder.
    pub fn inner_ref(&self) -> &W {
        &self.inner
    }

    pub fn item_count(&self) -> usize {
        self.item_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(format: OutputFormat, items: &[&[u8]]) -> Vec<u8> {
        let mut enc = FramedEncoder::new(Vec::new(), format);
        for it in items {
            enc.write_item(it).unwrap();
        }
        enc.finish().unwrap()
    }

    #[test]
    fn json_lines_is_byte_exact_passthrough() {
        let out = encode(
            OutputFormat::JsonLines,
            &[b"{\"a\":1}\n", b"{\"b\":2}\n", b"{\"c\":3}\n"],
        );
        assert_eq!(out, b"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");
    }

    #[test]
    fn json_lines_zero_items_is_empty() {
        let out = encode(OutputFormat::JsonLines, &[]);
        assert!(out.is_empty());
    }

    #[test]
    fn json_array_compact_three_items() {
        let out = encode(
            OutputFormat::JsonArray { pretty: false },
            &[b"{\"a\":1}\n", b"{\"b\":2}\n", b"{\"c\":3}\n"],
        );
        assert_eq!(out, b"[{\"a\":1},{\"b\":2},{\"c\":3}]");
    }

    #[test]
    fn json_array_compact_single_item() {
        let out = encode(
            OutputFormat::JsonArray { pretty: false },
            &[b"{\"only\":true}\n"],
        );
        assert_eq!(out, b"[{\"only\":true}]");
    }

    #[test]
    fn json_array_compact_zero_items_is_empty() {
        let out = encode(OutputFormat::JsonArray { pretty: false }, &[]);
        assert!(out.is_empty(), "got {:?}", out);
    }

    #[test]
    fn json_array_pretty_three_items() {
        let out = encode(
            OutputFormat::JsonArray { pretty: true },
            &[b"{\"a\":1}\n", b"{\"b\":2}\n", b"{\"c\":3}\n"],
        );
        assert_eq!(out, b"[\n{\"a\":1},\n{\"b\":2},\n{\"c\":3}\n]" as &[u8]);
    }

    #[test]
    fn json_array_pretty_zero_items_is_empty() {
        let out = encode(OutputFormat::JsonArray { pretty: true }, &[]);
        assert!(out.is_empty());
    }

    #[test]
    fn json_array_handles_items_without_trailing_newline() {
        let out = encode(
            OutputFormat::JsonArray { pretty: false },
            &[b"{\"a\":1}", b"{\"b\":2}"],
        );
        assert_eq!(out, b"[{\"a\":1},{\"b\":2}]");
    }

    #[test]
    fn item_count_tracks_writes() {
        let mut enc = FramedEncoder::new(Vec::new(), OutputFormat::JsonArray { pretty: false });
        assert_eq!(enc.item_count(), 0);
        enc.write_item(b"{\"a\":1}\n").unwrap();
        enc.write_item(b"{\"b\":2}\n").unwrap();
        assert_eq!(enc.item_count(), 2);
    }

    #[test]
    fn format_defaults_to_json_lines() {
        let f = OutputFormat::default();
        assert!(matches!(f, OutputFormat::JsonLines));
        assert!(!f.is_json_array());
    }

    #[test]
    fn json_array_compact_deserializes() {
        let yaml = "kind: json_array\n";
        let f: OutputFormat = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(f, OutputFormat::JsonArray { pretty: false });
    }

    #[test]
    fn json_array_pretty_deserializes() {
        let yaml = "kind: json_array\npretty: true\n";
        let f: OutputFormat = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(f, OutputFormat::JsonArray { pretty: true });
    }

    #[test]
    fn json_lines_deserializes() {
        let yaml = "kind: json_lines\n";
        let f: OutputFormat = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(f, OutputFormat::JsonLines);
    }

    #[test]
    fn unknown_field_rejected() {
        let yaml = "kind: json_array\nspaces: 4\n";
        let err = serde_yaml::from_str::<OutputFormat>(yaml).unwrap_err();
        assert!(format!("{err}").contains("spaces"));
    }
}
