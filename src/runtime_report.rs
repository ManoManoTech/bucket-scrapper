//! Effective performance-parameter report.
//!
//! Built once after CLI/YAML/inference resolution and emitted at startup
//! (so operators see what's actually running) and again as part of the
//! end-of-run completion log (so the post-mortem has the same numbers).
//!
//! Every reported value carries a [`Source`] explaining *where* the value
//! came from — user-supplied, static literal, function-computed default,
//! or inferred from another value (and whether that basis was itself
//! user-supplied or default). The goal is to give a future debugger one
//! place to check "is this the number I think I'm running with, and why
//! is it that number?".
//!
//! The inference helpers in this module are the **single source of
//! truth** for the inference rules. The CLI fill-in code in `main.rs`
//! and the report builder both call into the same helpers, so the
//! report can't drift from what's actually running.

use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::info;

// ---------------------------------------------------------------------------
// Source provenance
// ---------------------------------------------------------------------------

/// Where a reported value came from.
///
/// Variants are ordered roughly from "most explicit" to "most synthetic":
/// the `Display` impl renders them as a short tag fit for log lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// User provided the value (CLI flag or YAML config).
    UserDefined,
    /// Literal value baked into the source code — no inference, no
    /// function call. Changing it requires editing Rust.
    StaticDefault,
    /// Function-computed default that doesn't depend on the environment
    /// or other parameters (e.g. zstd's default level 3 derived from
    /// `format = zstd`). Distinct from `StaticDefault` because the rule
    /// could in principle vary by other settings.
    Default,
    /// Computed from another value via an inference rule; the basis was
    /// itself a default (or another inferred-from-default value). Stable
    /// across runs of the same code on the same machine.
    InferredFromDefault { rule: &'static str, basis: String },
    /// Computed from another value via an inference rule; the basis was
    /// `UserDefined`. Means "you set X, we computed Y from it" — touch X
    /// and Y moves.
    InferredFromSetting { rule: &'static str, basis: String },
}

impl Source {
    /// `true` when the value did not come from the user. Useful for
    /// quickly classifying a row of the report.
    pub fn is_default_like(&self) -> bool {
        !matches!(self, Source::UserDefined)
    }

    /// Tag used as the short label in the text rendering.
    pub fn tag(&self) -> &'static str {
        match self {
            Source::UserDefined => "user-defined",
            Source::StaticDefault => "static-default",
            Source::Default => "default",
            Source::InferredFromDefault { .. } => "inferred-from-default",
            Source::InferredFromSetting { .. } => "inferred-from-setting",
        }
    }
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::UserDefined | Source::StaticDefault | Source::Default => {
                f.write_str(self.tag())
            }
            Source::InferredFromDefault { rule, basis }
            | Source::InferredFromSetting { rule, basis } => {
                write!(f, "{}: {rule}, basis: {basis}", self.tag())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Report rows
// ---------------------------------------------------------------------------

/// One row in the effective-parameter report.
///
/// `name` follows the dotted stage-prefix scheme:
/// `source.s3.*`, `source.decompress.*`, `filter.*`, `pipeline.*`,
/// `sink.compress.*`, `sink.framing.*`, `sink.s3.*`, `sink.http.*`,
/// `sink.file.*`, plus the meta row `sink.kind`.
#[derive(Debug, Clone)]
pub struct ReportEntry {
    pub name: &'static str,
    pub value: String,
    pub source: Source,
}

impl ReportEntry {
    pub fn user_defined(name: &'static str, value: impl ToString) -> Self {
        Self {
            name,
            value: value.to_string(),
            source: Source::UserDefined,
        }
    }

    pub fn static_default(name: &'static str, value: impl ToString) -> Self {
        Self {
            name,
            value: value.to_string(),
            source: Source::StaticDefault,
        }
    }

    pub fn default_(name: &'static str, value: impl ToString) -> Self {
        Self {
            name,
            value: value.to_string(),
            source: Source::Default,
        }
    }

    pub fn inferred_from_default(
        name: &'static str,
        value: impl ToString,
        rule: &'static str,
        basis: impl ToString,
    ) -> Self {
        Self {
            name,
            value: value.to_string(),
            source: Source::InferredFromDefault {
                rule,
                basis: basis.to_string(),
            },
        }
    }

    pub fn inferred_from_setting(
        name: &'static str,
        value: impl ToString,
        rule: &'static str,
        basis: impl ToString,
    ) -> Self {
        Self {
            name,
            value: value.to_string(),
            source: Source::InferredFromSetting {
                rule,
                basis: basis.to_string(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Param<T>: typed value + source, the thing main.rs builds for each setting
// ---------------------------------------------------------------------------

/// Resolved performance parameter carrying both its runtime value and its
/// provenance. main.rs constructs one per setting after CLI/YAML resolve,
/// uses `.value` to populate the actual config structs, and the report
/// builder converts the `Param<T>` into a [`ReportEntry`] for emission.
#[derive(Debug, Clone)]
pub struct Param<T> {
    pub value: T,
    pub source: Source,
}

impl<T> Param<T> {
    pub fn user_defined(value: T) -> Self {
        Self {
            value,
            source: Source::UserDefined,
        }
    }

    pub fn static_default(value: T) -> Self {
        Self {
            value,
            source: Source::StaticDefault,
        }
    }

    pub fn default_(value: T) -> Self {
        Self {
            value,
            source: Source::Default,
        }
    }

    pub fn inferred_from_default(value: T, rule: &'static str, basis: String) -> Self {
        Self {
            value,
            source: Source::InferredFromDefault { rule, basis },
        }
    }

    pub fn inferred_from_setting(value: T, rule: &'static str, basis: String) -> Self {
        Self {
            value,
            source: Source::InferredFromSetting { rule, basis },
        }
    }
}

impl<T: std::fmt::Display> Param<T> {
    /// Convert to the report-row form. The `name` follows the
    /// dotted stage-prefix convention (`source.s3.*` etc).
    pub fn into_entry(self, name: &'static str) -> ReportEntry {
        ReportEntry {
            name,
            value: self.value.to_string(),
            source: self.source,
        }
    }
}

/// Helper for the common "CLI flag with a literal fallback" shape.
///
/// `cli` is the user's `Option<T>` flag; if `Some`, that's user-defined,
/// otherwise we fall back to `static_default` with `StaticDefault`
/// provenance. Saves a couple of `match` arms at every call site.
pub fn cli_or_static<T: Clone>(cli: Option<T>, static_default: T) -> Param<T> {
    match cli {
        Some(v) => Param::user_defined(v),
        None => Param::static_default(static_default),
    }
}

/// Helper for "CLI flag with an inference rule fallback". Carries the
/// inferred rule + basis into the resulting `Source::InferredFromDefault`.
pub fn cli_or_inferred<T: Clone>(cli: Option<T>, inferred: Inferred<T>) -> Param<T> {
    match cli {
        Some(v) => Param::user_defined(v),
        None => Param::inferred_from_default(inferred.value, inferred.rule, inferred.basis),
    }
}

// ---------------------------------------------------------------------------
// Inference helpers — single source of truth for the rules
// ---------------------------------------------------------------------------

/// Result of an inference: the value the rule produced plus the inputs
/// it used. Callers that just want the value take `.value`; the report
/// builder takes everything to construct a [`Source::InferredFromDefault`].
#[derive(Debug, Clone)]
pub struct Inferred<T> {
    pub value: T,
    pub rule: &'static str,
    pub basis: String,
}

/// Cached `available_parallelism()`. We log it once in the report so
/// every inference can refer to the same value, and we save a syscall
/// on repeated lookups.
fn available_parallelism() -> usize {
    static CACHE: AtomicUsize = AtomicUsize::new(0);
    let cached = CACHE.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    let n = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    CACHE.store(n, Ordering::Relaxed);
    n
}

/// `source.s3.max_concurrent_downloads = max(8, available_parallelism)`.
///
/// Scales linearly with instance size. Floor of 8 keeps small boxes
/// usable; no upper cap so larger instances scale naturally with their
/// (typically larger) network bandwidth allocation. Operators who hit
/// an SDK / DNS / FD ceiling on monster instances can override.
pub fn infer_max_concurrent_downloads() -> Inferred<usize> {
    let cpus = available_parallelism();
    let value = cpus.max(8);
    Inferred {
        value,
        rule: "max(8, available_parallelism)",
        basis: format!("available_parallelism = {cpus}"),
    }
}

/// `filter.tasks = max(1, available_parallelism / 2)`.
pub fn infer_filter_tasks() -> Inferred<usize> {
    let cpus = available_parallelism();
    let value = (cpus / 2).max(1);
    Inferred {
        value,
        rule: "max(1, available_parallelism / 2)",
        basis: format!("available_parallelism = {cpus}"),
    }
}

/// `sink.http.compressor_tasks = max(1, available_parallelism / 8)`.
pub fn infer_http_compressor_tasks() -> Inferred<usize> {
    let cpus = available_parallelism();
    let value = (cpus / 8).max(1);
    Inferred {
        value,
        rule: "max(1, available_parallelism / 8)",
        basis: format!("available_parallelism = {cpus}"),
    }
}

/// `sink.http.upload_tasks = 4 * compressor_tasks`. The Source of the
/// returned value is `InferredFromSetting` iff `compressor_was_user_set`,
/// otherwise `InferredFromDefault` — the report builder uses both.
pub fn infer_http_upload_tasks(compressor_tasks: usize) -> Inferred<usize> {
    Inferred {
        value: 4 * compressor_tasks,
        rule: "4 * compressor_tasks",
        basis: format!("compressor_tasks = {compressor_tasks}"),
    }
}

/// Codec compression level default derived from format. zstd → 3, gzip → 6,
/// none → none (the caller renders as `(unset)`).
pub fn default_compression_level(format: &str) -> Option<i32> {
    match format {
        "zstd" => Some(3),
        "gzip" => Some(6),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Report assembly
// ---------------------------------------------------------------------------

use crate::config::output::OutputConfig;
use crate::pipeline::codec::{CodecFormat, CompressionConfig};
use crate::pipeline::framing::OutputFormat;

/// Pipeline-level resolved parameters that main.rs already built via the
/// `cli_or_*` helpers. Bundled into one struct so the report builder
/// signature stays manageable.
pub struct PipelineParams {
    pub max_parallel: Param<usize>,
    pub max_retries: Param<u32>,
    pub retry_delay_s: Param<u64>,
    pub progress_interval_s: Param<f64>,
    pub filter_tasks: Param<usize>,
    pub line_buffer_size: Param<usize>,
    // ── Parallel chunked download (None ⇒ disabled) ──
    pub chunk_size: Option<usize>,
    pub file_slots: usize,
    pub file_slots_user_set: bool,
    pub file_slots_clamped: bool,
    pub max_input_buffer_bytes: usize,
    pub decode_input_buffer_bytes: usize,
}

/// Build the effective-parameter report.
///
/// Stage prefixes follow the documented scheme:
/// `source.s3.*`, `source.decompress.*`, `filter.*`, `pipeline.*`,
/// `sink.compress.*`, `sink.framing.*`, and one of
/// `sink.s3.*` / `sink.http.*` / `sink.file.*` depending on
/// `sink.kind`. (`void` sinks emit no sink-specific rows.)
pub fn build_report(pipeline: &PipelineParams, resolved_output: &OutputConfig) -> Vec<ReportEntry> {
    // Pre-size for the typical case: ~7 pipeline rows + 1 sink.kind + 4
    // codec/framing + 5-12 sink rows. Avoids the immediate-resize clippy
    // lint and lets the typical run skip a couple of reallocations.
    let mut out: Vec<ReportEntry> = Vec::with_capacity(24);

    // ── source.s3 ──────────────────────────────────────────────────────────
    out.push(
        pipeline
            .max_parallel
            .clone()
            .into_entry("source.s3.max_concurrent_downloads"),
    );
    out.push(
        pipeline
            .max_retries
            .clone()
            .into_entry("source.s3.max_retries"),
    );
    out.push(
        pipeline
            .retry_delay_s
            .clone()
            .into_entry("source.s3.initial_retry_delay_s"),
    );

    // ── source.decompress ──────────────────────────────────────────────────
    // Chunk channel between download and decompress is a hardcoded constant
    // in `download_and_stream` (capacity 4 ≈ 256 KB resident per active
    // download). Surface it so anyone debugging download/decompress
    // backpressure sees the bound.
    out.push(ReportEntry::static_default(
        "source.decompress.chunk_channel_capacity",
        4,
    ));

    // ── source.download (parallel chunked download) + buffer budget ─────────
    match pipeline.chunk_size {
        None => out.push(ReportEntry::static_default(
            "source.download.chunk_size_mb",
            "disabled",
        )),
        Some(cs) => {
            let cs_mb = cs / 1_000_000;
            out.push(ReportEntry::user_defined(
                "source.download.chunk_size_mb",
                cs_mb,
            ));
            // file_slots: user-set, clamped, or inferred from max_parallel.
            out.push(if pipeline.file_slots_clamped {
                ReportEntry::inferred_from_setting(
                    "source.download.file_slots",
                    pipeline.file_slots,
                    "min(requested, max_input_buffer / chunk_size)",
                    "clamped so pool ≥ file_slots × chunk_size",
                )
            } else if pipeline.file_slots_user_set {
                ReportEntry::user_defined("source.download.file_slots", pipeline.file_slots)
            } else {
                ReportEntry::inferred_from_default(
                    "source.download.file_slots",
                    pipeline.file_slots,
                    "= max_parallel",
                    format!("max_parallel = {}", pipeline.max_parallel.value),
                )
            });
            // B1 reassembly pool.
            out.push(ReportEntry::user_defined(
                "buffer.b1_input_pool_mb",
                pipeline.max_input_buffer_bytes / 1_000_000,
            ));
            out.push(ReportEntry::inferred_from_setting(
                "buffer.b1_pool_slots",
                pipeline.max_input_buffer_bytes / cs.max(1),
                "max_input_buffer / chunk_size",
                format!(
                    "pool {} MB / chunk {cs_mb} MB",
                    pipeline.max_input_buffer_bytes / 1_000_000
                ),
            ));
            // B2 decode-input.
            out.push(ReportEntry::user_defined(
                "buffer.b2_decode_input_mb_per_file",
                pipeline.decode_input_buffer_bytes / 1_000_000,
            ));
            out.push(ReportEntry::inferred_from_setting(
                "buffer.b2_decode_input_mb_total",
                (pipeline.decode_input_buffer_bytes / 1_000_000) * pipeline.file_slots,
                "decode_input × file_slots",
                format!("file_slots = {}", pipeline.file_slots),
            ));
        }
    }

    // ── filter ─────────────────────────────────────────────────────────────
    out.push(pipeline.filter_tasks.clone().into_entry("filter.tasks"));
    out.push(
        pipeline
            .line_buffer_size
            .clone()
            .into_entry("filter.line_buffer_size"),
    );

    // ── pipeline ───────────────────────────────────────────────────────────
    out.push(
        pipeline
            .progress_interval_s
            .clone()
            .into_entry("pipeline.progress_interval_s"),
    );

    // ── sink.kind ─────────────────────────────────────────────────────────
    // Always user-defined: the user picked the sink type via `--output` or
    // the YAML `outputs[].type` field.
    out.push(ReportEntry::user_defined(
        "sink.kind",
        resolved_output.type_name(),
    ));

    // ── sink.compress + sink.framing (per-sink configs) ───────────────────
    if let Some((compression, format)) = sink_compression_and_format(resolved_output) {
        push_compress_entries(&mut out, compression);
        push_framing_entries(&mut out, format);
    }

    // ── sink.{s3|http|file} ────────────────────────────────────────────────
    match resolved_output {
        OutputConfig::S3(c) => push_sink_s3_entries(&mut out, c),
        OutputConfig::Http(c) => push_sink_http_entries(&mut out, c),
        OutputConfig::File(c) => push_sink_file_entries(&mut out, c),
        OutputConfig::Void => {}
    }

    out
}

/// Extract the codec + framing configs for any sink that has them.
/// Returns `None` for the void sink.
fn sink_compression_and_format(
    output: &OutputConfig,
) -> Option<(&CompressionConfig, &OutputFormat)> {
    match output {
        OutputConfig::File(c) => Some((&c.compression, &c.format)),
        OutputConfig::Http(c) => Some((&c.compression, &c.format)),
        OutputConfig::S3(c) => Some((&c.compression, &c.format)),
        OutputConfig::Void => None,
    }
}

fn push_compress_entries(out: &mut Vec<ReportEntry>, compression: &CompressionConfig) {
    let default_cfg = CompressionConfig::default();
    let format_source = if compression.format == default_cfg.format {
        Source::Default
    } else {
        Source::UserDefined
    };
    let format_str = format!("{:?}", compression.format).to_lowercase();
    out.push(ReportEntry {
        name: "sink.compress.format",
        value: format_str.clone(),
        source: format_source.clone(),
    });

    // Compression level: `default` (function-computed from format) when
    // user left it unset; otherwise user-defined.
    let level_source = match (compression.level, &compression.format) {
        (None, _) => Source::Default,
        (Some(_), _) => Source::UserDefined,
    };
    let level_value = match compression.level {
        Some(l) => l.to_string(),
        None => match compression.format {
            CodecFormat::Zstd => "3".to_string(),
            CodecFormat::Gzip => "6".to_string(),
            CodecFormat::None => "(none)".to_string(),
        },
    };
    out.push(ReportEntry {
        name: "sink.compress.level",
        value: level_value,
        source: level_source,
    });
}

fn push_framing_entries(out: &mut Vec<ReportEntry>, format: &OutputFormat) {
    let (kind, pretty, kind_source) = match format {
        OutputFormat::JsonLines => ("json_lines", false, Source::Default),
        OutputFormat::JsonArray { pretty } => ("json_array", *pretty, Source::UserDefined),
    };
    out.push(ReportEntry {
        name: "sink.framing.kind",
        value: kind.into(),
        source: kind_source,
    });
    // `pretty` only meaningful for json_array; for json_lines it's
    // hard-coded to false so we surface it as static-default to keep the
    // row layout symmetric.
    out.push(ReportEntry {
        name: "sink.framing.pretty",
        value: pretty.to_string(),
        source: if matches!(format, OutputFormat::JsonArray { .. }) && pretty {
            Source::UserDefined
        } else {
            Source::Default
        },
    });
}

fn push_sink_s3_entries(out: &mut Vec<ReportEntry>, cfg: &crate::config::output::S3OutputConfig) {
    out.push(ReportEntry {
        name: "sink.s3.batch_max_mb",
        value: cfg
            .batch_max_mb
            .map(|v| v.to_string())
            .unwrap_or_else(|| "(unset)".into()),
        source: if cfg.batch_max_mb.is_some() {
            Source::UserDefined
        } else {
            Source::Default
        },
    });
    out.push(ReportEntry {
        name: "sink.s3.multipart_threshold_mb",
        value: cfg.multipart_threshold_mb.to_string(),
        // Default for this field is 5 in YAML and clap; the Default impl
        // for `S3OutputConfig` is the same.
        source: if cfg.multipart_threshold_mb == 5 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.s3.multipart_part_mb",
        value: cfg.multipart_part_mb.to_string(),
        source: if cfg.multipart_part_mb == 5 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.s3.multipart_concurrency",
        value: cfg
            .multipart_concurrency
            .map(|n| n.to_string())
            .unwrap_or_else(|| "Auto".into()),
        source: if cfg.multipart_concurrency.is_some() {
            Source::UserDefined
        } else {
            Source::Default
        },
    });
    // Streaming channel between codec encoder and TM PartStream. Constant
    // in `s3_streaming::CHANNEL_CAPACITY`; surfaced because it directly
    // bounds per-prefix in-flight bytes.
    out.push(ReportEntry::static_default(
        "sink.s3.channel_capacity",
        crate::pipeline::s3_streaming::CHANNEL_CAPACITY,
    ));
}

fn push_sink_http_entries(
    out: &mut Vec<ReportEntry>,
    cfg: &crate::config::output::HttpOutputConfig,
) {
    // batch_max_mb has a function-computed default of 2.0.
    out.push(ReportEntry {
        name: "sink.http.batch_max_mb",
        value: cfg.batch_max_mb.to_string(),
        source: if (cfg.batch_max_mb - 2.0).abs() < f64::EPSILON {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });

    // compressor_tasks: if user-set, UserDefined; else inferred.
    let compressor_inferred = infer_http_compressor_tasks();
    let (compressor_value, compressor_was_user_set) = match cfg.compressor_tasks {
        Some(v) => (v, true),
        None => (compressor_inferred.value, false),
    };
    out.push(ReportEntry {
        name: "sink.http.compressor_tasks",
        value: compressor_value.to_string(),
        source: if compressor_was_user_set {
            Source::UserDefined
        } else {
            Source::InferredFromDefault {
                rule: compressor_inferred.rule,
                basis: compressor_inferred.basis.clone(),
            }
        },
    });

    // upload_tasks: rule "4 * compressor_tasks", but the source-kind
    // depends on whether compressor was user-set (then InferredFromSetting)
    // or default (then InferredFromDefault).
    let (upload_value, upload_source) = match cfg.upload_tasks {
        Some(v) => (v, Source::UserDefined),
        None => {
            let v = 4 * compressor_value;
            let basis = format!("compressor_tasks = {compressor_value}");
            let src = if compressor_was_user_set {
                Source::InferredFromSetting {
                    rule: "4 * compressor_tasks",
                    basis,
                }
            } else {
                Source::InferredFromDefault {
                    rule: "4 * compressor_tasks",
                    basis,
                }
            };
            (v, src)
        }
    };
    out.push(ReportEntry {
        name: "sink.http.upload_tasks",
        value: upload_value.to_string(),
        source: upload_source,
    });

    out.push(ReportEntry {
        name: "sink.http.upload_channel_size",
        value: cfg.upload_channel_size.to_string(),
        source: if cfg.upload_channel_size == 4 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.http.line_channel_size",
        value: cfg.line_channel_size.to_string(),
        source: if cfg.line_channel_size == 1000 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.http.max_retries",
        value: cfg.max_retries.to_string(),
        source: if cfg.max_retries == 3 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.http.timeout_secs",
        value: cfg.timeout_secs.to_string(),
        source: if cfg.timeout_secs == 30 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.http.max_upload_rate_mbps",
        value: cfg.max_upload_rate_mbps.to_string(),
        source: if cfg.max_upload_rate_mbps == 0.0 {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    // AIMD knobs — all static-default unless overridden.
    out.push(ReportEntry {
        name: "sink.http.aimd.decrease_factor",
        value: cfg.aimd.decrease_factor.to_string(),
        source: if (cfg.aimd.decrease_factor - 0.15).abs() < f64::EPSILON {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.http.aimd.increase_mbps",
        value: cfg.aimd.increase_mbps.to_string(),
        source: if (cfg.aimd.increase_mbps - 1.0).abs() < f64::EPSILON {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
    out.push(ReportEntry {
        name: "sink.http.aimd.max_submission_time_s",
        value: cfg.aimd.max_submission_time_s.to_string(),
        source: if (cfg.aimd.max_submission_time_s - 4.0).abs() < f64::EPSILON {
            Source::StaticDefault
        } else {
            Source::UserDefined
        },
    });
}

fn push_sink_file_entries(
    out: &mut Vec<ReportEntry>,
    cfg: &crate::config::output::FileOutputConfig,
) {
    out.push(ReportEntry {
        name: "sink.file.path_template",
        value: cfg.path_template.clone(),
        source: if cfg.path_template == "{prefix}.{ext}" {
            Source::Default
        } else {
            Source::UserDefined
        },
    });
}

// ---------------------------------------------------------------------------
// Rendering + emission
// ---------------------------------------------------------------------------

/// Render the report as a multi-line block fit for plain-text logs.
/// JSON-log consumers should use [`emit`] instead, which surfaces each
/// row as a structured field.
pub fn render_text(entries: &[ReportEntry]) -> String {
    let mut out = String::from("Runtime parameters (effective):\n");
    // Compute the column width once so values align across rows.
    let name_w = entries.iter().map(|e| e.name.len()).max().unwrap_or(0);
    let value_w = entries.iter().map(|e| e.value.len()).max().unwrap_or(0);
    for e in entries {
        use std::fmt::Write;
        let _ = writeln!(
            out,
            "  {:<name_w$} = {:<value_w$}  [{}]",
            e.name,
            e.value,
            e.source,
            name_w = name_w,
            value_w = value_w,
        );
    }
    // Trailing newline trimmed by the tracing formatter.
    out
}

/// Emit the report as one `tracing::info!` event. In text mode the
/// rendered block lands as the message; in JSON mode the same data is
/// duplicated into a `params` field so structured consumers can ingest
/// it without parsing the message string.
pub fn emit(label: &str, entries: &[ReportEntry]) {
    let text = render_text(entries);
    let json = serde_json::Value::Array(
        entries
            .iter()
            .map(|e| {
                let mut obj = serde_json::Map::new();
                obj.insert("name".into(), serde_json::Value::String(e.name.into()));
                obj.insert("value".into(), serde_json::Value::String(e.value.clone()));
                obj.insert(
                    "source".into(),
                    serde_json::Value::String(e.source.tag().into()),
                );
                if let Source::InferredFromDefault { rule, basis }
                | Source::InferredFromSetting { rule, basis } = &e.source
                {
                    obj.insert("rule".into(), serde_json::Value::String((*rule).into()));
                    obj.insert("basis".into(), serde_json::Value::String(basis.clone()));
                }
                serde_json::Value::Object(obj)
            })
            .collect(),
    );
    info!(label = label, params = %json, "{text}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_tags_match_documented_names() {
        assert_eq!(Source::UserDefined.tag(), "user-defined");
        assert_eq!(Source::StaticDefault.tag(), "static-default");
        assert_eq!(Source::Default.tag(), "default");
        assert_eq!(
            Source::InferredFromDefault {
                rule: "r",
                basis: "b".into()
            }
            .tag(),
            "inferred-from-default"
        );
        assert_eq!(
            Source::InferredFromSetting {
                rule: "r",
                basis: "b".into()
            }
            .tag(),
            "inferred-from-setting"
        );
    }

    #[test]
    fn source_display_includes_rule_and_basis_for_inferred() {
        let s = Source::InferredFromDefault {
            rule: "max(1, X / 2)",
            basis: "X = 8".into(),
        };
        let rendered = format!("{s}");
        assert!(rendered.contains("inferred-from-default"));
        assert!(rendered.contains("max(1, X / 2)"));
        assert!(rendered.contains("X = 8"));
    }

    #[test]
    fn infer_max_concurrent_downloads_respects_floor() {
        // Can't easily mock available_parallelism, but we can at least
        // assert the floor: result must be >= 8 regardless of CPU count.
        let r = infer_max_concurrent_downloads();
        assert!(r.value >= 8, "value {} below 8-CPU floor", r.value);
        assert!(r.rule.contains("max(8"));
        assert!(r.basis.contains("available_parallelism"));
    }

    #[test]
    fn infer_filter_tasks_respects_floor_of_1() {
        let r = infer_filter_tasks();
        assert!(r.value >= 1);
    }

    #[test]
    fn infer_http_upload_tasks_uses_compressor_as_basis() {
        let r = infer_http_upload_tasks(3);
        assert_eq!(r.value, 12);
        assert!(r.basis.contains("compressor_tasks = 3"));
    }

    #[test]
    fn default_compression_level_matches_codec_defaults() {
        assert_eq!(default_compression_level("zstd"), Some(3));
        assert_eq!(default_compression_level("gzip"), Some(6));
        assert_eq!(default_compression_level("none"), None);
    }

    #[test]
    fn cli_or_static_picks_cli_when_set() {
        let p: Param<usize> = cli_or_static(Some(7), 32);
        assert_eq!(p.value, 7);
        assert_eq!(p.source, Source::UserDefined);
    }

    #[test]
    fn cli_or_static_picks_default_when_cli_none() {
        let p: Param<usize> = cli_or_static(None, 32);
        assert_eq!(p.value, 32);
        assert_eq!(p.source, Source::StaticDefault);
    }

    #[test]
    fn cli_or_inferred_picks_cli_when_set() {
        let p: Param<usize> = cli_or_inferred(Some(7), infer_max_concurrent_downloads());
        assert_eq!(p.value, 7);
        assert_eq!(p.source, Source::UserDefined);
    }

    #[test]
    fn cli_or_inferred_picks_inferred_when_cli_none() {
        let inferred = infer_max_concurrent_downloads();
        let expected_value = inferred.value;
        let p: Param<usize> = cli_or_inferred(None, inferred);
        assert_eq!(p.value, expected_value);
        match p.source {
            Source::InferredFromDefault { rule, basis } => {
                assert!(rule.contains("max(8"));
                assert!(basis.contains("available_parallelism"));
            }
            other => panic!("expected InferredFromDefault, got {other:?}"),
        }
    }

    fn dummy_pipeline_params() -> PipelineParams {
        PipelineParams {
            max_parallel: Param::user_defined(64),
            max_retries: Param::static_default(10),
            retry_delay_s: Param::static_default(2),
            progress_interval_s: Param::static_default(1.0),
            filter_tasks: Param::inferred_from_default(
                16,
                "max(1, available_parallelism / 2)",
                "available_parallelism = 32".into(),
            ),
            line_buffer_size: Param::static_default(1000),
            chunk_size: None,
            file_slots: 64,
            file_slots_user_set: false,
            file_slots_clamped: false,
            max_input_buffer_bytes: 4096 * 1_000_000,
            decode_input_buffer_bytes: 128 * 1_000_000,
        }
    }

    #[test]
    fn build_report_includes_every_expected_stage_prefix_for_s3() {
        use crate::config::output::{OutputConfig, S3OutputConfig};
        use crate::pipeline::codec::CompressionConfig;

        let s3 = OutputConfig::S3(S3OutputConfig {
            bucket: "b".into(),
            region: None,
            endpoint_url: None,
            key_template: "k/{prefix}/{run_id}-{seq}.{ext}".into(),
            batch_max_mb: None,
            compression: CompressionConfig::default(),
            multipart_threshold_mb: 5,
            multipart_part_mb: 5,
            multipart_concurrency: None,
            format: OutputFormat::default(),
        });
        let entries = build_report(&dummy_pipeline_params(), &s3);

        let names: Vec<&str> = entries.iter().map(|e| e.name).collect();
        for expected in [
            "source.s3.max_concurrent_downloads",
            "source.s3.max_retries",
            "source.s3.initial_retry_delay_s",
            "source.decompress.chunk_channel_capacity",
            "filter.tasks",
            "filter.line_buffer_size",
            "pipeline.progress_interval_s",
            "sink.kind",
            "sink.compress.format",
            "sink.compress.level",
            "sink.framing.kind",
            "sink.framing.pretty",
            "sink.s3.batch_max_mb",
            "sink.s3.multipart_threshold_mb",
            "sink.s3.multipart_part_mb",
            "sink.s3.multipart_concurrency",
            "sink.s3.channel_capacity",
        ] {
            assert!(
                names.contains(&expected),
                "missing report row: {expected}. got: {names:?}"
            );
        }
    }

    #[test]
    fn build_report_marks_max_parallel_as_user_defined_when_param_says_so() {
        use crate::config::output::OutputConfig;
        let entries = build_report(&dummy_pipeline_params(), &OutputConfig::Void);
        let row = entries
            .iter()
            .find(|e| e.name == "source.s3.max_concurrent_downloads")
            .expect("row missing");
        assert_eq!(row.value, "64");
        assert_eq!(row.source, Source::UserDefined);
    }

    #[test]
    fn build_report_http_upload_tasks_is_inferred_from_setting_when_compressor_user_set() {
        use crate::config::output::{HttpAimdConfig, HttpOutputConfig, OutputConfig};
        use crate::pipeline::codec::CompressionConfig;

        let http = OutputConfig::Http(HttpOutputConfig {
            url: "http://x".into(),
            bearer_auth: None,
            timeout_secs: 30,
            batch_max_mb: 2.0,
            compressor_tasks: Some(7), // user-set
            upload_tasks: None,        // inferred from compressor
            upload_channel_size: 4,
            line_channel_size: 1000,
            compression: CompressionConfig::default(),
            max_retries: 3,
            max_upload_rate_mbps: 0.0,
            aimd: HttpAimdConfig::default(),
            format: OutputFormat::default(),
        });
        let entries = build_report(&dummy_pipeline_params(), &http);
        let upload = entries
            .iter()
            .find(|e| e.name == "sink.http.upload_tasks")
            .expect("upload_tasks row missing");
        assert_eq!(upload.value, "28"); // 4 * 7
        match &upload.source {
            Source::InferredFromSetting { rule, basis } => {
                assert!(rule.contains("4 * compressor_tasks"));
                assert!(basis.contains("compressor_tasks = 7"));
            }
            other => panic!("expected InferredFromSetting, got {other:?}"),
        }
    }

    #[test]
    fn build_report_http_upload_tasks_is_inferred_from_default_when_compressor_unset() {
        use crate::config::output::{HttpAimdConfig, HttpOutputConfig, OutputConfig};
        use crate::pipeline::codec::CompressionConfig;

        let http = OutputConfig::Http(HttpOutputConfig {
            url: "http://x".into(),
            bearer_auth: None,
            timeout_secs: 30,
            batch_max_mb: 2.0,
            compressor_tasks: None, // inferred from default
            upload_tasks: None,     // inferred from inferred default
            upload_channel_size: 4,
            line_channel_size: 1000,
            compression: CompressionConfig::default(),
            max_retries: 3,
            max_upload_rate_mbps: 0.0,
            aimd: HttpAimdConfig::default(),
            format: OutputFormat::default(),
        });
        let entries = build_report(&dummy_pipeline_params(), &http);
        let upload = entries
            .iter()
            .find(|e| e.name == "sink.http.upload_tasks")
            .expect("upload_tasks row missing");
        match &upload.source {
            Source::InferredFromDefault { rule, .. } => {
                assert!(rule.contains("4 * compressor_tasks"));
            }
            other => panic!("expected InferredFromDefault, got {other:?}"),
        }
    }

    #[test]
    fn render_text_aligns_columns_and_includes_each_row() {
        let entries = vec![
            ReportEntry::static_default("a.b", 32),
            ReportEntry::inferred_from_default("a.cc", 16, "X / 2", "X = 32"),
        ];
        let s = render_text(&entries);
        assert!(s.contains("a.b"));
        assert!(s.contains("a.cc"));
        assert!(s.contains("static-default"));
        assert!(s.contains("inferred-from-default: X / 2, basis: X = 32"));
    }
}
