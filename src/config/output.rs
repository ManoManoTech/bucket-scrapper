//! Tagged-enum output configuration plus `${ENV}` interpolation pass.
//!
//! Pipeline output is described by a list of [`OutputConfig`] entries under
//! the `outputs:` key in the YAML config (or built from CLI flags when the
//! config has no `outputs:` block — see [`crate::config::resolve`]).
//!
//! Today the list must contain exactly one entry. The schema and pipeline
//! plumbing are designed so a future fan-out implementation can drop the
//! single-entry restriction without breaking changes.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// One configured output.
///
/// Variants are tagged via `type:` in YAML, matching `snake_case` names —
/// `file`, `http`, `s3`, `void`.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputConfig {
    File(FileOutputConfig),
    Http(HttpOutputConfig),
    S3(S3OutputConfig),
    Void,
}

impl OutputConfig {
    /// Lower-snake-case label for diagnostics and logging.
    pub fn type_name(&self) -> &'static str {
        match self {
            OutputConfig::File(_) => "file",
            OutputConfig::Http(_) => "http",
            OutputConfig::S3(_) => "s3",
            OutputConfig::Void => "void",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FileOutputConfig {
    pub dir: String,
    #[serde(default)]
    pub compression_level: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct HttpOutputConfig {
    pub url: String,
    #[serde(default)]
    pub bearer_auth: Option<String>,
    #[serde(default = "default_http_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_http_batch_max_mb")]
    pub batch_max_mb: f64,
    #[serde(default)]
    pub compressor_tasks: Option<usize>,
    #[serde(default)]
    pub upload_tasks: Option<usize>,
    #[serde(default = "default_http_upload_channel_size")]
    pub upload_channel_size: usize,
    #[serde(default = "default_http_line_channel_size")]
    pub line_channel_size: usize,
    #[serde(default)]
    pub compression_level: Option<i32>,
    #[serde(default = "default_http_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_http_max_upload_rate_mbps")]
    pub max_upload_rate_mbps: f64,
    #[serde(default)]
    pub aimd: HttpAimdConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct HttpAimdConfig {
    #[serde(default = "default_aimd_decrease_factor")]
    pub decrease_factor: f64,
    #[serde(default = "default_aimd_increase_mbps")]
    pub increase_mbps: f64,
    /// Per-batch submission time threshold in seconds. `0` disables AIMD.
    #[serde(default = "default_aimd_max_submission_time_s")]
    pub max_submission_time_s: f64,
}

impl Default for HttpAimdConfig {
    fn default() -> Self {
        Self {
            decrease_factor: default_aimd_decrease_factor(),
            increase_mbps: default_aimd_increase_mbps(),
            max_submission_time_s: default_aimd_max_submission_time_s(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3OutputConfig {
    pub bucket: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default = "default_s3_key_template")]
    pub key_template: String,
    #[serde(default = "default_s3_batch_max_mb")]
    pub batch_max_mb: f64,
    #[serde(default)]
    pub compression_level: Option<i32>,
    #[serde(default = "default_s3_multipart_threshold_mb")]
    pub multipart_threshold_mb: u64,
    #[serde(default = "default_s3_multipart_part_mb")]
    pub multipart_part_mb: u64,
    #[serde(default)]
    pub upload_tasks: Option<usize>,
}

fn default_http_timeout_secs() -> u64 {
    30
}
fn default_http_batch_max_mb() -> f64 {
    2.0
}
fn default_http_upload_channel_size() -> usize {
    4
}
fn default_http_line_channel_size() -> usize {
    1000
}
fn default_http_max_retries() -> u32 {
    3
}
fn default_http_max_upload_rate_mbps() -> f64 {
    0.0
}
fn default_aimd_decrease_factor() -> f64 {
    0.15
}
fn default_aimd_increase_mbps() -> f64 {
    1.0
}
fn default_aimd_max_submission_time_s() -> f64 {
    4.0
}
fn default_s3_key_template() -> String {
    "results/{date}/{hour}/{prefix_hash}-{seq}.ndjson.zst".to_string()
}
fn default_s3_batch_max_mb() -> f64 {
    16.0
}
fn default_s3_multipart_threshold_mb() -> u64 {
    64
}
fn default_s3_multipart_part_mb() -> u64 {
    16
}

/// In-place expand `${VAR}` and `${VAR:-default}` placeholders inside every
/// string field of `cfg`.
///
/// Errors when a `${VAR}` (no default) references an unset environment
/// variable. The error message names the offending field path so users can
/// fix their config quickly.
pub fn expand_env(cfg: &mut OutputConfig) -> Result<()> {
    match cfg {
        OutputConfig::File(c) => {
            expand_in_place(&mut c.dir, "outputs[].dir")?;
        }
        OutputConfig::Http(c) => {
            expand_in_place(&mut c.url, "outputs[].url")?;
            if let Some(s) = c.bearer_auth.as_mut() {
                expand_in_place(s, "outputs[].bearer_auth")?;
            }
        }
        OutputConfig::S3(c) => {
            expand_in_place(&mut c.bucket, "outputs[].bucket")?;
            if let Some(s) = c.region.as_mut() {
                expand_in_place(s, "outputs[].region")?;
            }
            if let Some(s) = c.endpoint_url.as_mut() {
                expand_in_place(s, "outputs[].endpoint_url")?;
            }
            expand_in_place(&mut c.key_template, "outputs[].key_template")?;
        }
        OutputConfig::Void => {}
    }
    Ok(())
}

fn expand_in_place(s: &mut String, field_path: &str) -> Result<()> {
    let expanded = expand_str(s, field_path)?;
    *s = expanded;
    Ok(())
}

/// Expand `${VAR}` and `${VAR:-default}` references in `s`.
///
/// Syntax:
/// - `${NAME}` — substitute the value of env var `NAME`. Unset → error.
/// - `${NAME:-fallback}` — substitute env var `NAME` if set, else `fallback`.
///   `fallback` is plain text; nested expansions are not supported.
/// - `$$` — escaped `$`. A bare `$` not followed by `{` passes through verbatim.
fn expand_str(s: &str, field_path: &str) -> Result<String> {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'$' {
            out.push('$');
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let close = bytes[i + 2..]
                .iter()
                .position(|&b| b == b'}')
                .ok_or_else(|| {
                    anyhow!("{field_path}: unterminated `${{` in env interpolation: {s:?}")
                })?;
            let inner = &s[i + 2..i + 2 + close];
            let (name, fallback) = match inner.find(":-") {
                Some(pos) => (&inner[..pos], Some(&inner[pos + 2..])),
                None => (inner, None),
            };
            if name.is_empty() {
                return Err(anyhow!(
                    "{field_path}: empty variable name in `${{}}` placeholder"
                ));
            }
            match std::env::var(name) {
                Ok(v) => out.push_str(&v),
                Err(_) => match fallback {
                    Some(f) => out.push_str(f),
                    None => {
                        return Err(anyhow!(
                            "{field_path}: environment variable `{name}` is not set \
                             (use `${{{name}:-fallback}}` to provide a default)"
                        ))
                    }
                },
            }
            i += 2 + close + 1;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_passthrough_when_no_placeholders() {
        let s = expand_str("plain text", "f").unwrap();
        assert_eq!(s, "plain text");
    }

    #[test]
    fn expand_resolves_set_var() {
        // SAFETY: tests are single-threaded per process when the env is touched
        unsafe { std::env::set_var("BS_TEST_VAR_RESOLVES", "hello") };
        let s = expand_str("x=${BS_TEST_VAR_RESOLVES}!", "f").unwrap();
        assert_eq!(s, "x=hello!");
    }

    #[test]
    fn expand_uses_default_when_unset() {
        unsafe { std::env::remove_var("BS_TEST_VAR_DEFAULT") };
        let s = expand_str("x=${BS_TEST_VAR_DEFAULT:-fallback}", "f").unwrap();
        assert_eq!(s, "x=fallback");
    }

    #[test]
    fn expand_errors_when_unset_and_no_default() {
        unsafe { std::env::remove_var("BS_TEST_VAR_MISSING_NODEF") };
        let err = expand_str("${BS_TEST_VAR_MISSING_NODEF}", "outputs[0].url").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("outputs[0].url"), "{msg}");
        assert!(msg.contains("BS_TEST_VAR_MISSING_NODEF"), "{msg}");
    }

    #[test]
    fn expand_double_dollar_is_literal() {
        let s = expand_str("$$VAR", "f").unwrap();
        assert_eq!(s, "$VAR");
    }

    #[test]
    fn expand_unterminated_brace_errors() {
        let err = expand_str("${UNCLOSED", "f").unwrap_err();
        assert!(format!("{err}").contains("unterminated"));
    }

    #[test]
    fn output_config_deserializes_each_variant() {
        let yaml = r#"
type: file
dir: /tmp/out
"#;
        let c: OutputConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(c, OutputConfig::File(_)));

        let yaml = r#"
type: void
"#;
        let c: OutputConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(c, OutputConfig::Void));

        let yaml = r#"
type: http
url: https://example.com/api
"#;
        let c: OutputConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(c.type_name(), "http");

        let yaml = r#"
type: s3
bucket: my-results
"#;
        let c: OutputConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(c.type_name(), "s3");
    }

    #[test]
    fn output_config_rejects_unknown_keys() {
        let yaml = r#"
type: file
dir: /tmp/out
mystery: 42
"#;
        let err = serde_yaml::from_str::<OutputConfig>(yaml).unwrap_err();
        assert!(format!("{err}").contains("mystery"));
    }
}
