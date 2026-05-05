//! Resolve the active [`OutputConfig`] from CLI flags and config-file state.
//!
//! Two modes, mutually exclusive:
//!
//! - **Config-driven** — the YAML config has a non-empty `outputs:` list.
//!   The output spec comes entirely from there. Any per-output CLI flag
//!   being set is a hard error. String fields in the config undergo
//!   `${ENV_VAR}` interpolation.
//! - **CLI-driven** — the config has no `outputs:` list (or no config file
//!   was loaded). The output is built entirely from CLI flags. `--output`
//!   is required; flags targeting a different output type than the active
//!   one are an error.
//!
//! No per-field merge, no precedence puzzles.

use crate::config::output::{
    expand_env, FileOutputConfig, HttpAimdConfig, HttpOutputConfig, OutputConfig, S3OutputConfig,
};
use crate::config::types::ConfigSchema;
use anyhow::{anyhow, Result};

/// Selectable output type on the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum OutputKind {
    File,
    Http,
    S3,
    Void,
}

impl OutputKind {
    pub fn label(self) -> &'static str {
        match self {
            OutputKind::File => "file",
            OutputKind::Http => "http",
            OutputKind::S3 => "s3",
            OutputKind::Void => "void",
        }
    }
}

/// Per-output CLI inputs. All fields are `Option` so the resolver can
/// detect which were actually set on the command line.
///
/// Add new per-output fields here when introducing a new output knob.
#[derive(Debug, Default, Clone)]
pub struct OutputCli {
    pub output: Option<OutputKind>,

    // file
    pub output_dir: Option<String>,

    // http
    pub http_url: Option<String>,
    pub http_bearer_auth: Option<String>,
    pub http_timeout_secs: Option<u64>,
    pub http_batch_max_mb: Option<f64>,
    pub http_compressor_tasks: Option<usize>,
    pub http_upload_tasks: Option<usize>,
    pub http_upload_channel_size: Option<usize>,
    pub http_line_channel_size: Option<usize>,
    pub http_max_retries: Option<u32>,
    pub http_max_upload_rate_mbps: Option<f64>,
    pub http_aimd_decrease_factor: Option<f64>,
    pub http_aimd_increase_mbps: Option<f64>,
    pub http_aimd_max_submission_time_s: Option<f64>,

    // s3
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint_url: Option<String>,
    pub s3_key_template: Option<String>,
    pub s3_batch_max_mb: Option<f64>,
    pub s3_multipart_threshold_mb: Option<u64>,
    pub s3_multipart_part_mb: Option<u64>,
    pub s3_upload_tasks: Option<usize>,
    pub s3_compression_level: Option<i32>,

    // shared
    pub compression_level: Option<i32>,
}

impl OutputCli {
    /// Names of every field that's set, with their `--flag-name` form. Used
    /// when reporting a config-driven mode conflict so the error message
    /// names exactly which flag the user passed.
    fn set_flags(&self) -> Vec<&'static str> {
        let mut v = Vec::new();
        macro_rules! check {
            ($field:ident, $name:literal) => {
                if self.$field.is_some() {
                    v.push($name);
                }
            };
        }
        check!(output, "--output");
        check!(output_dir, "--output-dir");
        check!(http_url, "--http-url");
        check!(http_bearer_auth, "--http-bearer-auth");
        check!(http_timeout_secs, "--http-timeout");
        check!(http_batch_max_mb, "--http-batch-max-mb");
        check!(http_compressor_tasks, "--http-compressor-tasks");
        check!(http_upload_tasks, "--http-upload-tasks");
        check!(http_upload_channel_size, "--http-upload-channel-size");
        check!(http_line_channel_size, "--http-line-channel-size");
        check!(http_max_retries, "--http-max-retries");
        check!(http_max_upload_rate_mbps, "--max-upload-rate");
        check!(http_aimd_decrease_factor, "--http-aimd-decrease-factor");
        check!(http_aimd_increase_mbps, "--http-aimd-increase");
        check!(http_aimd_max_submission_time_s, "--max-submission-time");
        check!(s3_bucket, "--s3-output-bucket");
        check!(s3_region, "--s3-output-region");
        check!(s3_endpoint_url, "--s3-output-endpoint-url");
        check!(s3_key_template, "--s3-output-key-template");
        check!(s3_batch_max_mb, "--s3-output-batch-max-mb");
        check!(
            s3_multipart_threshold_mb,
            "--s3-output-multipart-threshold-mb"
        );
        check!(s3_multipart_part_mb, "--s3-output-multipart-part-mb");
        check!(s3_upload_tasks, "--s3-output-upload-tasks");
        check!(s3_compression_level, "--s3-output-compression-level");
        check!(compression_level, "--compression-level");
        v
    }

    fn is_empty(&self) -> bool {
        self.set_flags().is_empty()
    }
}

/// Resolve the active output from CLI + config.
///
/// Returns the chosen [`OutputConfig`] with `${ENV}` placeholders expanded,
/// or an error describing which flag/field caused the conflict.
pub fn resolve_output(cli: &OutputCli, schema: &ConfigSchema) -> Result<OutputConfig> {
    schema.validate_outputs().map_err(|e| anyhow!(e))?;

    let config_has_outputs = !schema.outputs.is_empty();

    if config_has_outputs {
        if !cli.is_empty() {
            let flags = cli.set_flags().join(", ");
            return Err(anyhow!(
                "output settings come from the config file `outputs:` block; \
                 remove these CLI flags or drop `outputs:` from the config: {flags}"
            ));
        }
        let mut chosen = schema.outputs[0].clone();
        expand_env(&mut chosen)?;
        Ok(chosen)
    } else {
        build_from_cli(cli)
    }
}

fn build_from_cli(cli: &OutputCli) -> Result<OutputConfig> {
    let kind = cli.output.ok_or_else(|| {
        anyhow!(
            "no output configured: pass `--output <file|http|s3|void>` \
             or define an `outputs:` block in the config file"
        )
    })?;

    // Reject per-type flags that don't apply to the chosen output.
    reject_inactive_flags(kind, cli)?;

    Ok(match kind {
        OutputKind::File => OutputConfig::File(FileOutputConfig {
            dir: cli
                .output_dir
                .clone()
                .ok_or_else(|| anyhow!("--output file requires --output-dir <path>"))?,
            compression_level: cli.compression_level,
        }),
        OutputKind::Http => {
            let url = cli
                .http_url
                .clone()
                .ok_or_else(|| anyhow!("--output http requires --http-url <url>"))?;
            OutputConfig::Http(HttpOutputConfig {
                url,
                bearer_auth: cli.http_bearer_auth.clone(),
                timeout_secs: cli.http_timeout_secs.unwrap_or(30),
                batch_max_mb: cli.http_batch_max_mb.unwrap_or(2.0),
                compressor_tasks: cli.http_compressor_tasks,
                upload_tasks: cli.http_upload_tasks,
                upload_channel_size: cli.http_upload_channel_size.unwrap_or(4),
                line_channel_size: cli.http_line_channel_size.unwrap_or(1000),
                compression_level: cli.compression_level,
                max_retries: cli.http_max_retries.unwrap_or(3),
                max_upload_rate_mbps: cli.http_max_upload_rate_mbps.unwrap_or(0.0),
                aimd: HttpAimdConfig {
                    decrease_factor: cli.http_aimd_decrease_factor.unwrap_or(0.15),
                    increase_mbps: cli.http_aimd_increase_mbps.unwrap_or(1.0),
                    max_submission_time_s: cli.http_aimd_max_submission_time_s.unwrap_or(4.0),
                },
            })
        }
        OutputKind::S3 => {
            let bucket = cli
                .s3_bucket
                .clone()
                .ok_or_else(|| anyhow!("--output s3 requires --s3-output-bucket <name>"))?;
            OutputConfig::S3(S3OutputConfig {
                bucket,
                region: cli.s3_region.clone(),
                endpoint_url: cli.s3_endpoint_url.clone(),
                key_template: cli
                    .s3_key_template
                    .clone()
                    .unwrap_or_else(|| "results/{prefix}/part-{seq}.ndjson.zst".to_string()),
                batch_max_mb: cli.s3_batch_max_mb,
                compression_level: cli.s3_compression_level.or(cli.compression_level),
                multipart_threshold_mb: cli.s3_multipart_threshold_mb.unwrap_or(64),
                multipart_part_mb: cli.s3_multipart_part_mb.unwrap_or(16),
                upload_tasks: cli.s3_upload_tasks,
            })
        }
        OutputKind::Void => OutputConfig::Void,
    })
}

fn reject_inactive_flags(kind: OutputKind, cli: &OutputCli) -> Result<()> {
    let mut bad: Vec<&'static str> = Vec::new();
    macro_rules! reject_unless {
        ($cond:expr, $field:ident, $flag:literal) => {
            if !($cond) && cli.$field.is_some() {
                bad.push($flag);
            }
        };
    }

    let is_file = matches!(kind, OutputKind::File);
    let is_http = matches!(kind, OutputKind::Http);
    let is_s3 = matches!(kind, OutputKind::S3);

    reject_unless!(is_file, output_dir, "--output-dir");

    reject_unless!(is_http, http_url, "--http-url");
    reject_unless!(is_http, http_bearer_auth, "--http-bearer-auth");
    reject_unless!(is_http, http_timeout_secs, "--http-timeout");
    reject_unless!(is_http, http_batch_max_mb, "--http-batch-max-mb");
    reject_unless!(is_http, http_compressor_tasks, "--http-compressor-tasks");
    reject_unless!(is_http, http_upload_tasks, "--http-upload-tasks");
    reject_unless!(
        is_http,
        http_upload_channel_size,
        "--http-upload-channel-size"
    );
    reject_unless!(is_http, http_line_channel_size, "--http-line-channel-size");
    reject_unless!(is_http, http_max_retries, "--http-max-retries");
    reject_unless!(is_http, http_max_upload_rate_mbps, "--max-upload-rate");
    reject_unless!(
        is_http,
        http_aimd_decrease_factor,
        "--http-aimd-decrease-factor"
    );
    reject_unless!(is_http, http_aimd_increase_mbps, "--http-aimd-increase");
    reject_unless!(
        is_http,
        http_aimd_max_submission_time_s,
        "--max-submission-time"
    );

    reject_unless!(is_s3, s3_bucket, "--s3-output-bucket");
    reject_unless!(is_s3, s3_region, "--s3-output-region");
    reject_unless!(is_s3, s3_endpoint_url, "--s3-output-endpoint-url");
    reject_unless!(is_s3, s3_key_template, "--s3-output-key-template");
    reject_unless!(is_s3, s3_batch_max_mb, "--s3-output-batch-max-mb");
    reject_unless!(
        is_s3,
        s3_multipart_threshold_mb,
        "--s3-output-multipart-threshold-mb"
    );
    reject_unless!(is_s3, s3_multipart_part_mb, "--s3-output-multipart-part-mb");
    reject_unless!(is_s3, s3_upload_tasks, "--s3-output-upload-tasks");
    reject_unless!(is_s3, s3_compression_level, "--s3-output-compression-level");

    if !bad.is_empty() {
        return Err(anyhow!(
            "--output {active} is incompatible with: {flags} \
             (those flags target a different output type)",
            active = kind.label(),
            flags = bad.join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_cli() -> OutputCli {
        OutputCli::default()
    }

    #[test]
    fn cli_driven_void_works() {
        let cli = OutputCli {
            output: Some(OutputKind::Void),
            ..Default::default()
        };
        let cfg = resolve_output(&cli, &ConfigSchema::default()).unwrap();
        assert!(matches!(cfg, OutputConfig::Void));
    }

    #[test]
    fn cli_driven_file_requires_output_dir() {
        let cli = OutputCli {
            output: Some(OutputKind::File),
            ..Default::default()
        };
        let err = resolve_output(&cli, &ConfigSchema::default()).unwrap_err();
        assert!(format!("{err}").contains("--output-dir"));
    }

    #[test]
    fn cli_driven_file_with_output_dir_resolves() {
        let cli = OutputCli {
            output: Some(OutputKind::File),
            output_dir: Some("/tmp/x".to_string()),
            ..Default::default()
        };
        let cfg = resolve_output(&cli, &ConfigSchema::default()).unwrap();
        match cfg {
            OutputConfig::File(f) => assert_eq!(f.dir, "/tmp/x"),
            other => panic!("expected file, got {other:?}"),
        }
    }

    #[test]
    fn cli_driven_no_output_flag_errors() {
        let err = resolve_output(&empty_cli(), &ConfigSchema::default()).unwrap_err();
        assert!(format!("{err}").contains("--output"));
    }

    #[test]
    fn config_driven_rejects_any_cli_flag() {
        let mut schema = ConfigSchema::default();
        schema.outputs.push(OutputConfig::Void);
        let cli = OutputCli {
            output: Some(OutputKind::Void),
            ..Default::default()
        };
        let err = resolve_output(&cli, &schema).unwrap_err();
        assert!(format!("{err}").contains("--output"));
    }

    #[test]
    fn config_driven_picks_first_entry() {
        let mut schema = ConfigSchema::default();
        schema.outputs.push(OutputConfig::Void);
        let cfg = resolve_output(&empty_cli(), &schema).unwrap();
        assert!(matches!(cfg, OutputConfig::Void));
    }

    #[test]
    fn cli_driven_rejects_inactive_flags() {
        let cli = OutputCli {
            output: Some(OutputKind::File),
            output_dir: Some("/tmp".to_string()),
            s3_bucket: Some("results".to_string()),
            ..Default::default()
        };
        let err = resolve_output(&cli, &ConfigSchema::default()).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("--s3-output-bucket"), "{msg}");
    }

    #[test]
    fn config_driven_expands_env_in_string_fields() {
        unsafe { std::env::set_var("BS_TEST_RESOLVE_TOKEN", "abc123") };
        let mut schema = ConfigSchema::default();
        schema.outputs.push(OutputConfig::Http(HttpOutputConfig {
            url: "https://api.example.com/${BS_TEST_RESOLVE_TOKEN}".to_string(),
            bearer_auth: Some("${BS_TEST_RESOLVE_TOKEN}".to_string()),
            timeout_secs: 10,
            batch_max_mb: 1.0,
            compressor_tasks: None,
            upload_tasks: None,
            upload_channel_size: 4,
            line_channel_size: 1000,
            compression_level: None,
            max_retries: 1,
            max_upload_rate_mbps: 0.0,
            aimd: HttpAimdConfig::default(),
        }));
        let cfg = resolve_output(&empty_cli(), &schema).unwrap();
        match cfg {
            OutputConfig::Http(h) => {
                assert_eq!(h.url, "https://api.example.com/abc123");
                assert_eq!(h.bearer_auth.as_deref(), Some("abc123"));
            }
            other => panic!("expected http, got {other:?}"),
        }
    }

    #[test]
    fn schema_with_two_outputs_errors() {
        let mut schema = ConfigSchema::default();
        schema.outputs.push(OutputConfig::Void);
        schema.outputs.push(OutputConfig::Void);
        let err = resolve_output(&empty_cli(), &schema).unwrap_err();
        assert!(format!("{err}").contains("only 1 is supported"));
    }
}
