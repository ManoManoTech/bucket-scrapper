//! Output path / S3 key templating shared by the file and s3 sinks.
//!
//! Both sinks render a per-prefix output destination from a template
//! string. They share a placeholder language so users learn it once:
//!
//! - `{prefix}` — the source S3 prefix (e.g. `logs/dt=20240101/hour=09`)
//! - `{prefix_hash}` — 8-char hex hash of the prefix; useful when the
//!   raw prefix contains characters that are awkward in destination paths
//! - `{run_id}` — 8-char hex hash unique to this process invocation
//! - `{seq}` — zero-padded per-prefix sequence number (s3 only;
//!   incremented every time a mid-run batch is finalized when
//!   `batch_max_mb` is set)
//! - `{ext}` — codec-derived file extension (`zst`, `gz`, or empty for
//!   plaintext); when the template ends with `.{ext}` and the codec is
//!   `none`, the trailing dot is also dropped
//!
//! Templates that don't contain `{prefix}` or `{prefix_hash}` are
//! rejected at config-resolve time — every source prefix would otherwise
//! render to the same destination, which corrupts the file sink (two
//! encoders writing to one file) and silently overwrites s3 objects.

use anyhow::{anyhow, Result};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Template-validation rules for one of the sinks.
#[derive(Debug, Clone, Copy)]
pub struct TemplateRules {
    /// `{seq}` must appear in the template (s3 with `batch_max_mb` set).
    pub require_seq: bool,
    /// `{seq}` is meaningful for this sink. File sink emits one object
    /// per prefix, so a `{seq}` placeholder there is almost certainly
    /// a user mistake.
    pub allow_seq: bool,
}

/// Validate that a template contains the placeholders the sink needs.
///
/// `field_path` is included in error messages so the user can identify
/// which YAML field tripped the rule.
pub fn validate_template(template: &str, field_path: &str, rules: TemplateRules) -> Result<()> {
    let has_prefix = template.contains("{prefix}");
    let has_prefix_hash = template.contains("{prefix_hash}");
    if !has_prefix && !has_prefix_hash {
        return Err(anyhow!(
            "{field_path}: template `{template}` must contain `{{prefix}}` or `{{prefix_hash}}` — \
             without one, every source prefix would render to the same destination \
             and outputs would collide"
        ));
    }
    let has_seq = template.contains("{seq}");
    if rules.require_seq && !has_seq {
        return Err(anyhow!(
            "{field_path}: template `{template}` must contain `{{seq}}` when `batch_max_mb` is set \
             — without it, each mid-run batch within a prefix would render to the same key and \
             silently overwrite the previous one"
        ));
    }
    if !rules.allow_seq && has_seq {
        return Err(anyhow!(
            "{field_path}: template `{template}` contains `{{seq}}`, but this output emits one \
             file per source prefix and has no per-batch sequence; remove `{{seq}}` \
             (it would render as a literal string)"
        ));
    }
    Ok(())
}

/// Bag of placeholder values for [`render_template`].
#[derive(Debug, Clone)]
pub struct TemplateValues<'a> {
    pub prefix: &'a str,
    pub run_id: &'a str,
    pub seq: u64,
    pub ext: &'a str,
}

/// Render a template against concrete values. When `ext` is empty,
/// `.{ext}` collapses to nothing (we drop both the dot and the
/// placeholder) so plaintext outputs don't end with a stray `.`.
pub fn render_template(template: &str, values: &TemplateValues<'_>) -> String {
    let prefix_hash = short_hash(values.prefix);
    let seq_str = format!("{:05}", values.seq);

    let mut s = template.to_string();
    if values.ext.is_empty() {
        // Collapse `.{ext}` first so the trailing dot disappears with the placeholder.
        s = s.replace(".{ext}", "");
        s = s.replace("{ext}", "");
    } else {
        s = s.replace("{ext}", values.ext);
    }
    s = s.replace("{prefix_hash}", &prefix_hash);
    s = s.replace("{prefix}", values.prefix);
    s = s.replace("{run_id}", values.run_id);
    s = s.replace("{seq}", &seq_str);
    s
}

/// 8-char hex hash of an arbitrary string. Stable across runs.
pub fn short_hash(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

/// Mint an opaque per-run identifier for `{run_id}` substitution.
pub fn make_run_id() -> String {
    let mut hasher = DefaultHasher::new();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    nanos.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    format!("{:08x}", hasher.finish() as u32)
}

/// Defence-in-depth runtime guard. Static template validation catches the
/// common case (template missing `{prefix}` / `{prefix_hash}`); this
/// catches the residual case where two distinct prefixes hash to the
/// same `{prefix_hash}`, or where a template's literal segments mask
/// the distinguishing placeholders for some inputs.
///
/// Records each (rendered destination → first-seen prefix) and reports
/// when a *different* prefix subsequently renders to the same
/// destination. Single-prefix re-use (e.g. s3 rollover with `{seq}`
/// changing) is fine and not flagged.
#[derive(Debug, Default)]
pub struct CollisionTracker {
    seen: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CollisionResult<'a> {
    /// First time we see this rendered destination.
    First,
    /// Same prefix already wrote here (e.g. rollover with `{seq}` not in
    /// the template — but that should have been caught statically).
    SamePrefix,
    /// A different prefix rendered to the same destination.
    Collision { existing_prefix: &'a str },
}

impl CollisionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, prefix: &str, rendered: &str) -> CollisionResult<'_> {
        if let Some(existing) = self.seen.get(rendered) {
            if existing == prefix {
                CollisionResult::SamePrefix
            } else {
                CollisionResult::Collision {
                    existing_prefix: self.seen.get(rendered).map(|s| s.as_str()).unwrap(),
                }
            }
        } else {
            self.seen.insert(rendered.to_string(), prefix.to_string());
            CollisionResult::First
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vals<'a>(prefix: &'a str, ext: &'a str) -> TemplateValues<'a> {
        TemplateValues {
            prefix,
            run_id: "abcd1234",
            seq: 0,
            ext,
        }
    }

    #[test]
    fn render_substitutes_all_known_placeholders() {
        let s = render_template(
            "out/{prefix}/{prefix_hash}-{seq}-{run_id}.{ext}",
            &TemplateValues {
                prefix: "logs/dt=20240101",
                run_id: "abcd1234",
                seq: 7,
                ext: "zst",
            },
        );
        assert!(s.starts_with("out/logs/dt=20240101/"));
        assert!(s.contains("00007"));
        assert!(s.ends_with("-abcd1234.zst"));
    }

    #[test]
    fn render_drops_dot_when_ext_empty() {
        let s = render_template("{prefix}.{ext}", &vals("data", ""));
        assert_eq!(s, "data");
    }

    #[test]
    fn render_keeps_dot_when_ext_present() {
        let s = render_template("{prefix}.{ext}", &vals("data", "zst"));
        assert_eq!(s, "data.zst");
    }

    #[test]
    fn render_handles_double_extension_with_empty_ext() {
        let s = render_template("{prefix}.ndjson.{ext}", &vals("data", ""));
        assert_eq!(s, "data.ndjson");
    }

    #[test]
    fn validate_rejects_template_without_prefix_or_prefix_hash() {
        let err = validate_template(
            "out/results.{ext}",
            "outputs[].path_template",
            TemplateRules {
                require_seq: false,
                allow_seq: false,
            },
        )
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("path_template"));
        assert!(msg.contains("{prefix}"));
    }

    #[test]
    fn validate_rejects_missing_seq_when_required() {
        let err = validate_template(
            "out/{prefix}.{ext}",
            "outputs[].key_template",
            TemplateRules {
                require_seq: true,
                allow_seq: true,
            },
        )
        .unwrap_err();
        assert!(format!("{err}").contains("{seq}"));
    }

    #[test]
    fn validate_accepts_prefix_hash_alone() {
        validate_template(
            "out/{prefix_hash}.{ext}",
            "outputs[].path_template",
            TemplateRules {
                require_seq: false,
                allow_seq: false,
            },
        )
        .unwrap();
    }

    #[test]
    fn validate_rejects_seq_when_disallowed() {
        let err = validate_template(
            "{prefix}-{seq}.{ext}",
            "outputs[].path_template",
            TemplateRules {
                require_seq: false,
                allow_seq: false,
            },
        )
        .unwrap_err();
        assert!(format!("{err}").contains("{seq}"));
    }

    #[test]
    fn validate_accepts_seq_when_allowed() {
        validate_template(
            "{prefix}-{seq}.{ext}",
            "outputs[].key_template",
            TemplateRules {
                require_seq: true,
                allow_seq: true,
            },
        )
        .unwrap();
    }

    #[test]
    fn collision_tracker_first_then_same() {
        let mut t = CollisionTracker::new();
        assert_eq!(t.record("a", "out/a.zst"), CollisionResult::First);
        assert_eq!(t.record("a", "out/a.zst"), CollisionResult::SamePrefix);
    }

    #[test]
    fn collision_tracker_detects_different_prefix() {
        let mut t = CollisionTracker::new();
        t.record("a", "out/x.zst");
        match t.record("b", "out/x.zst") {
            CollisionResult::Collision { existing_prefix } => assert_eq!(existing_prefix, "a"),
            other => panic!("expected collision, got {other:?}"),
        }
    }
}
