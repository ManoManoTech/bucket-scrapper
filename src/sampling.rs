//! File-level sampling: probabilistic drop of S3 objects after key filtering
//! and before they enter the download/decompress pipeline.
//!
//! Sampling is the coarsest of the work-shedding mechanisms — it sheds whole
//! files. For sources with high per-file size variance, the resulting
//! line-volume sample can be noisy.

use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::s3::S3ObjectInfo;

/// Validate a sample rate. Accepts `(0.0, 1.0]`. Rejects NaN, `<= 0.0`, `> 1.0`.
///
/// `None` is the way to express "no sampling"; `0.0` would silently drop every
/// file and is almost certainly a configuration mistake.
pub fn is_valid_sampling_rate(rate: f64) -> Result<(), String> {
    if rate.is_nan() {
        return Err("sample rate is NaN".to_string());
    }
    if rate <= 0.0 {
        return Err(format!(
            "sample rate must be in (0.0, 1.0], got {rate} \
             (omit the field to disable sampling rather than setting 0.0)"
        ));
    }
    if rate > 1.0 {
        return Err(format!("sample rate must be in (0.0, 1.0], got {rate}"));
    }
    Ok(())
}

/// Clap value parser for `--sample-files`.
pub fn parse_unit_interval(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|e| format!("not a float: {e}"))?;
    is_valid_sampling_rate(v)?;
    Ok(v)
}

pub struct FileSampler {
    rng: StdRng,
    per_bucket: HashMap<String, f64>,
    default_rate: f64,
}

impl FileSampler {
    pub fn new(default_rate: f64, per_bucket: HashMap<String, f64>, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };
        Self {
            rng,
            per_bucket,
            default_rate,
        }
    }

    /// Apply sampling in place. Returns `(kept, dropped)`.
    pub fn apply(&mut self, objects: &mut Vec<S3ObjectInfo>) -> (usize, usize) {
        let before = objects.len();
        objects.retain(|o| {
            let rate = self
                .per_bucket
                .get(&o.bucket)
                .copied()
                .unwrap_or(self.default_rate);
            rate >= 1.0 || self.rng.gen::<f64>() < rate
        });
        let kept = objects.len();
        (kept, before - kept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn obj(bucket: &str, key: &str) -> S3ObjectInfo {
        S3ObjectInfo {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: 1,
            last_modified: Utc::now(),
            prefix: String::new(),
        }
    }

    fn fake_objects(bucket: &str, n: usize) -> Vec<S3ObjectInfo> {
        (0..n).map(|i| obj(bucket, &format!("k{i}"))).collect()
    }

    #[test]
    fn valid_rate_accepts_unit_interval() {
        is_valid_sampling_rate(0.1).unwrap();
        is_valid_sampling_rate(1.0).unwrap();
        is_valid_sampling_rate(0.0001).unwrap();
    }

    #[test]
    fn valid_rate_rejects_zero_negative_and_too_big() {
        assert!(is_valid_sampling_rate(0.0).is_err());
        assert!(is_valid_sampling_rate(-0.1).is_err());
        assert!(is_valid_sampling_rate(1.5).is_err());
        assert!(is_valid_sampling_rate(f64::NAN).is_err());
    }

    #[test]
    fn parse_unit_interval_round_trip() {
        assert_eq!(parse_unit_interval("0.5").unwrap(), 0.5);
        assert!(parse_unit_interval("-0.1").is_err());
        assert!(parse_unit_interval("1.5").is_err());
        assert!(parse_unit_interval("abc").is_err());
    }

    #[test]
    fn seeded_sampling_is_deterministic() {
        let input = fake_objects("b", 1000);
        let mut a = input.clone();
        let mut b = input.clone();

        let mut s1 = FileSampler::new(0.5, HashMap::new(), Some(42));
        let mut s2 = FileSampler::new(0.5, HashMap::new(), Some(42));
        s1.apply(&mut a);
        s2.apply(&mut b);

        let keys_a: Vec<_> = a.iter().map(|o| o.key.clone()).collect();
        let keys_b: Vec<_> = b.iter().map(|o| o.key.clone()).collect();
        assert_eq!(keys_a, keys_b);
    }

    #[test]
    fn rate_one_keeps_everything() {
        let mut objs = fake_objects("b", 100);
        let mut s = FileSampler::new(1.0, HashMap::new(), Some(0));
        let (kept, dropped) = s.apply(&mut objs);
        assert_eq!(kept, 100);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn rate_half_keeps_roughly_half() {
        let mut objs = fake_objects("b", 10_000);
        let mut s = FileSampler::new(0.5, HashMap::new(), Some(7));
        let (kept, _dropped) = s.apply(&mut objs);
        assert!(
            (4_700..=5_300).contains(&kept),
            "expected ~5000 kept, got {kept}"
        );
    }

    #[test]
    fn per_bucket_overrides_default() {
        let mut objs = fake_objects("keep", 50);
        objs.extend(fake_objects("drop", 50));

        let mut per_bucket = HashMap::new();
        per_bucket.insert("keep".to_string(), 1.0);
        // default_rate just needs to be valid; we want the override path
        // to dominate. Use 1.0 here too, then override "drop" to a tiny rate.
        per_bucket.insert("drop".to_string(), 0.0001);

        let mut s = FileSampler::new(1.0, per_bucket, Some(123));
        s.apply(&mut objs);

        let kept_keep = objs.iter().filter(|o| o.bucket == "keep").count();
        let kept_drop = objs.iter().filter(|o| o.bucket == "drop").count();
        assert_eq!(kept_keep, 50);
        assert!(
            kept_drop < 5,
            "expected almost all 'drop' shed, got {kept_drop}"
        );
    }
}
