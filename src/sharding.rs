//! Index-modulo sharding: shed objects whose index in the deterministically
//! sorted, sampled list falls outside this instance's shard.
//!
//! Naive partition for distributing work across N stateless workers. Each
//! worker lists the same bucket(s), applies the same filter/sort/sampling,
//! then keeps only objects where `index % shard_count == shard_number`.
//! Workload imbalance scales with per-file size variance — accepted tradeoff
//! for zero-coordination distribution.
//!
//! Determinism precondition: callers must guarantee the input `Vec` is in the
//! same order across all worker instances at the moment `apply` is invoked.
//! See `main.rs` for the size+bucket+key total-order sort that backs this.

use crate::s3::S3ObjectInfo;

pub struct ShardSelector {
    count: usize,
    number: usize,
}

impl ShardSelector {
    /// Returns `Err` on `count < 2` (no-op or zero, almost certainly a config
    /// mistake) or `number >= count`.
    pub fn new(count: usize, number: usize) -> Result<Self, String> {
        if count < 2 {
            return Err(format!(
                "shard-count must be >= 2, got {count} \
                 (omit both shard flags to disable sharding rather than setting count=1)"
            ));
        }
        if number >= count {
            return Err(format!(
                "shard-number must be < shard-count, got number={number} count={count}"
            ));
        }
        Ok(Self { count, number })
    }

    /// Keep only objects whose index satisfies `i % count == number`.
    /// Returns `(kept, dropped)`.
    pub fn apply(&self, objects: &mut Vec<S3ObjectInfo>) -> (usize, usize) {
        let before = objects.len();
        let mut i = 0usize;
        objects.retain(|_| {
            let keep = i % self.count == self.number;
            i += 1;
            keep
        });
        let kept = objects.len();
        (kept, before - kept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashSet;

    fn obj(key: &str) -> S3ObjectInfo {
        S3ObjectInfo {
            bucket: "b".to_string(),
            key: key.to_string(),
            size: 1,
            last_modified: Utc::now(),
            prefix: String::new(),
        }
    }

    fn fake_objects(n: usize) -> Vec<S3ObjectInfo> {
        (0..n).map(|i| obj(&format!("k{i:04}"))).collect()
    }

    #[test]
    fn rejects_count_below_two() {
        assert!(ShardSelector::new(0, 0).is_err());
        assert!(ShardSelector::new(1, 0).is_err());
    }

    #[test]
    fn rejects_number_out_of_range() {
        assert!(ShardSelector::new(3, 3).is_err());
        assert!(ShardSelector::new(3, 99).is_err());
    }

    #[test]
    fn accepts_valid_inputs() {
        ShardSelector::new(2, 0).unwrap();
        ShardSelector::new(2, 1).unwrap();
        ShardSelector::new(100, 99).unwrap();
    }

    #[test]
    fn shards_partition_input_exactly() {
        const N: usize = 50;
        const COUNT: usize = 7;
        let input = fake_objects(N);

        let mut union: Vec<String> = Vec::new();
        for number in 0..COUNT {
            let mut shard = input.clone();
            ShardSelector::new(COUNT, number).unwrap().apply(&mut shard);
            for o in &shard {
                union.push(o.key.clone());
            }
        }

        // Coverage: every input key shows up exactly once across all shards.
        union.sort();
        let mut expected: Vec<String> = input.iter().map(|o| o.key.clone()).collect();
        expected.sort();
        assert_eq!(union, expected);

        // Disjointness: total kept == input size.
        let unique: HashSet<&String> = union.iter().collect();
        assert_eq!(unique.len(), N);
    }

    #[test]
    fn deterministic_across_calls() {
        let input = fake_objects(20);
        let mut a = input.clone();
        let mut b = input.clone();
        ShardSelector::new(4, 2).unwrap().apply(&mut a);
        ShardSelector::new(4, 2).unwrap().apply(&mut b);
        let keys_a: Vec<_> = a.iter().map(|o| o.key.clone()).collect();
        let keys_b: Vec<_> = b.iter().map(|o| o.key.clone()).collect();
        assert_eq!(keys_a, keys_b);
    }

    #[test]
    fn input_smaller_than_count_yields_partial_shards() {
        // 2 objects, 5 shards: shards 0 and 1 each get one object,
        // shards 2..5 get zero. That's allowed.
        let input = fake_objects(2);
        let mut total = 0usize;
        for number in 0..5 {
            let mut shard = input.clone();
            ShardSelector::new(5, number).unwrap().apply(&mut shard);
            total += shard.len();
        }
        assert_eq!(total, 2);
    }

    #[test]
    fn empty_input_yields_empty_shard() {
        let mut input: Vec<S3ObjectInfo> = Vec::new();
        let (kept, dropped) = ShardSelector::new(3, 1).unwrap().apply(&mut input);
        assert_eq!(kept, 0);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn returns_correct_kept_dropped_counts() {
        let mut input = fake_objects(10);
        let (kept, dropped) = ShardSelector::new(3, 0).unwrap().apply(&mut input);
        // Indices 0,3,6,9 → 4 kept, 6 dropped.
        assert_eq!(kept, 4);
        assert_eq!(dropped, 6);
        assert_eq!(input.len(), 4);
    }
}
