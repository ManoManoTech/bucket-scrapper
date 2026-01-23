// src/utils/character_counter.rs
// Location: src/utils/character_counter.rs

use std::ops::AddAssign;

/// Stores character count information for files
#[derive(Debug, Clone)]
pub struct DetailedCharacterCount {
    /// Counts of each ASCII character (0-255)
    pub bucket: String,
    pub prefix: String,
    pub counts: [u64; 256],
}

// impl Default for DetailedCharacterCount {
//     fn default() -> Self {
//         Self { counts: [0; 256] }
//     }
// }

impl DetailedCharacterCount {
    /// Creates a new DetailedCharacterCount with all counts set to 0
    ///
    /// # Examples
    ///
    /// ```
    /// use log_consolidator_checker_rust::utils::character_counter::DetailedCharacterCount;
    ///
    /// let counter = DetailedCharacterCount::new();
    /// assert_eq!(counter.total(), 0);
    /// assert_eq!(counter.total_excluding_newlines(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            bucket: "".to_string(),
            prefix: "".to_string(),
            counts: [0; 256],
        }
    }

    /// Increments the count for a specific byte
    #[allow(dead_code)]
    pub fn increment(&mut self, byte: u8) {
        self.counts[byte as usize] = self.counts[byte as usize].wrapping_add(1);
    }

    #[inline(always)]
    pub fn increment_batch_unsafe(&mut self, bytes: &[u8]) {
        let counts_ptr = self.counts.as_mut_ptr();
        let bytes_ptr = bytes.as_ptr();
        let len = bytes.len();

        unsafe {
            let mut p = bytes_ptr;
            let end = bytes_ptr.add(len);
            let end_aligned = bytes_ptr.add(len - (len % 8));

            // Process 8 bytes at a time
            while p < end_aligned {
                // Load 8 bytes and increment their respective counters
                *counts_ptr.add(*p as usize) += 1;
                *counts_ptr.add(*p.add(1) as usize) += 1;
                *counts_ptr.add(*p.add(2) as usize) += 1;
                *counts_ptr.add(*p.add(3) as usize) += 1;
                *counts_ptr.add(*p.add(4) as usize) += 1;
                *counts_ptr.add(*p.add(5) as usize) += 1;
                *counts_ptr.add(*p.add(6) as usize) += 1;
                *counts_ptr.add(*p.add(7) as usize) += 1;

                p = p.add(8);
            }

            // Handle remaining bytes
            while p < end {
                *counts_ptr.add(*p as usize) += 1;
                p = p.add(1);
            }
        }
    }

    /// Adds the counts from another DetailedCharacterCount
    pub fn add(&mut self, other: &DetailedCharacterCount) {
        for i in 0..256 {
            self.counts[i] = self.counts[i].wrapping_add(other.counts[i]);
        }
    }

    /// Gets the total count of characters, excluding newlines and carriage returns
    ///
    /// # Examples
    ///
    /// ```
    /// use log_consolidator_checker_rust::utils::character_counter::DetailedCharacterCount;
    ///
    /// let mut counter = DetailedCharacterCount::new();
    /// counter.increment(b'a');
    /// counter.increment(b'b');
    /// counter.increment(b'\n'); // newline - should be excluded
    /// counter.increment(b'\r'); // carriage return - should be excluded
    ///
    /// assert_eq!(counter.total(), 4);
    /// assert_eq!(counter.total_excluding_newlines(), 2); // Only 'a' and 'b'
    /// ```
    pub fn total_excluding_newlines(&self) -> u64 {
        let mut total: u64 = 0;
        for (i, &count) in self.counts.iter().enumerate() {
            // Skip newlines (10) and carriage returns (13)
            if i != 10 && i != 13 {
                total = total.wrapping_add(count);
            }
        }
        total
    }

    /// Gets the total count of all characters
    ///
    /// # Examples
    ///
    /// ```
    /// use log_consolidator_checker_rust::utils::character_counter::DetailedCharacterCount;
    ///
    /// let mut counter = DetailedCharacterCount::new();
    /// counter.increment(b'a');
    /// counter.increment(b'b');
    /// counter.increment(b'\n');
    ///
    /// assert_eq!(counter.total(), 3);
    /// ```
    pub fn total(&self) -> u64 {
        self.counts.iter().fold(0, |acc, &c| acc.wrapping_add(c))
    }

    /// Compare with another DetailedCharacterCount, returning differences
    ///
    /// # Examples
    ///
    /// ```
    /// use log_consolidator_checker_rust::utils::character_counter::DetailedCharacterCount;
    ///
    /// // Test basic comparison
    /// let mut counter1 = DetailedCharacterCount::new();
    /// counter1.increment(b'a');
    /// counter1.increment(b'b');
    ///
    /// let mut counter2 = DetailedCharacterCount::new();
    /// counter2.increment(b'a');
    /// counter2.increment(b'c');
    ///
    /// let (is_equal, differences) = counter1.compare(&counter2);
    /// assert_eq!(is_equal, false);
    /// assert_eq!(differences.len(), 2); // 'b' and 'c' are different
    ///
    /// // Test increment_batch_unsafe with 30 chars (tests both aligned and unaligned paths)
    /// let mut counter3 = DetailedCharacterCount::new();
    /// let buffer: Vec<u8> = (b'A'..=b'Z').chain(b'a'..=b'd').collect(); // 30 bytes: A-Z + a-d
    /// counter3.increment_batch_unsafe(&buffer);
    ///
    /// assert_eq!(counter3.total(), 30);
    /// assert_eq!(counter3.counts[b'A' as usize], 1);
    /// assert_eq!(counter3.counts[b'Z' as usize], 1);
    /// assert_eq!(counter3.counts[b'a' as usize], 1);
    /// assert_eq!(counter3.counts[b'd' as usize], 1);
    ///
    /// // Test comparison ignoring newlines and CR
    /// let mut counter4 = DetailedCharacterCount::new();
    /// counter4.increment(b'x');
    /// counter4.increment(b'\n'); // Should be ignored in comparison
    /// counter4.increment(b'\r'); // Should be ignored in comparison
    ///
    /// let mut counter5 = DetailedCharacterCount::new();
    /// counter5.increment(b'x');
    ///
    /// let (is_equal_cr, differences_cr) = counter4.compare(&counter5);
    /// assert_eq!(is_equal_cr, true); // Should be equal since newlines are ignored
    /// assert_eq!(differences_cr.len(), 0);
    /// ```
    pub fn compare(
        &self,
        other: &DetailedCharacterCount,
    ) -> (bool, std::collections::HashMap<usize, i64>) {
        let mut differences = std::collections::HashMap::new();
        let mut is_equal = true;

        for i in 0..256 {
            // Skip newlines (10) and carriage returns (13) as in the original
            if i == 10 || i == 13 {
                continue;
            }

            if self.counts[i] != other.counts[i] {
                is_equal = false;
                let diff = self.counts[i] as i64 - other.counts[i] as i64;
                differences.insert(i, diff);
            }
        }

        (is_equal, differences)
    }
}

// Implement AddAssign to make it easier to combine counts
impl AddAssign<&DetailedCharacterCount> for DetailedCharacterCount {
    fn add_assign(&mut self, other: &Self) {
        self.add(other);
    }
}

impl std::fmt::Display for DetailedCharacterCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Total characters: {}", self.total())?;
        write!(
            f,
            " (excluding newlines: {})",
            self.total_excluding_newlines()
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let counter = DetailedCharacterCount::new();
        assert_eq!(counter.total(), 0);
        assert_eq!(counter.total_excluding_newlines(), 0);
        assert_eq!(counter.bucket, "");
        assert_eq!(counter.prefix, "");
        for count in counter.counts.iter() {
            assert_eq!(*count, 0);
        }
    }

    #[test]
    fn test_increment() {
        let mut counter = DetailedCharacterCount::new();
        counter.increment(b'a');
        counter.increment(b'b');
        counter.increment(b'a'); // Increment 'a' again

        assert_eq!(counter.total(), 3);
        assert_eq!(counter.counts[b'a' as usize], 2);
        assert_eq!(counter.counts[b'b' as usize], 1);
    }

    #[test]
    fn test_increment_batch_unsafe_aligned() {
        let mut counter = DetailedCharacterCount::new();
        let buffer = vec![b'a'; 8]; // Exactly 8 bytes - tests aligned path
        counter.increment_batch_unsafe(&buffer);

        assert_eq!(counter.total(), 8);
        assert_eq!(counter.counts[b'a' as usize], 8);
    }

    #[test]
    fn test_increment_batch_unsafe_unaligned() {
        let mut counter = DetailedCharacterCount::new();
        let buffer = vec![b'x'; 5]; // 5 bytes - tests unaligned path only
        counter.increment_batch_unsafe(&buffer);

        assert_eq!(counter.total(), 5);
        assert_eq!(counter.counts[b'x' as usize], 5);
    }

    #[test]
    fn test_increment_batch_unsafe_mixed() {
        let mut counter = DetailedCharacterCount::new();
        // 30 bytes: tests both aligned (24) and unaligned (6) paths
        let buffer: Vec<u8> = (b'A'..=b'Z').chain(b'a'..=b'd').collect();
        counter.increment_batch_unsafe(&buffer);

        assert_eq!(counter.total(), 30);
        assert_eq!(counter.counts[b'A' as usize], 1);
        assert_eq!(counter.counts[b'Z' as usize], 1);
        assert_eq!(counter.counts[b'a' as usize], 1);
        assert_eq!(counter.counts[b'd' as usize], 1);
    }

    #[test]
    fn test_increment_batch_unsafe_empty() {
        let mut counter = DetailedCharacterCount::new();
        counter.increment_batch_unsafe(&[]);
        assert_eq!(counter.total(), 0);
    }

    #[test]
    fn test_increment_batch_unsafe_all_sizes() {
        // Test sizes 1-15 to cover all possible alignments
        for size in 1..=15 {
            let mut counter = DetailedCharacterCount::new();
            let buffer = vec![b'z'; size];
            counter.increment_batch_unsafe(&buffer);
            assert_eq!(counter.total(), size as u64);
            assert_eq!(counter.counts[b'z' as usize], size as u64);
        }
    }

    #[test]
    fn test_total_excluding_newlines() {
        let mut counter = DetailedCharacterCount::new();
        counter.increment(b'a');
        counter.increment(b'b');
        counter.increment(b'\n'); // newline - should be excluded
        counter.increment(b'\r'); // carriage return - should be excluded
        counter.increment(b'c');

        assert_eq!(counter.total(), 5);
        assert_eq!(counter.total_excluding_newlines(), 3); // Only a, b, c
    }

    #[test]
    fn test_add() {
        let mut counter1 = DetailedCharacterCount::new();
        counter1.increment(b'a');
        counter1.increment(b'b');

        let mut counter2 = DetailedCharacterCount::new();
        counter2.increment(b'a');
        counter2.increment(b'c');

        counter1.add(&counter2);

        assert_eq!(counter1.counts[b'a' as usize], 2);
        assert_eq!(counter1.counts[b'b' as usize], 1);
        assert_eq!(counter1.counts[b'c' as usize], 1);
        assert_eq!(counter1.total(), 4);
    }

    #[test]
    fn test_add_assign() {
        let mut counter1 = DetailedCharacterCount::new();
        counter1.increment(b'x');

        let mut counter2 = DetailedCharacterCount::new();
        counter2.increment(b'y');

        counter1 += &counter2;

        assert_eq!(counter1.counts[b'x' as usize], 1);
        assert_eq!(counter1.counts[b'y' as usize], 1);
        assert_eq!(counter1.total(), 2);
    }

    #[test]
    fn test_compare_equal() {
        let mut counter1 = DetailedCharacterCount::new();
        counter1.increment(b'a');
        counter1.increment(b'b');

        let mut counter2 = DetailedCharacterCount::new();
        counter2.increment(b'a');
        counter2.increment(b'b');

        let (is_equal, differences) = counter1.compare(&counter2);
        assert_eq!(is_equal, true);
        assert_eq!(differences.len(), 0);
    }

    #[test]
    fn test_compare_different() {
        let mut counter1 = DetailedCharacterCount::new();
        counter1.increment(b'a');
        counter1.increment(b'b');

        let mut counter2 = DetailedCharacterCount::new();
        counter2.increment(b'a');
        counter2.increment(b'c');

        let (is_equal, differences) = counter1.compare(&counter2);
        assert_eq!(is_equal, false);
        assert_eq!(differences.len(), 2);
        assert_eq!(differences[&(b'b' as usize)], 1); // counter1 has 1 more 'b'
        assert_eq!(differences[&(b'c' as usize)], -1); // counter1 has 1 less 'c'
    }

    #[test]
    fn test_compare_ignores_newlines() {
        let mut counter1 = DetailedCharacterCount::new();
        counter1.increment(b'x');
        counter1.increment(b'\n'); // Should be ignored
        counter1.increment(b'\r'); // Should be ignored

        let mut counter2 = DetailedCharacterCount::new();
        counter2.increment(b'x');

        let (is_equal, differences) = counter1.compare(&counter2);
        assert_eq!(is_equal, true);
        assert_eq!(differences.len(), 0);
    }

    #[test]
    fn test_consistency_increment_vs_batch() {
        let test_data = b"Hello, World! 123\n\r";

        let mut counter1 = DetailedCharacterCount::new();
        for &byte in test_data {
            counter1.increment(byte);
        }

        let mut counter2 = DetailedCharacterCount::new();
        counter2.increment_batch_unsafe(test_data);

        let (is_equal, differences) = counter1.compare(&counter2);
        assert_eq!(is_equal, true);
        assert_eq!(differences.len(), 0);
        assert_eq!(counter1.total(), counter2.total());
    }

    #[test]
    fn test_overflow_wrapping() {
        let mut counter = DetailedCharacterCount::new();
        counter.counts[b'a' as usize] = u64::MAX;
        counter.increment(b'a'); // Should wrap to 0
        assert_eq!(counter.counts[b'a' as usize], 0);
    }

    #[test]
    fn test_display() {
        let mut counter = DetailedCharacterCount::new();
        counter.increment(b'a');
        counter.increment(b'\n');

        let display_string = format!("{}", counter);
        assert!(display_string.contains("Total characters: 2"));
        assert!(display_string.contains("excluding newlines: 1"));
    }

    #[test]
    fn test_all_bytes() {
        let mut counter = DetailedCharacterCount::new();
        let all_bytes: Vec<u8> = (0..=255).collect();
        counter.increment_batch_unsafe(&all_bytes);

        // Each byte should have count of 1
        for i in 0..=255 {
            assert_eq!(counter.counts[i], 1);
        }
        assert_eq!(counter.total(), 256);
        assert_eq!(counter.total_excluding_newlines(), 254); // 256 - 2 (newline and CR)
    }

    #[test]
    fn test_performance_alignment() {
        // Test different data alignments to ensure unsafe code works correctly
        for offset in 0..8 {
            let mut data = vec![0u8; offset];
            data.extend_from_slice(b"abcdefghijklmnop"); // 16 bytes after offset

            let mut counter = DetailedCharacterCount::new();
            counter.increment_batch_unsafe(&data[offset..]);
            assert_eq!(counter.total(), 16);
        }
    }
}
