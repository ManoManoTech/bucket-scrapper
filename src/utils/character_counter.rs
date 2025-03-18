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
    pub fn new() -> Self {
        Self { bucket: "".to_string(), prefix: "".to_string(), counts: [0; 256] }
    }

    /// Increments the count for a specific byte
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
    pub fn total(&self) -> u64 {
        self.counts.iter().fold(0, |acc, &c| acc.wrapping_add(c))
    }

    /// Compare with another DetailedCharacterCount, returning differences
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
impl AddAssign for DetailedCharacterCount {
    fn add_assign(&mut self, other: Self) {
        self.add(&other);
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
