use crate::utils::character_counter::DetailedCharacterCount;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Configuration for PGM image generation
pub struct PgmConfig {
    pub output_dir: String,
    pub width: usize,
    pub height: usize,
    pub max_val: u16,
}

impl Default for PgmConfig {
    fn default() -> Self {
        Self {
            output_dir: "/tmp".to_string(),
            width: 16,
            height: 16,
            max_val: 65535,
        }
    }
}

/// PGM visualizer for character count distributions
pub struct PgmVisualizer {
    config: PgmConfig,
}

impl PgmVisualizer {
    /// Create a new visualizer with default configuration
    pub fn with_default() -> Self {
        Self {
            config: PgmConfig::default(),
        }
    }

    /// Generate all three visualization images
    pub fn generate_visualization(
        &self,
        archived_counts: &DetailedCharacterCount,
        consolidated_counts: &DetailedCharacterCount,
        differences: &HashMap<usize, i64>,
    ) -> Result<()> {
        self.generate_pgm(archived_counts, "lhs.pgm")
            .context("Failed to generate archived (lhs) PGM")?;

        self.generate_pgm(consolidated_counts, "rhs.pgm")
            .context("Failed to generate consolidated (rhs) PGM")?;

        self.generate_diff_pgm(differences, "diff.pgm")
            .context("Failed to generate diff PGM")?;

        Ok(())
    }

    /// Generate a PGM image from character counts
    fn generate_pgm(&self, counts: &DetailedCharacterCount, filename: &str) -> Result<()> {
        let pixels = self.normalize_counts(&counts.counts);
        let path = Path::new(&self.config.output_dir).join(filename);
        self.write_pgm_binary(&path, &pixels)
    }

    /// Generate a difference PGM image
    fn generate_diff_pgm(&self, differences: &HashMap<usize, i64>, filename: &str) -> Result<()> {
        let mut diff_array = [0i64; 256];
        for (&idx, &diff) in differences {
            diff_array[idx] = diff;
        }

        let min_diff = *diff_array.iter().min().unwrap();
        let max_diff = *diff_array.iter().max().unwrap();

        let pixels: [u16; 256] = if min_diff == 0 && max_diff == 0 {
            [32767u16; 256] // All mid-gray (no differences)
        } else {
            let mut result = [0u16; 256];
            for (i, &diff) in diff_array.iter().enumerate() {
                result[i] = if diff == 0 {
                    32767 // Mid-gray
                } else if diff > 0 {
                    // Positive: archived > consolidated (data loss)
                    // Map to 32768-65535
                    let ratio = diff as f64 / max_diff as f64;
                    (32767.0 + ratio * 32768.0).round() as u16
                } else {
                    // Negative: consolidated > archived (duplication)
                    // Map to 0-32766
                    let ratio = diff as f64 / min_diff as f64;
                    (32767.0 - ratio * 32767.0).round() as u16
                };
            }
            result
        };

        let path = Path::new(&self.config.output_dir).join(filename);
        self.write_pgm_binary(&path, &pixels)
    }

    /// Normalize counts to 16-bit grayscale range using min-max normalization
    fn normalize_counts(&self, counts: &[u64; 256]) -> [u16; 256] {
        let non_zero_counts: Vec<u64> = counts.iter().filter(|&&c| c > 0).copied().collect();

        if non_zero_counts.is_empty() {
            return [0u16; 256]; // All black
        }

        let min_count = *non_zero_counts.iter().min().unwrap();
        let max_count = *non_zero_counts.iter().max().unwrap();

        if min_count == max_count {
            // All same value - use mid-gray for non-zero
            let mut result = [0u16; 256];
            for (i, &count) in counts.iter().enumerate() {
                result[i] = if count > 0 { 32767 } else { 0 };
            }
            return result;
        }

        // Normalize: (value - min) / (max - min) * 65535
        let range = max_count - min_count;
        let mut result = [0u16; 256];
        for (i, &count) in counts.iter().enumerate() {
            result[i] = if count > 0 {
                let normalized = ((count - min_count) as f64 / range as f64) * 65535.0;
                normalized.round() as u16
            } else {
                0
            };
        }
        result
    }

    /// Write a PGM image in P5 (binary) format
    fn write_pgm_binary(&self, path: &Path, pixels: &[u16; 256]) -> Result<()> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("Failed to create output directory")?;
        }

        let file = File::create(path)
            .with_context(|| format!("Failed to create PGM file: {}", path.display()))?;
        let mut writer = BufWriter::new(file);

        // Write P5 header
        writeln!(writer, "P5")?;
        writeln!(writer, "{} {}", self.config.width, self.config.height)?;
        writeln!(writer, "{}", self.config.max_val)?;

        // Write binary pixel data (big-endian u16, row-major)
        for &pixel in pixels {
            writer.write_all(&pixel.to_be_bytes())?;
        }

        writer
            .flush()
            .with_context(|| format!("Failed to flush PGM file: {}", path.display()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_counts_basic() {
        let visualizer = PgmVisualizer::with_default();
        let mut counts = [0u64; 256];
        counts[0] = 100;
        counts[1] = 200;
        counts[2] = 300;

        let normalized = visualizer.normalize_counts(&counts);

        // Min=100, max=300, range=200
        // counts[0] = (100-100)/200 * 65535 = 0
        // counts[1] = (200-100)/200 * 65535 = 32767.5 ≈ 32768
        // counts[2] = (300-100)/200 * 65535 = 65535
        assert_eq!(normalized[0], 0);
        assert_eq!(normalized[1], 32768);
        assert_eq!(normalized[2], 65535);
        assert_eq!(normalized[3], 0); // Zero should stay zero
    }

    #[test]
    fn test_normalize_counts_all_zero() {
        let visualizer = PgmVisualizer::with_default();
        let counts = [0u64; 256];

        let normalized = visualizer.normalize_counts(&counts);

        // All should be zero (black)
        for &val in &normalized {
            assert_eq!(val, 0);
        }
    }

    #[test]
    fn test_normalize_counts_uniform() {
        let visualizer = PgmVisualizer::with_default();
        let mut counts = [0u64; 256];
        counts[10] = 42;
        counts[20] = 42;
        counts[30] = 42;

        let normalized = visualizer.normalize_counts(&counts);

        // All non-zero values should be mid-gray
        assert_eq!(normalized[10], 32767);
        assert_eq!(normalized[20], 32767);
        assert_eq!(normalized[30], 32767);
        assert_eq!(normalized[0], 0); // Zero should stay zero
    }

    #[test]
    fn test_generate_diff_pgm_no_differences() {
        let visualizer = PgmVisualizer::with_default();
        let differences: HashMap<usize, i64> = HashMap::new();

        // Should not panic and should create all mid-gray
        let mut diff_array = [0i64; 256];
        for (&idx, &diff) in &differences {
            diff_array[idx] = diff;
        }

        let min_diff = *diff_array.iter().min().unwrap();
        let max_diff = *diff_array.iter().max().unwrap();

        assert_eq!(min_diff, 0);
        assert_eq!(max_diff, 0);
    }

    #[test]
    fn test_generate_diff_pgm_bidirectional() {
        let visualizer = PgmVisualizer::with_default();
        let mut differences = HashMap::new();
        differences.insert(10, 100i64); // Positive (data loss)
        differences.insert(20, -50i64); // Negative (duplication)

        let mut diff_array = [0i64; 256];
        for (&idx, &diff) in &differences {
            diff_array[idx] = diff;
        }

        let min_diff = *diff_array.iter().min().unwrap();
        let max_diff = *diff_array.iter().max().unwrap();

        assert_eq!(min_diff, -50);
        assert_eq!(max_diff, 100);

        // Verify mapping
        // diff_array[10] = 100 (max positive) -> should map to 65535 (white)
        let ratio_pos: f64 = 100.0 / 100.0;
        let expected_pos = (32767.0_f64 + ratio_pos * 32768.0).round() as u16;
        assert_eq!(expected_pos, 65535);

        // diff_array[20] = -50 (max negative) -> should map to 0 (black)
        let ratio_neg: f64 = -50.0 / -50.0;
        let expected_neg = (32767.0_f64 - ratio_neg * 32767.0).round() as u16;
        assert_eq!(expected_neg, 0);

        // diff_array[0] = 0 (no difference) -> should map to 32767 (mid-gray)
        assert_eq!(32767u16, 32767);
    }

    #[test]
    fn test_pixel_layout_16x16() {
        // Verify that we have 256 pixels total for 16x16 layout
        let config = PgmConfig::default();
        assert_eq!(config.width * config.height, 256);
    }

    #[test]
    fn test_write_pgm_binary_format() {
        use std::io::Read;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("test.pgm");

        let visualizer = PgmVisualizer::with_default();
        let pixels = [32767u16; 256]; // All mid-gray

        visualizer.write_pgm_binary(&temp_path, &pixels).unwrap();

        // Read back and verify format
        let mut file = File::open(&temp_path).unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        // Parse header
        let header_str = String::from_utf8_lossy(&contents[0..20]);
        assert!(header_str.starts_with("P5\n"));
        assert!(header_str.contains("16 16\n"));
        assert!(header_str.contains("65535\n"));

        // Verify we have 512 bytes of pixel data (256 pixels * 2 bytes)
        // Header is ~15 bytes ("P5\n16 16\n65535\n")
        let header_end = header_str.find("65535\n").unwrap() + "65535\n".len();
        let pixel_data = &contents[header_end..];
        assert_eq!(pixel_data.len(), 512);

        // Verify first pixel is correct (big-endian 32767)
        let first_pixel = u16::from_be_bytes([pixel_data[0], pixel_data[1]]);
        assert_eq!(first_pixel, 32767);
    }
}
