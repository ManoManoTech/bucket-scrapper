// src/utils/duration_parser.rs
//! Parser for duration strings like "70m", "528h", "22d"

use anyhow::{anyhow, Result};
use std::time::Duration;

/// Parse a duration string like "70m", "528h", or "22d" into a std::time::Duration.
///
/// Supported units:
/// - `m` - minutes
/// - `h` - hours
/// - `d` - days
///
/// # Examples
/// ```
/// use log_consolidator_checker_rust::utils::duration_parser::parse_duration;
///
/// let d = parse_duration("70m").unwrap();
/// assert_eq!(d.as_secs(), 70 * 60);
///
/// let d = parse_duration("528h").unwrap();
/// assert_eq!(d.as_secs(), 528 * 60 * 60);
/// ```
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("Empty duration string"));
    }

    // Find where the numeric part ends
    let (value_str, unit) = s
        .chars()
        .position(|c| !c.is_ascii_digit())
        .map(|pos| s.split_at(pos))
        .ok_or_else(|| anyhow!("Invalid duration format: no unit found in '{}'", s))?;

    if value_str.is_empty() {
        return Err(anyhow!(
            "Invalid duration format: no numeric value in '{}'",
            s
        ));
    }

    let value: u64 = value_str
        .parse()
        .map_err(|_| anyhow!("Invalid duration value: '{}'", value_str))?;

    let duration = match unit {
        "m" => Duration::from_secs(value * 60),
        "h" => Duration::from_secs(value * 60 * 60),
        "d" => Duration::from_secs(value * 24 * 60 * 60),
        _ => {
            return Err(anyhow!(
                "Unknown duration unit: '{}'. Use 'm' (minutes), 'h' (hours), or 'd' (days)",
                unit
            ))
        }
    };

    Ok(duration)
}

/// Convert std::time::Duration to chrono::Duration
pub fn to_chrono_duration(d: Duration) -> chrono::Duration {
    chrono::Duration::seconds(d.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minutes() {
        let d = parse_duration("70m").unwrap();
        assert_eq!(d.as_secs(), 70 * 60);
    }

    #[test]
    fn test_parse_hours() {
        let d = parse_duration("528h").unwrap();
        assert_eq!(d.as_secs(), 528 * 60 * 60);
    }

    #[test]
    fn test_parse_days() {
        let d = parse_duration("22d").unwrap();
        assert_eq!(d.as_secs(), 22 * 24 * 60 * 60);
    }

    #[test]
    fn test_parse_with_whitespace() {
        let d = parse_duration("  100h  ").unwrap();
        assert_eq!(d.as_secs(), 100 * 60 * 60);
    }

    #[test]
    fn test_invalid_unit() {
        let result = parse_duration("100x");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown duration unit"));
    }

    #[test]
    fn test_empty_string() {
        let result = parse_duration("");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Empty duration string"));
    }

    #[test]
    fn test_no_unit() {
        let result = parse_duration("100");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no unit found"));
    }

    #[test]
    fn test_no_value() {
        let result = parse_duration("h");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no numeric value"));
    }

    #[test]
    fn test_to_chrono_duration() {
        let std_duration = Duration::from_secs(3600);
        let chrono_duration = to_chrono_duration(std_duration);
        assert_eq!(chrono_duration.num_seconds(), 3600);
    }
}
