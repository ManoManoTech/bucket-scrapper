// src/utils/date.rs
use crate::config::types::{DateString, HourString};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use regex::Regex;

pub struct DateHour {
    pub date: DateString,
    pub hour: HourString,
}

/// Formats date as YYYYMMDD
pub fn format_date(date: &DateTime<Utc>) -> DateString {
    date.format("%Y%m%d").to_string()
}

/// Formats hour as HH
pub fn format_hour(date: &DateTime<Utc>) -> HourString {
    date.format("%H").to_string()
}

/// Converts a DateTime to a DateHour struct
pub fn date_to_date_hour(date: &DateTime<Utc>) -> DateHour {
    DateHour {
        date: format_date(date),
        hour: format_hour(date),
    }
}

/// Returns a list of DateHour structs between start and end
pub fn date_range_to_date_hour_list(
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Result<Vec<DateHour>> {
    if start > end {
        return Err(anyhow::anyhow!("Start date is after end date"));
    }

    let mut date_hour_list = Vec::new();
    let mut current_date = *start;

    while current_date <= *end {
        date_hour_list.push(date_to_date_hour(&current_date));
        current_date = current_date + Duration::hours(1);
    }

    if date_hour_list.is_empty() {
        return Err(anyhow::anyhow!(
            "No DateHour found ({} to {})",
            start.to_rfc3339(),
            end.to_rfc3339()
        ));
    }

    Ok(date_hour_list)
}

/// Formats date and hour as dt=YYYYMMDD/hour=HH
pub fn common_date_format(date: &DateString, hour: &HourString) -> String {
    format!("dt={}/hour={}", date, hour)
}

/// Formats date and hour as YYYY/MM/DD/HH
pub fn raw_logs_date_format(date: &DateString, hour: &HourString) -> Result<String> {
    if date.len() != 8 {
        return Err(anyhow::anyhow!(
            "Invalid date: {}. Should be YYYYMMDD, 8 characters.",
            date
        ));
    }

    Ok(format!(
        "{}/{}/{}/{}",
        &date[0..4],
        &date[4..6],
        &date[6..8],
        hour
    ))
}

/// Returns an empty string (used for some edge cases)
pub fn empty_date_format(_date: &DateString, _hour: &HourString) -> String {
    String::new()
}
