// src/utils/date.rs
use crate::config::types::{DateString, HourString};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct DateHour {
    pub date: DateString,
    pub hour: HourString,
}

/// Formats date as YYYYMMDD
///
/// # Examples
///
/// ```
/// use chrono::{DateTime, Utc, TimeZone};
/// use bucket_scrapper::utils::date::format_date;
///
/// let date = Utc.with_ymd_and_hms(2023, 12, 25, 14, 30, 0).unwrap();
/// assert_eq!(format_date(&date), "20231225");
/// ```
pub fn format_date(date: &DateTime<Utc>) -> DateString {
    date.format("%Y%m%d").to_string()
}

/// Formats hour as HH
///
/// # Examples
///
/// ```
/// use chrono::{DateTime, Utc, TimeZone};
/// use bucket_scrapper::utils::date::format_hour;
///
/// let date = Utc.with_ymd_and_hms(2023, 12, 25, 14, 30, 0).unwrap();
/// assert_eq!(format_hour(&date), "14");
///
/// let date_zero = Utc.with_ymd_and_hms(2023, 12, 25, 0, 30, 0).unwrap();
/// assert_eq!(format_hour(&date_zero), "00");
/// ```
pub fn format_hour(date: &DateTime<Utc>) -> HourString {
    date.format("%H").to_string()
}

/// Converts a DateTime to a DateHour struct
///
/// # Examples
///
/// ```
/// use chrono::{DateTime, Utc, TimeZone};
/// use bucket_scrapper::utils::date::date_to_date_hour;
///
/// let date = Utc.with_ymd_and_hms(2023, 12, 25, 14, 30, 0).unwrap();
/// let date_hour = date_to_date_hour(&date);
/// assert_eq!(date_hour.date, "20231225");
/// assert_eq!(date_hour.hour, "14");
/// ```
pub fn date_to_date_hour(date: &DateTime<Utc>) -> DateHour {
    DateHour {
        date: format_date(date),
        hour: format_hour(date),
    }
}

/// Returns a list of DateHour structs between start and end
///
/// # Examples
///
/// ```
/// use chrono::{DateTime, Utc, TimeZone};
/// use bucket_scrapper::utils::date::date_range_to_date_hour_list;
///
/// let start = Utc.with_ymd_and_hms(2023, 12, 25, 14, 0, 0).unwrap();
/// let end = Utc.with_ymd_and_hms(2023, 12, 25, 16, 0, 0).unwrap();
/// let result = date_range_to_date_hour_list(&start, &end).unwrap();
///
/// assert_eq!(result.len(), 3);
/// assert_eq!(result[0].date, "20231225");
/// assert_eq!(result[0].hour, "14");
/// assert_eq!(result[1].hour, "15");
/// assert_eq!(result[2].hour, "16");
///
/// // Test error case: start after end
/// let later = Utc.with_ymd_and_hms(2023, 12, 26, 14, 0, 0).unwrap();
/// assert!(date_range_to_date_hour_list(&later, &start).is_err());
/// ```
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
        current_date += Duration::hours(1);
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
///
/// # Examples
///
/// ```
/// use bucket_scrapper::utils::date::common_date_format;
///
/// let date = "20231225".to_string();
/// let hour = "14".to_string();
/// assert_eq!(common_date_format(&date, &hour), "dt=20231225/hour=14");
/// ```
pub fn common_date_format(date: &DateString, hour: &HourString) -> String {
    format!("dt={date}/hour={hour}")
}

/// Formats date and hour as YYYY/MM/DD/HH
///
/// # Examples
///
/// ```
/// use bucket_scrapper::utils::date::raw_logs_date_format;
///
/// let date = "20231225".to_string();
/// let hour = "14".to_string();
/// assert_eq!(raw_logs_date_format(&date, &hour).unwrap(), "2023/12/25/14");
///
/// // Test error case: invalid date format
/// let invalid_date = "2023125".to_string(); // 7 characters instead of 8
/// assert!(raw_logs_date_format(&invalid_date, &hour).is_err());
/// ```
pub fn raw_logs_date_format(date: &DateString, hour: &HourString) -> Result<String> {
    if date.len() != 8 {
        return Err(anyhow::anyhow!(
            "Invalid date: {date}. Should be YYYYMMDD, 8 characters."
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_format_date() {
        let date = Utc.with_ymd_and_hms(2023, 12, 25, 14, 30, 0).unwrap();
        assert_eq!(format_date(&date), "20231225");

        let date2 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(format_date(&date2), "20240101");
    }

    #[test]
    fn test_format_hour() {
        let date = Utc.with_ymd_and_hms(2023, 12, 25, 14, 30, 0).unwrap();
        assert_eq!(format_hour(&date), "14");

        let date_midnight = Utc.with_ymd_and_hms(2023, 12, 25, 0, 30, 0).unwrap();
        assert_eq!(format_hour(&date_midnight), "00");

        let date_evening = Utc.with_ymd_and_hms(2023, 12, 25, 23, 30, 0).unwrap();
        assert_eq!(format_hour(&date_evening), "23");
    }

    #[test]
    fn test_date_to_date_hour() {
        let date = Utc.with_ymd_and_hms(2023, 12, 25, 14, 30, 0).unwrap();
        let date_hour = date_to_date_hour(&date);
        assert_eq!(date_hour.date, "20231225");
        assert_eq!(date_hour.hour, "14");
    }

    #[test]
    fn test_date_range_to_date_hour_list_success() {
        let start = Utc.with_ymd_and_hms(2023, 12, 25, 14, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 25, 16, 0, 0).unwrap();
        let result = date_range_to_date_hour_list(&start, &end).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].date, "20231225");
        assert_eq!(result[0].hour, "14");
        assert_eq!(result[1].hour, "15");
        assert_eq!(result[2].hour, "16");
    }

    #[test]
    fn test_date_range_to_date_hour_list_single_hour() {
        let start = Utc.with_ymd_and_hms(2023, 12, 25, 14, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 25, 14, 0, 0).unwrap();
        let result = date_range_to_date_hour_list(&start, &end).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].date, "20231225");
        assert_eq!(result[0].hour, "14");
    }

    #[test]
    fn test_date_range_to_date_hour_list_cross_day() {
        let start = Utc.with_ymd_and_hms(2023, 12, 25, 23, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 26, 1, 0, 0).unwrap();
        let result = date_range_to_date_hour_list(&start, &end).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].date, "20231225");
        assert_eq!(result[0].hour, "23");
        assert_eq!(result[1].date, "20231226");
        assert_eq!(result[1].hour, "00");
        assert_eq!(result[2].date, "20231226");
        assert_eq!(result[2].hour, "01");
    }

    #[test]
    fn test_date_range_to_date_hour_list_error_start_after_end() {
        let start = Utc.with_ymd_and_hms(2023, 12, 26, 14, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 25, 14, 0, 0).unwrap();
        let result = date_range_to_date_hour_list(&start, &end);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Start date is after end date"));
    }

    #[test]
    fn test_common_date_format() {
        let date = "20231225".to_string();
        let hour = "14".to_string();
        assert_eq!(common_date_format(&date, &hour), "dt=20231225/hour=14");

        let date2 = "20240101".to_string();
        let hour2 = "00".to_string();
        assert_eq!(common_date_format(&date2, &hour2), "dt=20240101/hour=00");
    }

    #[test]
    fn test_raw_logs_date_format_success() {
        let date = "20231225".to_string();
        let hour = "14".to_string();
        assert_eq!(raw_logs_date_format(&date, &hour).unwrap(), "2023/12/25/14");

        let date2 = "20240101".to_string();
        let hour2 = "00".to_string();
        assert_eq!(
            raw_logs_date_format(&date2, &hour2).unwrap(),
            "2024/01/01/00"
        );
    }

    #[test]
    fn test_raw_logs_date_format_error_invalid_date() {
        let invalid_date = "2023125".to_string(); // 7 characters instead of 8
        let hour = "14".to_string();
        let result = raw_logs_date_format(&invalid_date, &hour);

        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("Invalid date"));
        assert!(error_message.contains("Should be YYYYMMDD, 8 characters"));
    }

    #[test]
    fn test_raw_logs_date_format_error_too_long() {
        let invalid_date = "202312255".to_string(); // 9 characters
        let hour = "14".to_string();
        let result = raw_logs_date_format(&invalid_date, &hour);

        assert!(result.is_err());
    }

    #[test]
    fn test_raw_logs_date_format_error_empty() {
        let invalid_date = "".to_string();
        let hour = "14".to_string();
        let result = raw_logs_date_format(&invalid_date, &hour);

        assert!(result.is_err());
    }

    #[test]
    fn test_empty_date_format() {
        let date = "20231225".to_string();
        let hour = "14".to_string();
        assert_eq!(empty_date_format(&date, &hour), "");

        // Should work with any input
        let empty_date = "".to_string();
        let empty_hour = "".to_string();
        assert_eq!(empty_date_format(&empty_date, &empty_hour), "");
    }
}
