// src/continuous.rs
//! Continuous mode for automatic target hour selection.
//!
//! When the `check` command is run without explicit `--date` and `--hour` arguments,
//! this module selects a target hour to check based on the `continuousConsolidation`
//! config section.

use crate::config::loader::{get_archived_buckets, get_consolidated_buckets, get_results_bucket};
use crate::config::types::{BucketConfig, ConfigSchema, ContinuousConsolidationConfig};
use crate::s3::client::WrappedS3Client;
use crate::utils::date::{date_to_date_hour, DateHour};
use crate::utils::duration_parser::{parse_duration, to_chrono_duration};
use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Duration, Timelike, Utc};
use futures::{stream, StreamExt};
use tracing::{debug, error, info, warn};

/// Status of a candidate hour for checking
#[derive(Debug, Clone)]
pub enum CandidateStatus {
    /// Both archived and consolidated have data, no result exists - should check
    NeedsCheck,
    /// Both have data, result exists but failed - should retry
    NeedsRetry { previous_message: String },
    /// Result exists and passed - skip
    AlreadyPassed,
    /// No archived data - nothing to check
    NoArchivedData,
    /// No consolidated data - consolidation not done yet
    NoConsolidatedData,
    /// Error during status check
    Error(String),
}

/// Result of scanning for a target hour
#[derive(Debug)]
pub struct TargetSelectionResult {
    pub selected: Option<DateHour>,
    pub reason: String,
    pub candidates_scanned: usize,
}

/// Generate list of candidate hours for continuous mode checking.
/// Returns hours from (now - max_age) to (now - min_age), oldest first.
pub fn generate_candidate_hours(
    config: &ContinuousConsolidationConfig,
    now: DateTime<Utc>,
) -> Result<Vec<DateHour>> {
    let min_age = parse_duration(&config.min_age)?;
    let max_age = parse_duration(&config.max_age)?;

    // Calculate time boundaries
    // newest_eligible: now - min_age (most recent hour we can check)
    // oldest_eligible: now - max_age (oldest hour we consider)
    let newest_eligible = now - to_chrono_duration(min_age);
    let oldest_eligible = now - to_chrono_duration(max_age);

    // Truncate to hour boundaries
    let newest_hour = truncate_to_hour(newest_eligible);
    let oldest_hour = truncate_to_hour(oldest_eligible);

    if oldest_hour > newest_hour {
        return Err(anyhow::anyhow!(
            "Invalid time range: oldest ({}) is after newest ({}). Check minAge/maxAge config.",
            oldest_hour,
            newest_hour
        ));
    }

    // Generate list from oldest to newest
    let mut candidates = Vec::new();
    let mut current = oldest_hour;

    while current <= newest_hour {
        candidates.push(date_to_date_hour(&current));
        current = current + Duration::hours(1);
    }

    Ok(candidates)
}

/// Truncate a DateTime to the start of its hour
fn truncate_to_hour(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(dt)
}

/// Select a target hour for continuous mode checking.
///
/// Iterates through candidates from oldest to newest, checking each one's status.
/// Returns the first candidate that needs checking (no result) or retrying (failed result).
pub async fn select_target_hour(
    config: &ConfigSchema,
    s3_client: &WrappedS3Client,
    candidates: Vec<DateHour>,
    batch_size: usize,
) -> Result<TargetSelectionResult> {
    let client = s3_client.get_client().await?;

    let archived_buckets = get_archived_buckets(config);
    let consolidated_buckets = get_consolidated_buckets(config);
    let results_bucket = get_results_bucket(config);

    let consolidated_bucket = consolidated_buckets
        .first()
        .ok_or_else(|| anyhow::anyhow!("No consolidated bucket configured"))?;

    let results_bucket =
        results_bucket.ok_or_else(|| anyhow::anyhow!("No results bucket configured"))?;

    info!(
        total_candidates = candidates.len(),
        batch_size = batch_size,
        "Starting target hour selection"
    );

    let total_candidates = candidates.len();
    let mut scanned = 0;

    // Process candidates in batches from oldest to newest
    for (batch_idx, batch) in candidates.chunks(batch_size).enumerate() {
        debug!(
            batch = batch_idx,
            batch_size = batch.len(),
            "Checking candidate batch"
        );

        // Check all candidates in this batch in parallel
        let statuses: Vec<(DateHour, CandidateStatus)> = stream::iter(batch.iter().cloned())
            .map(|candidate| {
                let client = &client;
                let s3_client = s3_client;
                let archived_buckets = &archived_buckets;
                let consolidated_bucket = *consolidated_bucket;
                let results_bucket = results_bucket;

                async move {
                    let status = check_candidate_status(
                        client,
                        s3_client,
                        archived_buckets,
                        consolidated_bucket,
                        results_bucket,
                        &candidate.date,
                        &candidate.hour,
                    )
                    .await;

                    (candidate, status)
                }
            })
            .buffer_unordered(batch_size)
            .collect()
            .await;

        // Sort results by date/hour (buffer_unordered may return out of order)
        let mut sorted_statuses = statuses;
        sorted_statuses.sort_by(|a, b| (&a.0.date, &a.0.hour).cmp(&(&b.0.date, &b.0.hour)));

        // Process in order (oldest first within batch)
        for (candidate, status) in sorted_statuses {
            scanned += 1;

            match status {
                CandidateStatus::NeedsCheck => {
                    info!(
                        date = %candidate.date,
                        hour = %candidate.hour,
                        "Selected target: needs initial check"
                    );
                    return Ok(TargetSelectionResult {
                        selected: Some(candidate),
                        reason: "No previous check result exists".to_string(),
                        candidates_scanned: scanned,
                    });
                }
                CandidateStatus::NeedsRetry { previous_message } => {
                    info!(
                        date = %candidate.date,
                        hour = %candidate.hour,
                        previous_message = %previous_message,
                        "Selected target: needs retry (previous check failed)"
                    );
                    return Ok(TargetSelectionResult {
                        selected: Some(candidate),
                        reason: format!("Previous check failed: {}", previous_message),
                        candidates_scanned: scanned,
                    });
                }
                CandidateStatus::AlreadyPassed => {
                    debug!(
                        date = %candidate.date,
                        hour = %candidate.hour,
                        "Skipping: already passed"
                    );
                }
                CandidateStatus::NoArchivedData => {
                    debug!(
                        date = %candidate.date,
                        hour = %candidate.hour,
                        "Skipping: no archived data"
                    );
                }
                CandidateStatus::NoConsolidatedData => {
                    debug!(
                        date = %candidate.date,
                        hour = %candidate.hour,
                        "Skipping: no consolidated data"
                    );
                }
                CandidateStatus::Error(e) => {
                    error!(
                        date = %candidate.date,
                        hour = %candidate.hour,
                        error = %e,
                        "Error checking candidate status - aborting"
                    );
                    return Err(anyhow::anyhow!(
                        "Failed to check candidate {}/{}: {}",
                        candidate.date,
                        candidate.hour,
                        e
                    ));
                }
            }
        }
    }

    Ok(TargetSelectionResult {
        selected: None,
        reason: "All candidate hours already checked and passed".to_string(),
        candidates_scanned: total_candidates,
    })
}

/// Check the status of a single candidate hour
async fn check_candidate_status(
    client: &Client,
    s3_client: &WrappedS3Client,
    archived_buckets: &[&BucketConfig],
    consolidated_bucket: &BucketConfig,
    results_bucket: &BucketConfig,
    date: &String,
    hour: &String,
) -> CandidateStatus {
    // Step 1: Check if ANY archived bucket has files
    let has_archived = match check_any_bucket_has_files(client, s3_client, archived_buckets, date, hour).await {
        Ok(has) => has,
        Err(e) => return CandidateStatus::Error(format!("Error checking archived buckets: {}", e)),
    };

    if !has_archived {
        return CandidateStatus::NoArchivedData;
    }

    // Step 2: Check if consolidated bucket has files
    let has_consolidated = match check_bucket_has_files(client, s3_client, consolidated_bucket, date, hour).await {
        Ok(has) => has,
        Err(e) => return CandidateStatus::Error(format!("Error checking consolidated bucket: {}", e)),
    };

    if !has_consolidated {
        return CandidateStatus::NoConsolidatedData;
    }

    // Step 3: Both have data - check results bucket
    match check_result_status(client, s3_client, results_bucket, date, hour).await {
        Ok(None) => CandidateStatus::NeedsCheck,
        Ok(Some((true, _))) => CandidateStatus::AlreadyPassed,
        Ok(Some((false, msg))) => CandidateStatus::NeedsRetry {
            previous_message: msg,
        },
        Err(e) => CandidateStatus::Error(format!("Error checking result: {}", e)),
    }
}

/// Check if ANY of the archived buckets has files for the given date/hour
async fn check_any_bucket_has_files(
    client: &Client,
    s3_client: &WrappedS3Client,
    buckets: &[&BucketConfig],
    date: &String,
    hour: &String,
) -> Result<bool> {
    for bucket in buckets {
        let file_list = s3_client
            .get_matching_filenames_from_s3_with_client(client, bucket, date, hour, false)
            .await?;

        if !file_list.files.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Check if a single bucket has files for the given date/hour
async fn check_bucket_has_files(
    client: &Client,
    s3_client: &WrappedS3Client,
    bucket: &BucketConfig,
    date: &String,
    hour: &String,
) -> Result<bool> {
    let file_list = s3_client
        .get_matching_filenames_from_s3_with_client(client, bucket, date, hour, false)
        .await?;

    Ok(!file_list.files.is_empty())
}

/// Check the result status for a given date/hour.
/// Returns: Ok(None) if no result, Ok(Some((ok, message))) if result exists
async fn check_result_status(
    client: &Client,
    s3_client: &WrappedS3Client,
    results_bucket: &BucketConfig,
    date: &String,
    hour: &String,
) -> Result<Option<(bool, String)>> {
    let file_list = s3_client
        .get_matching_filenames_from_s3_with_client(client, results_bucket, date, hour, false)
        .await?;

    if file_list.files.is_empty() {
        return Ok(None);
    }

    // Download the last result file (check_result.json comes last lexicographically)
    if let Some(file) = file_list.files.last() {
        let bytes = s3_client
            .download_object_with_client(client, &file.bucket, &file.key)
            .await?;

        let json: serde_json::Value = serde_json::from_slice(&bytes)?;

        let ok = json.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);

        let message = json
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        return Ok(Some((ok, message)));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_truncate_to_hour() {
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 35, 42).unwrap();
        let truncated = truncate_to_hour(dt);
        assert_eq!(truncated.hour(), 14);
        assert_eq!(truncated.minute(), 0);
        assert_eq!(truncated.second(), 0);
    }

    #[test]
    fn test_generate_candidate_hours_basic() {
        let config = ContinuousConsolidationConfig {
            min_age: "2h".to_string(),
            max_age: "5h".to_string(),
            step: "1h".to_string(),
        };

        // Use a fixed time for reproducible tests
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 12, 30, 0).unwrap();

        let candidates = generate_candidate_hours(&config, now).unwrap();

        // max_age = 5h -> oldest = 12:30 - 5h = 07:30 -> truncated to 07:00
        // min_age = 2h -> newest = 12:30 - 2h = 10:30 -> truncated to 10:00
        // So we expect hours: 07, 08, 09, 10 = 4 candidates
        assert_eq!(candidates.len(), 4);

        // Verify oldest first
        assert_eq!(candidates[0].hour, "07");
        assert_eq!(candidates[3].hour, "10");
    }

    #[test]
    fn test_generate_candidate_hours_cross_day() {
        let config = ContinuousConsolidationConfig {
            min_age: "1h".to_string(),
            max_age: "3h".to_string(),
            step: "1h".to_string(),
        };

        // Time just after midnight
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 1, 30, 0).unwrap();

        let candidates = generate_candidate_hours(&config, now).unwrap();

        // max_age = 3h -> oldest = 01:30 - 3h = 22:30 (Jan 14) -> 22:00
        // min_age = 1h -> newest = 01:30 - 1h = 00:30 (Jan 15) -> 00:00
        // Hours: 22, 23, 00 = 3 candidates
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0].date, "20250114");
        assert_eq!(candidates[0].hour, "22");
        assert_eq!(candidates[2].date, "20250115");
        assert_eq!(candidates[2].hour, "00");
    }

    #[test]
    fn test_generate_candidate_hours_single_hour() {
        let config = ContinuousConsolidationConfig {
            min_age: "2h".to_string(),
            max_age: "2h".to_string(),
            step: "1h".to_string(),
        };

        let now = Utc.with_ymd_and_hms(2025, 1, 15, 12, 30, 0).unwrap();

        let candidates = generate_candidate_hours(&config, now).unwrap();

        // Both min and max point to same hour
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].hour, "10");
    }

    #[test]
    fn test_generate_candidate_hours_invalid_duration() {
        let config = ContinuousConsolidationConfig {
            min_age: "invalid".to_string(),
            max_age: "5h".to_string(),
            step: "1h".to_string(),
        };

        let now = Utc::now();
        let result = generate_candidate_hours(&config, now);
        assert!(result.is_err());
    }
}
