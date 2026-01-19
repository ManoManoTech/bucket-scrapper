use std::collections::HashMap;
use std::collections::HashSet;

/// Summary of check results for a single day
pub struct DaySummary {
    /// Date in YYYY-MM-DD format
    pub date: String,
    /// Hours that failed the check (HH format)
    pub failed_hours: Vec<String>,
    /// Hours that have no check result (HH format)
    pub missing_hours: Vec<String>,
    /// Hours where input files were cleaned after consolidation
    pub cleaned_hours: Vec<String>,
    /// Hours with no files to consolidate (empty buckets - ok state)
    pub to_consolidate_hours: Vec<String>,
    /// Hours pending consolidation (archived files exist but no consolidated output)
    pub pending_hours: Vec<String>,
    /// Last check date (from analysis_end_date, formatted as date only)
    pub last_check_date: Option<String>,
    /// Total archived files for the day
    pub total_archived_files: usize,
    /// Total consolidated files for the day
    pub total_consolidated_files: usize,
    /// Total number of checks performed for the day (sum of check_count for all hours)
    pub total_checks: usize,
}

/// Result classification for an hour
#[derive(Clone, Debug)]
enum HourStatus {
    /// Check passed normally
    Ok,
    /// Check failed (mismatch between archived and consolidated)
    Failed,
    /// No input files but consolidated files exist (cleaned up after consolidation)
    Cleaned,
    /// No files to consolidate (empty buckets - valid state)
    ToConsolidate,
    /// Archived files exist but no consolidated output yet (pending consolidation)
    Pending,
}

/// Aggregate hourly results into daily summaries
pub fn aggregate_by_day(
    results: &HashMap<String, serde_json::Value>,
    missing_checks: &[(String, String)],
) -> Vec<DaySummary> {
    // Group results by date (YYYYMMDD) with check dates, status, file counts, and check count
    // Tuple: (hour, status, check_date, archived_files, consolidated_files, check_count)
    let mut by_date: HashMap<
        String,
        Vec<(String, HourStatus, Option<String>, usize, usize, usize)>,
    > = HashMap::new();

    for (key, value) in results {
        // Key format: "YYYYMMDD/HH"
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 2 {
            continue;
        }
        let date = parts[0];
        let hour = parts[1];
        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        let message = value.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let check_date = value
            .get("analysis_end_date")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let archived_files = value
            .get("archived_files_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let consolidated_files = value
            .get("consolidated_files_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let check_count = value
            .get("check_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as usize;

        // Classify the result based on message content
        let status = if message.contains("No input files found")
            && message.contains("consolidated files exist")
        {
            HourStatus::Cleaned
        } else if message.contains("No files to consolidate") {
            HourStatus::ToConsolidate
        } else if message.contains("Consolidation pending") {
            HourStatus::Pending
        } else if ok {
            HourStatus::Ok
        } else {
            HourStatus::Failed
        };

        by_date.entry(date.to_string()).or_default().push((
            hour.to_string(),
            status,
            check_date,
            archived_files,
            consolidated_files,
            check_count,
        ));
    }

    // Group missing checks by date
    let mut missing_by_date: HashMap<String, Vec<String>> = HashMap::new();
    for (date, hour) in missing_checks {
        missing_by_date
            .entry(date.clone())
            .or_default()
            .push(hour.clone());
    }

    // Collect all dates (from both results and missing)
    let all_dates: HashSet<String> = by_date
        .keys()
        .chain(missing_by_date.keys())
        .cloned()
        .collect();

    // Convert to DaySummary, sorted by date
    let mut summaries: Vec<DaySummary> = all_dates
        .into_iter()
        .map(|date| {
            let hours = by_date.get(&date).cloned().unwrap_or_default();
            let missing = missing_by_date.get(&date).cloned().unwrap_or_default();

            let failed_hours: Vec<String> = hours
                .iter()
                .filter(|(_, status, _, _, _, _)| matches!(status, HourStatus::Failed))
                .map(|(h, _, _, _, _, _)| h.clone())
                .collect();

            let cleaned_hours: Vec<String> = hours
                .iter()
                .filter(|(_, status, _, _, _, _)| matches!(status, HourStatus::Cleaned))
                .map(|(h, _, _, _, _, _)| h.clone())
                .collect();

            let to_consolidate_hours: Vec<String> = hours
                .iter()
                .filter(|(_, status, _, _, _, _)| matches!(status, HourStatus::ToConsolidate))
                .map(|(h, _, _, _, _, _)| h.clone())
                .collect();

            let pending_hours: Vec<String> = hours
                .iter()
                .filter(|(_, status, _, _, _, _)| matches!(status, HourStatus::Pending))
                .map(|(h, _, _, _, _, _)| h.clone())
                .collect();

            // Sum up file counts and check counts for the day
            let total_archived_files: usize = hours.iter().map(|(_, _, _, a, _, _)| a).sum();
            let total_consolidated_files: usize = hours.iter().map(|(_, _, _, _, c, _)| c).sum();
            let total_checks: usize = hours.iter().map(|(_, _, _, _, _, cc)| cc).sum();

            // Find the latest check date for this day
            let last_check_date = hours
                .iter()
                .filter_map(|(_, _, check_date, _, _, _)| check_date.as_ref())
                .max()
                .map(|s| {
                    // Extract just the date part from ISO 8601 (e.g., "2025-01-15T10:30:00Z" -> "01-15")
                    if s.len() >= 10 {
                        format!("{}", &s[5..10]) // MM-DD
                    } else {
                        s.clone()
                    }
                });

            // Convert YYYYMMDD to YYYY-MM-DD
            let formatted_date = if date.len() == 8 {
                format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8])
            } else {
                date
            };

            // Show missing hours regardless of whether other hours have results
            // This handles the case where some hours have check results but others are missing
            let checked_hours: HashSet<String> = hours.iter().map(|(h, _, _, _, _, _)| h.clone()).collect();
            let effective_missing: Vec<String> = missing
                .into_iter()
                .filter(|h| !checked_hours.contains(h))
                .collect();

            DaySummary {
                date: formatted_date,
                failed_hours,
                missing_hours: effective_missing,
                cleaned_hours,
                to_consolidate_hours,
                pending_hours,
                last_check_date,
                total_archived_files,
                total_consolidated_files,
                total_checks,
            }
        })
        .collect();

    summaries.sort_by(|a, b| a.date.cmp(&b.date));
    summaries
}

/// Generate HTML recap report
pub fn generate_recap_html(summaries: &[DaySummary]) -> String {
    let mut boxes = String::new();

    for summary in summaries {
        let failed_count = summary.failed_hours.len();
        let missing_count = summary.missing_hours.len();
        let cleaned_count = summary.cleaned_hours.len();
        let to_consolidate_count = summary.to_consolidate_hours.len();
        let pending_count = summary.pending_hours.len();

        // Determine class and status based on priority: pending > cleaned > mixed > unchecked > failed > to_consolidate > ok
        let (class, status_text) = if pending_count > 0 {
            // Pending consolidation (archived files exist but no consolidated output)
            match pending_count {
                1 => {
                    let hour = &summary.pending_hours[0];
                    ("pending", format!("PEND,H={}", hour))
                }
                n => ("pending", format!("{}/24 PEND", n)),
            }
        } else if cleaned_count > 0 {
            // Cleaned (consolidated files exist but input files were cleaned up)
            match cleaned_count {
                1 => {
                    let hour = &summary.cleaned_hours[0];
                    ("cleaned", format!("CLN,H={}", hour))
                }
                n => ("cleaned", format!("{}/24 CLN", n)),
            }
        } else if missing_count > 0 && failed_count > 0 {
            // Both missing and failed
            ("mixed", format!("{}KO+{}?", failed_count, missing_count))
        } else if missing_count > 0 {
            // Missing checks - distinguish between fully unchecked and partial missing
            match missing_count {
                24 => ("unchecked", "unchecked".to_string()),
                1 => {
                    let hour = &summary.missing_hours[0];
                    // Use "partial" class when some hours are OK but one is missing
                    ("partial", format!("MISS,H={}", hour))
                }
                n => {
                    // Use "partial" class when some hours are OK but others missing
                    ("partial", format!("{}/24 MISS", n))
                }
            }
        } else if failed_count > 0 {
            // Only failed
            match failed_count {
                1 => {
                    let hour = &summary.failed_hours[0];
                    ("warn", format!("KO,H={}", hour))
                }
                n => ("fail", format!("{}/24 KO", n)),
            }
        } else if to_consolidate_count > 0 {
            // To consolidate (no files in either bucket yet)
            match to_consolidate_count {
                24 => ("toconsolidate", "to consolidate".to_string()),
                1 => {
                    let hour = &summary.to_consolidate_hours[0];
                    ("toconsolidate", format!("TODO,H={}", hour))
                }
                n => ("toconsolidate", format!("{}/24 TODO", n)),
            }
        } else {
            // All OK
            ("ok", String::new())
        };

        let check_date_text = summary
            .last_check_date
            .as_ref()
            .map(|d| format!("@{}", d))
            .unwrap_or_default();

        // File counts text (only show if there are files)
        let files_text = if summary.total_archived_files > 0 || summary.total_consolidated_files > 0
        {
            format!(
                "{}→{}",
                summary.total_archived_files, summary.total_consolidated_files
            )
        } else {
            String::new()
        };

        // Check count text (show number of checks performed)
        let checks_text = if summary.total_checks > 0 {
            format!("{}chk", summary.total_checks)
        } else {
            String::new()
        };

        boxes.push_str(&format!(
            r#"    <div class="day {}">
      <div class="date">{}</div>
      <div class="status">{}</div>
      <div class="stats">{} {}</div>
      <div class="check-date">{}</div>
    </div>
"#,
            class, summary.date, status_text, files_text, checks_text, check_date_text
        ));
    }

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Consolidation Check Recap</title>
  <style>
    body {{ font-family: sans-serif; padding: 20px; }}
    h1 {{ margin-bottom: 20px; }}
    .legend {{
      margin-bottom: 20px;
      padding: 12px;
      background: #f5f5f5;
      border-radius: 4px;
      font-size: 14px;
    }}
    .legend-title {{ font-weight: bold; margin-bottom: 8px; }}
    .legend-items {{ display: flex; flex-wrap: wrap; gap: 16px; }}
    .legend-item {{ display: flex; align-items: center; gap: 6px; }}
    .legend-color {{
      width: 16px;
      height: 16px;
      border-radius: 2px;
      border: 1px solid #333;
    }}
    .container {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .day {{
      width: 100px;
      height: 80px;
      border: 1px solid #333;
      border-radius: 4px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      font-family: monospace;
      font-size: 12px;
    }}
    .date {{ font-weight: bold; }}
    .status {{ margin-top: 4px; }}
    .stats {{ margin-top: 2px; font-size: 10px; opacity: 0.9; }}
    .check-date {{ margin-top: 2px; font-size: 10px; opacity: 0.8; }}
    .ok {{ background-color: #4CAF50; color: white; }}
    .warn {{ background-color: #FFC107; color: black; }}
    .fail {{ background-color: #F44336; color: white; }}
    .unchecked {{ background-color: #9E9E9E; color: white; }}
    .partial {{ background-color: #E91E63; color: white; }}
    .mixed {{ background-color: #FF9800; color: white; }}
    .cleaned {{ background-color: #9C27B0; color: white; }}
    .toconsolidate {{ background-color: #607D8B; color: white; }}
    .pending {{ background-color: #00BCD4; color: white; }}
  </style>
</head>
<body>
  <h1>Consolidation Check Recap</h1>
  <div class="legend">
    <div class="legend-title">Legend</div>
    <div class="legend-items">
      <div class="legend-item">
        <div class="legend-color ok"></div>
        <span>All 24 hours OK (consolidation verified)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color warn"></div>
        <span>1 hour KO &mdash; shows "KO,H=HH" (HH = failed hour)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color fail"></div>
        <span>Multiple hours KO &mdash; shows "N/24 KO" (N = failure count)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color unchecked"></div>
        <span>Unchecked &mdash; all 24 hours have no check result file</span>
      </div>
      <div class="legend-item">
        <div class="legend-color partial"></div>
        <span>Partial missing &mdash; shows "N/24 MISS" (some hours OK, N hours have no check file)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color mixed"></div>
        <span>Mixed &mdash; shows "NKO+M?" (N = failed, M = unchecked)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color cleaned"></div>
        <span>Cleaned &mdash; consolidated files exist, input files cleaned up</span>
      </div>
      <div class="legend-item">
        <div class="legend-color toconsolidate"></div>
        <span>To consolidate &mdash; no files in either bucket yet</span>
      </div>
      <div class="legend-item">
        <div class="legend-color pending"></div>
        <span>Pending &mdash; archived files exist, awaiting consolidation</span>
      </div>
    </div>
    <div style="margin-top: 8px; color: #666;">
      <strong>OK</strong> = consolidation check passed &nbsp;|&nbsp;
      <strong>KO</strong> = consolidation check failed &nbsp;|&nbsp;
      <strong>MISS</strong> = check result file missing &nbsp;|&nbsp;
      <strong>CLN</strong> = cleaned (input files removed) &nbsp;|&nbsp;
      <strong>TODO</strong> = to consolidate &nbsp;|&nbsp;
      <strong>PEND</strong> = pending consolidation
    </div>
    <div style="margin-top: 4px; color: #666;">
      <strong>N→M</strong> = N archived files → M consolidated files &nbsp;|&nbsp;
      <strong>Nchk</strong> = N checks performed (total check results in S3)
    </div>
  </div>
  <div class="container">
{}  </div>
</body>
</html>
"#,
        boxes
    )
}
