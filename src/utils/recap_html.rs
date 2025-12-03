use std::collections::HashMap;

/// Summary of check results for a single day
pub struct DaySummary {
    /// Date in YYYY-MM-DD format
    pub date: String,
    /// Hours that failed the check (HH format)
    pub failed_hours: Vec<String>,
}

/// Aggregate hourly results into daily summaries
pub fn aggregate_by_day(results: &HashMap<String, serde_json::Value>) -> Vec<DaySummary> {
    // Group results by date (YYYYMMDD)
    let mut by_date: HashMap<String, Vec<(String, bool)>> = HashMap::new();

    for (key, value) in results {
        // Key format: "YYYYMMDD/HH"
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 2 {
            continue;
        }
        let date = parts[0];
        let hour = parts[1];
        let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);

        by_date
            .entry(date.to_string())
            .or_default()
            .push((hour.to_string(), ok));
    }

    // Convert to DaySummary, sorted by date
    let mut summaries: Vec<DaySummary> = by_date
        .into_iter()
        .map(|(date, hours)| {
            let failed_hours: Vec<String> = hours
                .into_iter()
                .filter(|(_, ok)| !ok)
                .map(|(h, _)| h)
                .collect();

            // Convert YYYYMMDD to YYYY-MM-DD
            let formatted_date = if date.len() == 8 {
                format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8])
            } else {
                date
            };

            DaySummary {
                date: formatted_date,
                failed_hours,
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
        let (class, status_text) = match summary.failed_hours.len() {
            0 => ("ok", String::new()),
            1 => {
                let hour = &summary.failed_hours[0];
                ("warn", format!("KO,H={}", hour))
            }
            n => ("fail", format!("{}/24 KO", n)),
        };

        boxes.push_str(&format!(
            r#"    <div class="day {}">
      <div class="date">{}</div>
      <div class="status">{}</div>
    </div>
"#,
            class, summary.date, status_text
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
    .ok {{ background-color: #4CAF50; color: white; }}
    .warn {{ background-color: #FFC107; color: black; }}
    .fail {{ background-color: #F44336; color: white; }}
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
    </div>
    <div style="margin-top: 8px; color: #666;">
      <strong>OK</strong> = consolidation check passed &nbsp;|&nbsp;
      <strong>KO</strong> = consolidation check failed (character count mismatch)
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
