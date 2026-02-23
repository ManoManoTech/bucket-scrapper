use chrono::{DateTime, Utc};
use serde::Serialize;

/// Information about an S3 object
#[derive(Debug, Clone, Serialize)]
pub struct S3ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    /// Date/hour prefix extracted from the key, used for output file grouping.
    pub prefix: String,
}
