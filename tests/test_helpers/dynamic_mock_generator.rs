use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc, Duration};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use std::io::Write;

/// Dynamic mock data generator that creates JSON log entries with appropriate timestamps
pub struct DynamicMockGenerator {
    target_time: DateTime<Utc>,
}

impl DynamicMockGenerator {
    /// Create a generator with timestamps suitable for continuous mode
    /// (between 70 minutes and 22 days old)
    pub fn new_for_continuous() -> Self {
        let now = Utc::now();
        // Generate timestamp 2 hours ago (more than 70min requirement, less than 22 days)
        let target_time = now - Duration::hours(2);
        
        Self { target_time }
    }

    /// Create a generator with a specific timestamp
    pub fn new_with_timestamp(target_time: DateTime<Utc>) -> Self {
        Self { target_time }
    }

    /// Generate log entry JSON for a specific service and environment
    pub fn generate_log_entry(&self, service: &str, env: &str, message: &str, extra_data: Option<serde_json::Value>) -> serde_json::Value {
        let mut log_entry = json!({
            "timestamp": self.target_time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "level": "info",
            "message": message,
            "service": service,
            "env": env
        });

        // Merge extra data if provided
        if let Some(extra) = extra_data {
            if let serde_json::Value::Object(extra_map) = extra {
                if let serde_json::Value::Object(ref mut log_map) = log_entry {
                    for (key, value) in extra_map {
                        log_map.insert(key, value);
                    }
                }
            }
        }

        log_entry
    }

    /// Generate multiple log entries for different scenarios
    pub fn generate_log_entries(&self) -> Vec<(String, String, serde_json::Value)> {
        vec![
            // Service: app, Env: support
            (
                "app".to_string(),
                "support".to_string(),
                self.generate_log_entry("app", "support", "Processing user request", Some(json!({
                    "request_id": "req_001"
                })))
            ),
            // Service: web, Env: support
            (
                "web".to_string(), 
                "support".to_string(),
                self.generate_log_entry("web", "support", "HTTP request received", Some(json!({
                    "method": "GET",
                    "path": "/api/users"
                })))
            ),
            // Service: app, Env: int
            (
                "app".to_string(),
                "int".to_string(),
                self.generate_log_entry("app", "int", "Integration test started", Some(json!({
                    "test_suite": "api_tests"
                })))
            ),
            // Service: web, Env: int
            (
                "web".to_string(),
                "int".to_string(),
                self.generate_log_entry("web", "int", "Datadog integration test", Some(json!({
                    "endpoint": "/health"
                })))
            ),
            // Service: app, Env: stg
            (
                "app".to_string(),
                "stg".to_string(),
                self.generate_log_entry("app", "stg", "Staging deployment completed", Some(json!({
                    "version": "v1.2.3"
                })))
            ),
            // Service: web, Env: stg
            (
                "web".to_string(),
                "stg".to_string(),
                self.generate_log_entry("web", "stg", "Staging web server started", Some(json!({
                    "port": 8080
                })))
            ),
        ]
    }

    /// Generate log entries with line returns for testing consolidation with special characters
    pub fn generate_log_entries_with_line_returns(&self) -> Vec<(String, String, serde_json::Value)> {
        vec![
            // Service: app, Env: support - with line returns in message
            (
                "app".to_string(),
                "support".to_string(),
                self.generate_log_entry("app", "support", "Processing user request with\nline returns", Some(json!({
                    "request_id": "req_001"
                })))
            ),
            // Service: app, Env: int - with multiple line returns
            (
                "app".to_string(),
                "int".to_string(),
                self.generate_log_entry("app", "int", "Integration test started\n with \n\n\n\n\n\n\n\n\n in", Some(json!({
                    "test_suite": "api_tests"
                })))
            ),
            // Service: app, Env: stg - with line returns in message
            (
                "app".to_string(),
                "stg".to_string(),
                self.generate_log_entry("app", "stg", "Staging deployment with\nmultiple\nline\nreturns", Some(json!({
                    "version": "v1.2.3"
                })))
            ),
        ]
    }

    /// Compress JSON data with gzip
    pub fn compress_json(&self, json_data: &serde_json::Value) -> Result<Vec<u8>> {
        let json_string = serde_json::to_string(json_data)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json_string.as_bytes())?;
        Ok(encoder.finish()?)
    }

    /// Upload a JSON file to S3 with gzip compression
    pub async fn upload_json_to_s3(
        &self,
        s3_client: &Client,
        bucket: &str,
        key: &str,
        json_data: &serde_json::Value,
    ) -> Result<()> {
        let compressed_data = self.compress_json(json_data)?;
        let data_size = compressed_data.len();
        
        s3_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(compressed_data.into())
            .content_type("application/json")
            .content_encoding("gzip")
            .send()
            .await?;
            
        println!("Uploaded {} bytes to s3://{}/{}", data_size, bucket, key);
        Ok(())
    }

    /// Populate S3 buckets with dynamically generated mock data for continuous mode
    pub async fn populate_s3_for_continuous_mode(&self, s3_client: &Client) -> Result<()> {
        let date_path = self.target_time.format("%Y/%m/%d/%H").to_string();
        let dt_date_path = self.target_time.format("dt=%Y%m%d/h=%H").to_string();
        
        let log_entries = self.generate_log_entries();
        
        for (service, env, json_data) in log_entries {
            match env.as_str() {
                "support" => {
                    // Bucket input-a uses YYYY/MM/DD/HH format
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(s3_client, "input-a", &key, &json_data).await?;
                }
                "int" => {
                    // Bucket input-b uses dt=YYYYMMDD/h=HH format
                    let key = format!("{}/{}-{}.json.gz", dt_date_path, service, env);
                    self.upload_json_to_s3(s3_client, "input-b", &key, &json_data).await?;
                }
                "stg" => {
                    // Bucket input-c uses YYYY/MM/DD/HH format
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(s3_client, "input-c", &key, &json_data).await?;
                }
                _ => {}
            }
        }
        
        Ok(())
    }

    /// Populate S3 buckets with line return test data for continuous mode
    pub async fn populate_s3_for_continuous_mode_line_returns(&self, s3_client: &Client) -> Result<()> {
        let date_path = self.target_time.format("%Y/%m/%d/%H").to_string();
        let dt_date_path = self.target_time.format("dt=%Y%m%d/h=%H").to_string();
        
        let log_entries = self.generate_log_entries_with_line_returns();
        
        for (service, env, json_data) in log_entries {
            match env.as_str() {
                "support" => {
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(s3_client, "input-a", &key, &json_data).await?;
                }
                "int" => {
                    let key = format!("{}/{}-{}.json.gz", dt_date_path, service, env);
                    self.upload_json_to_s3(s3_client, "input-b", &key, &json_data).await?;
                }
                "stg" => {
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(s3_client, "input-c", &key, &json_data).await?;
                }
                _ => {}
            }
        }
        
        Ok(())
    }

    /// Get the target timestamp for display/debugging
    pub fn get_target_time(&self) -> DateTime<Utc> {
        self.target_time
    }

    /// Get formatted date/hour strings for path generation
    pub fn get_date_hour_strings(&self) -> (String, String) {
        (
            self.target_time.format("%Y%m%d").to_string(),
            self.target_time.format("%H").to_string(),
        )
    }
}