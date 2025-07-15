use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Duration, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use std::io::Write;

#[derive(Debug, Clone)]
pub struct BucketMapping {
    pub support_bucket: String,
    pub int_bucket: String,
    pub stg_bucket: String,
}

pub struct DynamicMockGenerator {
    target_time: DateTime<Utc>,
    bucket_mapping: Option<BucketMapping>,
}

impl DynamicMockGenerator {
    pub fn new_for_continuous() -> Self {
        let now = Utc::now();
        let target_time = now - Duration::hours(2);

        Self {
            target_time,
            bucket_mapping: None,
        }
    }

    pub fn new_for_continuous_with_buckets(bucket_mapping: BucketMapping) -> Self {
        let now = Utc::now();
        let target_time = now - Duration::hours(2);

        Self {
            target_time,
            bucket_mapping: Some(bucket_mapping),
        }
    }

    pub fn new_with_timestamp(target_time: DateTime<Utc>) -> Self {
        Self {
            target_time,
            bucket_mapping: None,
        }
    }

    pub fn generate_log_entry(
        &self,
        service: &str,
        env: &str,
        message: &str,
        extra_data: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let mut log_entry = json!({
            "timestamp": self.target_time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "level": "info",
            "message": message,
            "service": service,
            "env": env
        });

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

    pub fn generate_log_entries(&self) -> Vec<(String, String, serde_json::Value)> {
        vec![
            (
                "app".to_string(),
                "support".to_string(),
                self.generate_log_entry(
                    "app",
                    "support",
                    "Processing user request",
                    Some(json!({
                        "request_id": "req_001"
                    })),
                ),
            ),
            (
                "web".to_string(),
                "support".to_string(),
                self.generate_log_entry(
                    "web",
                    "support",
                    "HTTP request received",
                    Some(json!({
                        "method": "GET",
                        "path": "/api/users"
                    })),
                ),
            ),
            (
                "app".to_string(),
                "int".to_string(),
                self.generate_log_entry(
                    "app",
                    "int",
                    "Integration test started",
                    Some(json!({
                        "test_suite": "api_tests"
                    })),
                ),
            ),
            (
                "web".to_string(),
                "int".to_string(),
                self.generate_log_entry(
                    "web",
                    "int",
                    "Datadog integration test",
                    Some(json!({
                        "endpoint": "/health"
                    })),
                ),
            ),
            (
                "app".to_string(),
                "stg".to_string(),
                self.generate_log_entry(
                    "app",
                    "stg",
                    "Staging deployment completed",
                    Some(json!({
                        "version": "v1.2.3"
                    })),
                ),
            ),
            (
                "web".to_string(),
                "stg".to_string(),
                self.generate_log_entry(
                    "web",
                    "stg",
                    "Staging web server started",
                    Some(json!({
                        "port": 8080
                    })),
                ),
            ),
        ]
    }

    pub fn generate_log_entries_with_line_returns(
        &self,
    ) -> Vec<(String, String, serde_json::Value)> {
        vec![
            (
                "app".to_string(),
                "support".to_string(),
                self.generate_log_entry(
                    "app",
                    "support",
                    "Processing user request with\nline returns",
                    Some(json!({
                        "request_id": "req_001"
                    })),
                ),
            ),
            (
                "app".to_string(),
                "int".to_string(),
                self.generate_log_entry(
                    "app",
                    "int",
                    "Integration test started\n with \n\n\n\n\n\n\n\n\n in",
                    Some(json!({
                        "test_suite": "api_tests"
                    })),
                ),
            ),
            (
                "app".to_string(),
                "stg".to_string(),
                self.generate_log_entry(
                    "app",
                    "stg",
                    "Staging deployment with\nmultiple\nline\nreturns",
                    Some(json!({
                        "version": "v1.2.3"
                    })),
                ),
            ),
        ]
    }

    pub fn compress_json(&self, json_data: &serde_json::Value) -> Result<Vec<u8>> {
        let json_string = serde_json::to_string(json_data)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json_string.as_bytes())?;
        Ok(encoder.finish()?)
    }

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

    pub async fn populate_s3_for_continuous_mode(&self, s3_client: &Client) -> Result<()> {
        let date_path = self.target_time.format("%Y/%m/%d/%H").to_string();
        let dt_date_path = self.target_time.format("dt=%Y%m%d/h=%H").to_string();

        let log_entries = self.generate_log_entries();

        let bucket_mapping = self
            .bucket_mapping
            .as_ref()
            .map(|bm| BucketMapping {
                support_bucket: bm.support_bucket.clone(),
                int_bucket: bm.int_bucket.clone(),
                stg_bucket: bm.stg_bucket.clone(),
            })
            .unwrap_or_else(|| BucketMapping {
                support_bucket: "input-a".to_string(),
                int_bucket: "input-b".to_string(),
                stg_bucket: "input-c".to_string(),
            });

        for (service, env, json_data) in log_entries {
            match env.as_str() {
                "support" => {
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(
                        s3_client,
                        &bucket_mapping.support_bucket,
                        &key,
                        &json_data,
                    )
                    .await?;
                }
                "int" => {
                    let key = format!("{}/{}-{}.json.gz", dt_date_path, service, env);
                    self.upload_json_to_s3(s3_client, &bucket_mapping.int_bucket, &key, &json_data)
                        .await?;
                }
                "stg" => {
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(s3_client, &bucket_mapping.stg_bucket, &key, &json_data)
                        .await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub async fn populate_s3_for_continuous_mode_line_returns(
        &self,
        s3_client: &Client,
    ) -> Result<()> {
        let date_path = self.target_time.format("%Y/%m/%d/%H").to_string();
        let dt_date_path = self.target_time.format("dt=%Y%m%d/h=%H").to_string();

        let log_entries = self.generate_log_entries_with_line_returns();

        let bucket_mapping = self
            .bucket_mapping
            .as_ref()
            .map(|bm| BucketMapping {
                support_bucket: bm.support_bucket.clone(),
                int_bucket: bm.int_bucket.clone(),
                stg_bucket: bm.stg_bucket.clone(),
            })
            .unwrap_or_else(|| BucketMapping {
                support_bucket: "input-a".to_string(),
                int_bucket: "input-b".to_string(),
                stg_bucket: "input-c".to_string(),
            });

        for (service, env, json_data) in log_entries {
            match env.as_str() {
                "support" => {
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(
                        s3_client,
                        &bucket_mapping.support_bucket,
                        &key,
                        &json_data,
                    )
                    .await?;
                }
                "int" => {
                    let key = format!("{}/{}-{}.json.gz", dt_date_path, service, env);
                    self.upload_json_to_s3(s3_client, &bucket_mapping.int_bucket, &key, &json_data)
                        .await?;
                }
                "stg" => {
                    let key = format!("{}/{}-{}.json.gz", date_path, service, env);
                    self.upload_json_to_s3(s3_client, &bucket_mapping.stg_bucket, &key, &json_data)
                        .await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn get_target_time(&self) -> DateTime<Utc> {
        self.target_time
    }

    pub fn get_date_hour_strings(&self) -> (String, String) {
        (
            self.target_time.format("%Y%m%d").to_string(),
            self.target_time.format("%H").to_string(),
        )
    }
}
