use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, primitives::ByteStream};
use aws_types::SdkConfig;
use flate2::Compression;
use flate2::write::GzEncoder;
use log_consolidator_checker_rust::config::types::{BucketConfig, ConfigSchema, PathSchema};
use std::collections::HashMap;
use std::io::Write;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use zstd::stream::write::Encoder as ZstdEncoder;

pub struct FakeConsolidationEnvironment {
    pub sdk_config: SdkConfig,
    pub s3_client: Client,
    pub minio_container: testcontainers::ContainerAsync<MinIO>,
    pub minio_port: u16,
    pub config: ConfigSchema,
    pub date: String,
    pub hour: String,
}

impl FakeConsolidationEnvironment {
    pub async fn setup() -> Result<Self> {
        // Setup MinIO container
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");
        let container = MinIO::default().start().await?;
        let port = container.get_host_port_ipv4(9000).await?;
        let endpoint = format!("http://127.0.0.1:{}", port);

        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .region(aws_config::Region::new("us-east-1"))
            .load()
            .await;

        let s3_client = Client::new(&sdk_config);

        // Create buckets
        s3_client
            .create_bucket()
            .bucket("input-bucket-1")
            .send()
            .await?;
        s3_client
            .create_bucket()
            .bucket("input-bucket-2")
            .send()
            .await?;
        s3_client
            .create_bucket()
            .bucket("consolidated-bucket")
            .send()
            .await?;
        s3_client
            .create_bucket()
            .bucket("results-bucket")
            .send()
            .await?;

        // Generate mock log data in-memory
        // These simulate logs from two different services that get consolidated
        let log_entries_service1 =
            generate_mock_log_entries("infra-clifford-the-parser-main", "noenv", 10);
        let log_entries_service2 = generate_mock_log_entries("upwind-cluster-agent", "stg", 8);

        // Create input files (gzip compressed, as they would be before consolidation)
        let input_file_1 = compress_with_gzip(&log_entries_service1)?;
        let input_file_2 = compress_with_gzip(&log_entries_service2)?;

        // Create consolidated files (zstd compressed, as they would be after consolidation)
        // The consolidated files contain the same data, just recompressed with zstd
        let consolidated_file_1 = compress_with_zstd(&log_entries_service1)?;
        let consolidated_file_2 = compress_with_zstd(&log_entries_service2)?;

        println!("Generated mock data:");
        println!("  Input file 1 (gzip): {} bytes", input_file_1.len());
        println!("  Input file 2 (gzip): {} bytes", input_file_2.len());
        println!(
            "  Consolidated file 1 (zstd): {} bytes",
            consolidated_file_1.len()
        );
        println!(
            "  Consolidated file 2 (zstd): {} bytes",
            consolidated_file_2.len()
        );

        // Upload input files to respective input buckets
        let file_name_1 = "1738292533-dc20a9e6-d9a1-405e-bd02-adafd451ae67.log.gz";
        let file_name_2 = "archive_030000.5708.dawW3EiDQKOoO_WCXKm5yA.json.gz";

        s3_client
            .put_object()
            .bucket("input-bucket-1")
            .key(format!("logs/dt=20250131/h=03/{}", file_name_1))
            .body(ByteStream::from(input_file_1))
            .send()
            .await?;

        s3_client
            .put_object()
            .bucket("input-bucket-2")
            .key(format!("logs/dt=20250131/h=03/{}", file_name_2))
            .body(ByteStream::from(input_file_2))
            .send()
            .await?;

        // Upload consolidated files to consolidated bucket
        s3_client
            .put_object()
            .bucket("consolidated-bucket")
            .key("consolidated/dt=20250131/h=03/noenv-infra-clifford-the-parser-main.json.zst")
            .body(ByteStream::from(consolidated_file_1))
            .send()
            .await?;

        s3_client
            .put_object()
            .bucket("consolidated-bucket")
            .key("consolidated/dt=20250131/h=03/stg-upwind-cluster-agent.json.zst")
            .body(ByteStream::from(consolidated_file_2))
            .send()
            .await?;

        println!("Uploaded all files to S3");

        // Create checker configuration
        let config = ConfigSchema {
            bucketsToConsolidate: vec![
                BucketConfig {
                    bucket: "input-bucket-1".to_string(),
                    path: vec![
                        PathSchema::Static {
                            static_path: "logs".to_string(),
                        },
                        PathSchema::DateFormat {
                            datefmt: "dt=20250131/h=03".to_string(),
                        },
                    ],
                    only_prefix_patterns: None,
                    proceed_without_matching_objects: false,
                    extra: HashMap::new(),
                },
                BucketConfig {
                    bucket: "input-bucket-2".to_string(),
                    path: vec![
                        PathSchema::Static {
                            static_path: "logs".to_string(),
                        },
                        PathSchema::DateFormat {
                            datefmt: "dt=20250131/h=03".to_string(),
                        },
                    ],
                    only_prefix_patterns: None,
                    proceed_without_matching_objects: false,
                    extra: HashMap::new(),
                },
            ],
            bucketsConsolidated: vec![BucketConfig {
                bucket: "consolidated-bucket".to_string(),
                path: vec![
                    PathSchema::Static {
                        static_path: "consolidated".to_string(),
                    },
                    PathSchema::DateFormat {
                        datefmt: "dt=20250131/h=03".to_string(),
                    },
                ],
                only_prefix_patterns: None,
                proceed_without_matching_objects: false,
                extra: HashMap::new(),
            }],
            bucketsCheckerResults: vec![],
            extra: HashMap::new(),
        };

        let date = "20250131".to_string();
        let hour = "03".to_string();

        Ok(Self {
            sdk_config,
            s3_client,
            minio_container: container,
            minio_port: port,
            config,
            date,
            hour,
        })
    }

    pub fn get_minio_endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.minio_port)
    }

    pub async fn write_config_file(&self, path: &str) -> Result<()> {
        // Manually write the config in YAML format since ConfigSchema doesn't implement Serialize
        let config_yaml = r#"bucketsToConsolidate:
  - bucket: input-bucket-1
    path:
      - static_path: logs
      - datefmt: "dt=20250131/h=03"
  - bucket: input-bucket-2
    path:
      - static_path: logs
      - datefmt: "dt=20250131/h=03"

bucketsConsolidated:
  - bucket: consolidated-bucket
    path:
      - static_path: consolidated
      - datefmt: "dt=20250131/h=03"

bucketsCheckerResults:
  - bucket: results-bucket
    path:
      - static_path: results
      - datefmt: "dt=20250131/h=03"
"#;
        tokio::fs::write(path, config_yaml).await?;
        Ok(())
    }
}

/// Generate mock log entries in JSON Lines format
fn generate_mock_log_entries(service: &str, env: &str, count: usize) -> Vec<u8> {
    let mut entries = Vec::new();

    for i in 0..count {
        let log_entry = format!(
            r#"{{"timestamp":"2025-01-31T03:{:02}:{:02}.000Z","level":"INFO","service":"{}","env":"{}","message":"Log message number {}","trace_id":"trace-{:04}","span_id":"span-{:04}"}}"#,
            i % 60,
            i % 60,
            service,
            env,
            i,
            i,
            i
        );
        entries.extend_from_slice(log_entry.as_bytes());
        entries.push(b'\n');
    }

    entries
}

/// Compress data with gzip
fn compress_with_gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}

/// Compress data with zstd
fn compress_with_zstd(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZstdEncoder::new(Vec::new(), 3)?;
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}
