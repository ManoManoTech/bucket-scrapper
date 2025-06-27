use anyhow::Result;
/// Common S3 operations for tests
/// Provides reusable S3 operation patterns across different test files
use aws_sdk_s3::primitives::ByteStream;

/// Helper for common S3 operations in tests
pub struct S3Operations;

impl S3Operations {
    /// Upload multiple files to a bucket in parallel
    pub async fn upload_files_parallel(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        files: Vec<(String, Vec<u8>)>, // (key, content)
    ) -> Result<()> {
        let upload_futures: Vec<_> = files
            .into_iter()
            .map(|(key, content)| {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(content))
                    .send()
            })
            .collect();

        {
            for future in upload_futures {
                future.await?;
            }
        };
        Ok(())
    }

    /// Create multiple buckets in parallel
    pub async fn create_buckets_parallel(
        client: &aws_sdk_s3::Client,
        bucket_names: Vec<&str>,
    ) -> Result<()> {
        let create_futures: Vec<_> = bucket_names
            .into_iter()
            .map(|bucket_name| client.create_bucket().bucket(bucket_name).send())
            .collect();

        {
            for future in create_futures {
                future.await?;
            }
        };
        Ok(())
    }

    /// List all files in a bucket with optional prefix filter
    pub async fn list_files(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut request = client.list_objects_v2().bucket(bucket);

        if let Some(p) = prefix {
            request = request.prefix(p);
        }

        let response = request.send().await?;

        let keys = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| k.to_string()))
            .collect();

        Ok(keys)
    }

    /// Download and return file content as bytes
    pub async fn download_file(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<u8>> {
        let response = client.get_object().bucket(bucket).key(key).send().await?;
        let data = response.body.collect().await?.into_bytes();
        Ok(data.to_vec())
    }

    /// Check if bucket exists
    pub async fn bucket_exists(client: &aws_sdk_s3::Client, bucket: &str) -> bool {
        client.head_bucket().bucket(bucket).send().await.is_ok()
    }

    /// Wait for bucket to become available
    pub async fn wait_for_bucket(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        max_attempts: usize,
    ) -> Result<()> {
        for _ in 0..max_attempts {
            if Self::bucket_exists(client, bucket).await {
                return Ok(());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Err(anyhow::anyhow!(
            "Bucket {} did not become available",
            bucket
        ))
    }
}
