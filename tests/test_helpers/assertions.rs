/// Test assertion helpers for S3 and integration tests
/// Provides common assertion patterns for validating test results
use anyhow::Result;

/// Assertions for S3 file operations
pub struct S3Assertions;

impl S3Assertions {
    /// Assert that a bucket contains expected number of files
    pub async fn assert_bucket_file_count(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        expected_count: usize,
    ) -> Result<()> {
        let response = client.list_objects_v2().bucket(bucket).send().await?;
        let actual_count = response.contents().len();

        if actual_count != expected_count {
            return Err(anyhow::anyhow!(
                "Expected {} files in bucket {}, found {}",
                expected_count,
                bucket,
                actual_count
            ));
        }

        Ok(())
    }

    /// Assert that all files in bucket have specific extension
    pub async fn assert_files_have_extension(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        extension: &str,
    ) -> Result<()> {
        let response = client.list_objects_v2().bucket(bucket).send().await?;

        for object in response.contents() {
            if let Some(key) = object.key() {
                if !key.ends_with(extension) {
                    return Err(anyhow::anyhow!(
                        "File {} in bucket {} does not have expected extension {}",
                        key,
                        bucket,
                        extension
                    ));
                }
            }
        }

        Ok(())
    }

    /// Assert that bucket exists and is accessible
    pub async fn assert_bucket_exists(client: &aws_sdk_s3::Client, bucket: &str) -> Result<()> {
        client.head_bucket().bucket(bucket).send().await?;
        Ok(())
    }
}
