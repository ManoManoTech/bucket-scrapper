use anyhow::Result;
use aws_sdk_s3::Client;
use std::io::Read;

pub trait S3Client {
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>>;
}

pub struct AwsS3Client {
    client: Client,
}

impl AwsS3Client {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl S3Client for AwsS3Client {
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>> {
        let objects = self.client.list_objects().bucket(bucket).send().await?;
        Ok(objects.contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| k.to_string()))
            .collect())
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let response = self.client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        
        let body = response.body.collect().await?;
        Ok(body.into_bytes().to_vec())
    }
}

pub struct S3FileDisplayer<T: S3Client> {
    s3_client: T,
}

impl<T: S3Client> S3FileDisplayer<T> {
    pub fn new(s3_client: T) -> Self {
        Self { s3_client }
    }

    pub async fn display_bucket_files(&self, bucket: &str, file_type: &str) -> Result<()> {
        let keys = self.s3_client.list_objects(bucket).await?;
        println!("Objects in {} bucket {}: {:?}", file_type, bucket, keys);

        for key in keys {
            println!("Processing {} file: {}", file_type, key);
            
            let data = self.s3_client.get_object(bucket, &key).await?;
            let content = self.decompress_and_display(&key, &data)?;
            
            println!("{} content {} ({} bytes):", file_type, key, content.len());
            println!("{}", content);
            println!("--- End of {} file {} ---\n", file_type, key);
        }
        
        Ok(())
    }

    pub async fn display_multiple_buckets(&self, buckets: &[&str], file_type: &str) -> Result<()> {
        for bucket in buckets {
            self.display_bucket_files(bucket, file_type).await?;
        }
        Ok(())
    }

    fn decompress_and_display(&self, key: &str, data: &[u8]) -> Result<String> {
        if key.ends_with(".zst") {
            let mut decompressor = zstd::Decoder::new(data)
                .map_err(|e| anyhow::anyhow!("Erreur zstd: {}", e))?;
            
            let mut decompressed = Vec::new();
            std::io::copy(&mut decompressor, &mut decompressed)?;
            
            Ok(String::from_utf8_lossy(&decompressed).to_string())
        } else if key.ends_with(".gz") {
            use flate2::read::GzDecoder;
            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::new();
            std::io::copy(&mut decoder, &mut decompressed)?;
            
            Ok(String::from_utf8_lossy(&decompressed).to_string())
        } else {
            Ok(String::from_utf8_lossy(data).to_string())
        }
    }
}

pub async fn check_output_bucket_not_empty(s3_client: &impl S3Client, bucket: &str) -> Result<()> {
    let keys = s3_client.list_objects(bucket).await?;
    if keys.is_empty() {
        anyhow::bail!("No objects found in the results bucket: {}", bucket);
    }
    Ok(())
}