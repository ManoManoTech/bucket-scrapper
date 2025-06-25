// Refactored S3 integration tests using test_helpers
// This replaces the old s3_integration_tests.rs with cleaner, reusable code

mod test_helpers;

use aws_sdk_s3::primitives::ByteStream;
use test_helpers::*;

#[tokio::test]
async fn test_s3_client_with_minio_refactored() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment (replaces ~15 lines of boilerplate)
    let test_env = TestEnvironment::new().await?;
    
    let bucket_name = "test-bucket";

    // Create bucket using helper
    test_env.create_bucket(bucket_name).await?;

    // List buckets to verify creation
    let buckets = test_env.client.list_buckets().send().await?;
    assert!(buckets
        .buckets()
        .iter()
        .any(|b| b.name() == Some(bucket_name)));

    // Put an object
    test_env.client
        .put_object()
        .bucket(bucket_name)
        .key("test-file.txt")
        .body(ByteStream::from_static(b"Hello MinIO!"))
        .send()
        .await?;

    // Get the object back
    let object = test_env.client
        .get_object()
        .bucket(bucket_name)
        .key("test-file.txt")
        .send()
        .await?;

    let body = object.body.collect().await?;
    let content = String::from_utf8(body.to_vec())?;
    assert_eq!(content, "Hello MinIO!");

    Ok(())
}

#[tokio::test]
async fn test_wrapped_s3_client_refactored() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let test_env = TestEnvironment::new().await?;
    
    let bucket_name = "test-bucket-wrapped";

    // Create bucket through wrapper
    test_env.create_bucket(bucket_name).await?;

    // Test list_objects_v2 through wrapper
    let objects = test_env.s3_client
        .get_client()
        .await?
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    // Should be empty initially
    assert_eq!(objects.contents().len(), 0);

    // Put some test objects using helper pattern
    let test_files = FileListFactory::create_sequential_files(
        "",
        "test-file", 
        5, 
        "txt"
    );

    for (key, content) in test_files {
        test_env.s3_client
            .get_client()
            .await?
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(content.into_bytes()))
            .send()
            .await?;
    }

    // List objects again
    let objects = test_env.s3_client
        .get_client()
        .await?
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    assert_eq!(objects.contents().len(), 5);

    Ok(())
}

#[tokio::test]
async fn test_s3_client_operations_refactored() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let test_env = TestEnvironment::new().await?;
    
    let bucket_name = "test-operations";

    // Create bucket
    test_env.create_bucket(bucket_name).await?;

    // Create test files using factory
    let test_files = FileListFactory::create_sequential_files(
        "",
        "test",
        3,
        "txt"
    );

    // Upload files to test client reuse
    for (key, content) in test_files {
        test_env.s3_client
            .get_client()
            .await?
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(content.into_bytes()))
            .send()
            .await?;
    }

    // Verify objects were created
    let objects = test_env.s3_client
        .get_client()
        .await?
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    assert_eq!(objects.contents().len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_json_log_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Test integration with JSON logs using helpers
    let test_env = TestEnvironment::new().await?;
    
    let bucket_name = "test-json-logs";
    test_env.create_bucket(bucket_name).await?;

    // Create JSON logs using factory
    let json_logs = JsonLogFactory::create_multi_env_service(
        vec![("test", "integration")],
        3
    );

    // Upload JSON logs
    for (i, json_log) in json_logs.iter().enumerate() {
        let compressed_data = JsonCompression::compress_json_zstd(json_log);
        
        test_env.client
            .put_object()
            .bucket(bucket_name)
            .key(&format!("logs/test-{}.json.zst", i))
            .body(ByteStream::from(compressed_data))
            .send()
            .await?;
    }

    // Verify files were uploaded
    let objects = test_env.client
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    assert_eq!(objects.contents().len(), 3);

    // Download and verify one file
    let response = test_env.client
        .get_object()
        .bucket(bucket_name)
        .key("logs/test-0.json.zst")
        .send()
        .await?;

    let compressed_bytes = response.body.collect().await?.into_bytes();
    let decompressed_content = JsonCompression::decompress_json_zstd(&compressed_bytes)?;

    // Verify JSON content using assertion helpers
    assert_json!(&decompressed_content, env = "test", service = "integration");

    Ok(())
}