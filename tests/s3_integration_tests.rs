use aws_sdk_s3::primitives::ByteStream;
use log_consolidator_checker_rust::s3::client::WrappedS3Client;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

#[tokio::test]
async fn test_s3_client_with_minio() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;

    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    // Set environment variables for AWS SDK to use our MinIO endpoint
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    // Use AWS default configuration which will pick up our environment variables
    let config = aws_config::load_from_env().await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    // Test basic S3 operations
    let bucket_name = "test-bucket";

    // Create bucket
    s3_client.create_bucket().bucket(bucket_name).send().await?;

    // List buckets to verify creation
    let buckets = s3_client.list_buckets().send().await?;
    assert!(buckets
        .buckets()
        .iter()
        .any(|b| b.name() == Some(bucket_name)));

    // Put an object
    s3_client
        .put_object()
        .bucket(bucket_name)
        .key("test-file.txt")
        .body(ByteStream::from_static(b"Hello MinIO!"))
        .send()
        .await?;

    // Get the object back
    let object = s3_client
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
async fn test_wrapped_s3_client() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;

    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    // Set environment variables for AWS SDK to use our MinIO endpoint
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    // Test our WrappedS3Client - it creates its own client internally
    let wrapped_client = WrappedS3Client::new("us-east-1", 1).await?;

    let bucket_name = "test-bucket-wrapped";

    // Create bucket through wrapper
    wrapped_client
        .get_client()
        .await?
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await?;

    // Test list_objects_v2 through wrapper
    let objects = wrapped_client
        .get_client()
        .await?
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    // Should be empty initially
    assert_eq!(objects.contents().len(), 0);

    // Put some test objects
    for i in 0..5 {
        wrapped_client
            .get_client()
            .await?
            .put_object()
            .bucket(bucket_name)
            .key(&format!("test-file-{}.txt", i))
            .body(ByteStream::from(format!("Content {}", i).into_bytes()))
            .send()
            .await?;
    }

    // List objects again
    let objects = wrapped_client
        .get_client()
        .await?
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    assert_eq!(objects.contents().len(), 5);

    // Note: WrappedS3Client doesn't implement Clone, so we'll skip concurrent test
    // This is actually good behavior for a rate-limited client

    Ok(())
}

#[tokio::test]
async fn test_s3_client_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Start MinIO container
    let minio_container = MinIO::default().start().await?;

    // Get MinIO connection details
    let host_port = minio_container.get_host_port_ipv4(9000).await?;
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    // Set environment variables for AWS SDK to use our MinIO endpoint
    std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    // Test WrappedS3Client operations
    let wrapped_client = WrappedS3Client::new("us-east-1", 1).await?;

    let bucket_name = "test-operations";

    // Create bucket
    wrapped_client
        .get_client()
        .await?
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await?;

    // Make 3 calls to test client reuse
    for i in 0..3 {
        wrapped_client
            .get_client()
            .await?
            .put_object()
            .bucket(bucket_name)
            .key(&format!("test-{}.txt", i))
            .body(ByteStream::from_static(b"test"))
            .send()
            .await?;
    }

    // Verify objects were created
    let objects = wrapped_client
        .get_client()
        .await?
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await?;

    assert_eq!(objects.contents().len(), 3);

    Ok(())
}
