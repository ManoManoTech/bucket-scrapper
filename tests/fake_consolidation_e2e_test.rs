mod test_helpers;

use anyhow::Result;
use test_helpers::fake_consolidation_setup::FakeConsolidationEnvironment;

#[tokio::test]
#[ignore = "ezci-impure"] // Requires S3_ENDPOINT support in the checker binary
async fn test_fake_consolidation_with_docker_container() -> Result<()> {
    println!("\n=== Testing with Docker Container (E2E) ===\n");
    println!("NOTE: This test is currently ignored because the checker binary");
    println!("does not yet support custom S3 endpoints via environment variable.");
    println!("The S3 client needs to be updated to respect S3_ENDPOINT.\n");

    // Setup environment with shared code
    let env = FakeConsolidationEnvironment::setup().await?;

    // Write config to temporary file
    let config_path = "/tmp/test-consolidation-config.yaml";
    env.write_config_file(config_path).await?;
    println!("Written config to {}", config_path);

    // Get MinIO endpoint accessible from Docker container
    // For Linux, use host.docker.internal or the bridge network IP
    // For this test, we'll use the host network mode or pass the actual MinIO container IP
    let minio_endpoint = format!("http://host.docker.internal:{}", env.minio_port);
    println!("MinIO endpoint for container: {}", minio_endpoint);

    println!("\nStarting checker container...");
    println!("  - MinIO endpoint: {}", minio_endpoint);
    println!("  - Config mounted from: {}", config_path);
    println!("  - Running container directly with check command...");

    let output = tokio::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-e",
            "AWS_ACCESS_KEY_ID=minioadmin",
            "-e",
            "AWS_SECRET_ACCESS_KEY=minioadmin",
            "-e",
            "AWS_REGION=us-east-1",
            "-e",
            &format!("S3_ENDPOINT={}", minio_endpoint),
            "-v",
            &format!("{}:/config/config.yaml", config_path),
            "304971447450.dkr.ecr.eu-west-3.amazonaws.com/infra/log-consolidator-checker:test",
            "--config",
            "/config/config.yaml",
            "--region",
            "us-east-1",
            "--max-parallel",
            "4",
            "--process-threads",
            "2",
            "--memory-pool-mb",
            "128",
            "check",
            "--date",
            "20250131",
            "--hour",
            "03",
        ])
        .output()
        .await?;

    println!("Container execution completed!");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("\n=== Container Output ===");
    println!("STDOUT:\n{}", stdout);
    if !stderr.is_empty() {
        println!("STDERR:\n{}", stderr);
    }
    println!("Exit code: {:?}", output.status.code());
    println!("======================\n");

    // Check exit code
    assert!(
        output.status.success(),
        "Checker container should exit successfully. Exit code: {:?}\nSTDOUT: {}\nSTDERR: {}",
        output.status.code(),
        stdout,
        stderr
    );

    // Verify output contains success indicators
    let output_combined = format!("{} {}", stdout, stderr);
    assert!(
        output_combined.contains("Check passed")
            || output_combined.contains("Consolidated size matches")
            || output_combined.contains("ok: true")
            || output_combined.contains("\"ok\":true"),
        "Output should indicate successful consolidation check. Got:\nSTDOUT: {}\nSTDERR: {}",
        stdout,
        stderr
    );

    println!("✅ E2E test passed!");

    Ok(())
}
