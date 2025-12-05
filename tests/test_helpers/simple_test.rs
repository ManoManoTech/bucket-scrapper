use super::test_consolidation::{ConsolidationResult, check_consolidation_with_config};
use super::{TestConstants, TestEnvironment};
use anyhow::Result;
use log_consolidator_checker_rust::config::loader::load_config;

pub async fn run_consolidation_test(test_dataset: &str) -> Result<ConsolidationResult> {
    let test_env = TestEnvironment::create(test_dataset.to_string()).await?;
    test_env.populate_all_buckets().await;

    let config = load_config(TestConstants::MOCK_CONFIG_PATH)?;
    check_consolidation_with_config(test_dataset.to_string(), &config, &test_env).await
}

pub fn assert_success(result: &ConsolidationResult) {
    assert!(
        result.ok,
        "Consolidation should succeed. Message: {}",
        result.message
    );
    assert!(
        !result.uploaded_result_key.is_empty(),
        "Result should be uploaded to S3"
    );
}

pub fn assert_failure(result: &ConsolidationResult) {
    assert!(
        !result.ok,
        "Consolidation should fail. Message: {}",
        result.message
    );
}
