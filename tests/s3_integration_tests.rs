mod test_helpers;

use anyhow::Result;
use test_helpers::{assert_failure, assert_success, run_consolidation_test};

#[tokio::test]
async fn test_check_consolidation() -> Result<()> {
    let result = run_consolidation_test("simple-001").await?;
    assert_success(&result);
    assert_eq!(result.date, "20231225");
    assert_eq!(result.hour, "14");
    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_line_return() -> Result<()> {
    let line_return_result = run_consolidation_test("line-return-001").await?;
    let without_line_return_result = run_consolidation_test("without-line-return-001").await?;

    assert_success(&line_return_result);
    assert_success(&without_line_return_result);
    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_broken_inputs() -> Result<()> {
    let result = run_consolidation_test("broken-input-001").await?;
    assert_failure(&result);
    Ok(())
}

#[tokio::test]
async fn test_check_consolidation_with_broken_outputs() -> Result<()> {
    let result = run_consolidation_test("broken-output-001").await?;
    assert_failure(&result);
    Ok(())
}
