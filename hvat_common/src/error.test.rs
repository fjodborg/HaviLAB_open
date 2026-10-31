use super::*;

#[test]
fn test_retryable_error_keeps_context_and_retry_policy() {
    let error = ProtocolError::retryable(ErrorCode::PyramidNotReady, "Pyramid is building", 5000)
        .with_context(ErrorContext::Pyramid {
            image_id: "test.png".to_string(),
            progress: Some(0.5),
            eta_seconds: Some(10),
        });

    assert_eq!(error.code, ErrorCode::PyramidNotReady);
    assert_eq!(error.severity, Severity::Error);
    assert!(error.retryable);
    assert_eq!(error.retry_after_ms, 5000);
    assert!(matches!(error.context, ErrorContext::Pyramid { .. }));
}

#[test]
fn test_fatal_error() {
    let error = ProtocolError::fatal(ErrorCode::ProtocolMismatch, "Incompatible version");

    assert_eq!(error.severity, Severity::Fatal);
    assert!(!error.retryable);
}
