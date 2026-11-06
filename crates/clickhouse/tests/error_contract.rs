//! Unit contracts for structured diagnostics and causal error chaining.
use std::error::Error as _;

use clickhouse::{Error, ServerException};

/// Server exception display preserves its stable code and diagnostic identity.
#[test]
fn server_exception_is_a_source() {
    let error = Error::Server(ServerException {
        code: 53,
        name: "TYPE_MISMATCH".into(),
        display_text: "column id requires UInt64".into(),
        stack_trace: String::new(),
    });
    let source = error
        .source()
        .expect("structured server error must expose its source");
    assert_eq!(
        source.to_string(),
        "TYPE_MISMATCH (53): column id requires UInt64"
    );
    assert!(error.to_string().contains("TYPE_MISMATCH"));
}

/// Cleanup preserves the original operation as its causal source and recovery separately.
#[test]
fn cleanup_keeps_primary_cause() {
    let error = Error::Cleanup {
        operation: Box::new(Error::Io(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        ))),
        recovery: Box::new(Error::ConnectionPoisoned),
    };
    let source = error
        .source()
        .expect("cleanup must retain its operation cause");
    let mut current = Some(source);
    let mut original_kind = None;
    while let Some(cause) = current {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            original_kind = Some(io.kind());
        }
        current = cause.source();
    }
    assert_eq!(original_kind, Some(std::io::ErrorKind::BrokenPipe));
    assert!(error.to_string().contains("reset"));
}
