//! Structured configuration, transport, schema, and session lifecycle errors.

use std::borrow::Cow;

use crate::query::ServerException;

/// A synchronous SDK operation result.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// A failed operation never leaves an unknown session available for reuse.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An HTTP or binary payload violated its declared layout.
    #[error("protocol: {0}")]
    Protocol(Cow<'static, str>),
    /// A declared ClickHouse column type has no supported decoding representation.
    #[error("unsupported column type: {type_name}")]
    UnsupportedColumnType {
        /// The exact declared ClickHouse type name.
        type_name: String,
    },
    /// The HTTP endpoint returned a non-success response.
    #[error("http status {status}: {message}")]
    Http {
        /// HTTP response status code.
        status: u32,
        /// Server diagnostic body.
        message: String,
    },
    /// A synchronous socket or compression operation failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The curl transfer or configuration failed.
    #[cfg(feature = "http")]
    #[error("curl: {0}")]
    Curl(String),
    /// A wire type or operation is not supported by the selected codec.
    #[error("unsupported: {0}")]
    Unsupported(Cow<'static, str>),

    /// The endpoint text could not be parsed.
    #[error("invalid endpoint `{endpoint}`: {message}")]
    Endpoint {
        /// Original endpoint text.
        endpoint: Cow<'static, str>,
        /// Why parsing failed.
        message: Cow<'static, str>,
    },
    /// The endpoint uses a transport that is not implemented yet.
    #[error("unsupported transport `{scheme}://`")]
    UnsupportedScheme {
        /// Scheme without the `://` separator.
        scheme: Cow<'static, str>,
    },
    /// `clickhouse-cpp` rejected or failed the operation.
    #[error("native transport failure: {message}")]
    Native {
        /// Diagnostic text from the bridge.
        message: Cow<'static, str>,
    },
    /// A native block column was missing, mismatched, or misaligned.
    #[error("block failure: {message}")]
    Block {
        /// Diagnostic text for the projection failure.
        message: Cow<'static, str>,
    },
    /// A configuration or fixture precondition was rejected.
    #[error("configuration rejected: {reason}")]
    Config {
        /// Why the configuration is invalid.
        reason: Cow<'static, str>,
    },
    /// A schema or derived row contract does not match the block.
    #[error("schema mismatch: {reason}")]
    SchemaMismatch {
        /// Optional table name for context.
        table: Option<Cow<'static, str>>,
        /// Why the schema is invalid.
        reason: Cow<'static, str>,
    },
    /// A fixed-width column could not be projected without copying.
    #[error("zero-copy cast failed for column {col_idx} `{col_name}`: {reason}")]
    ZeroCopyCast {
        /// Column index in the block.
        col_idx: usize,
        /// Column name.
        col_name: Cow<'static, str>,
        /// Projection failure detail.
        reason: Cow<'static, str>,
    },
    /// A named column is absent from the block.
    #[error("column `{col_name}` not found")]
    ColumnNotFound {
        /// Missing column name.
        col_name: Cow<'static, str>,
    },
    /// A row index is outside the projected block.
    #[error("row {row_idx} is out of bounds (row_count {row_count})")]
    RowIndexOutOfBounds {
        /// Requested row index.
        row_idx: usize,
        /// Number of rows in the projection.
        row_count: usize,
    },
    /// A request or endpoint violates a documented contract.
    #[error("invalid request: {0}")]
    Invalid(Cow<'static, str>),
    /// The server returned its structured exception packet.
    #[error("server exception {0:?}")]
    Server(ServerException),
    /// Upstream interactive inserts do not accept query event handlers.
    #[error("interactive inserts do not support query event callbacks")]
    InsertEventCallbacks,
    /// A contained callback panic interrupted the operation.
    #[error("query callback panicked; session cleanup was requested")]
    CallbackPanicked,
    /// An explicit reset is required before another request may run.
    #[error("connection is unusable after cleanup failure; reset it explicitly")]
    ConnectionPoisoned,
    /// Drop cleanup failed; retrieve the error before submitting another query.
    #[error("previous session cleanup failed; call take_cleanup_error")]
    PendingCleanup,
    /// Both the original cleanup and the recovery attempt failed.
    #[error("session cleanup failed: {operation}; recovery failed: {recovery}")]
    Cleanup {
        /// Original cancellation or reset failure.
        operation: Box<Error>,
        /// Subsequent recovery failure.
        recovery: Box<Error>,
    },
    /// Every worker slot is already leased by another live thread.
    #[error("all {workers} worker slots are leased by other live threads")]
    WorkerSlotsExhausted {
        /// Configured number of worker slots.
        workers: usize,
    },
    /// A serde value could not be encoded as RowBinary or a block column.
    #[error("serde bridge failure: {message}")]
    Serde {
        /// Diagnostic text for the rejected serde shape.
        message: Cow<'static, str>,
    },
}

impl Error {
    pub(crate) fn protocol(reason: impl Into<Cow<'static, str>>) -> Self {
        Self::Protocol(reason.into())
    }
    pub(crate) fn unsupported(reason: impl Into<Cow<'static, str>>) -> Self {
        Self::Unsupported(reason.into())
    }
    #[cfg(feature = "http")]
    pub(crate) fn curl(error: impl std::fmt::Display) -> Self {
        Self::Curl(format!("{error}"))
    }

    pub(crate) fn endpoint(
        endpoint: Cow<'static, str>,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self::Endpoint {
            endpoint,
            message: message.into(),
        }
    }

    pub(crate) fn native(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Native {
            message: message.into(),
        }
    }

    pub(crate) fn block(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Block {
            message: message.into(),
        }
    }

    pub(crate) fn serde(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Serde {
            message: message.into(),
        }
    }
}

#[cfg(feature = "tcp")]
impl From<cxx::Exception> for Error {
    fn from(error: cxx::Exception) -> Self {
        Self::Native {
            message: Cow::Owned(String::from(error.what())),
        }
    }
}

#[cfg(feature = "tcp")]
impl From<crate::WireError> for Error {
    fn from(error: crate::WireError) -> Self {
        use crate::WireError as W;
        match error {
            W::Native(message) => Self::native(message),
            W::UnsupportedColumnType { type_name } => Self::UnsupportedColumnType { type_name },
            W::UnknownColumn(col_name) => Self::ColumnNotFound {
                col_name: col_name.into(),
            },
            W::TypeMismatch {
                column,
                expected,
                actual,
            } => schema(format!(
                "column '{column}' has type '{actual}', expected '{expected}'"
            )),
            W::Unsupported(message) => Self::unsupported(message),
            W::IndexOutOfBounds { index, len } => Self::RowIndexOutOfBounds {
                row_idx: index,
                row_count: len,
            },
            error @ (W::Alignment | W::InvalidLength { .. }) => Self::ZeroCopyCast {
                // A standalone wire error has no block index or column name.
                col_idx: usize::MAX,
                col_name: Cow::Borrowed("<unknown>"),
                reason: error.to_string().into(),
            },
            error @ W::Utf8(_) => Self::protocol(error.to_string()),
        }
    }
}

/// Construct a schema error without allocating for static diagnostics.
pub(crate) fn schema(reason: impl Into<Cow<'static, str>>) -> Error {
    Error::SchemaMismatch {
        table: None,
        reason: reason.into(),
    }
}

#[cfg(all(test, feature = "tcp"))]
mod wire_conversion_tests {
    use super::*;
    use crate::WireError;

    #[test]
    fn conversion_preserves_categories_and_diagnostics() {
        assert!(
            matches!(Error::from(WireError::Native("failure".into())), Error::Native { message } if message == "failure")
        );
        assert!(
            matches!(Error::from(WireError::UnknownColumn("missing".into())), Error::ColumnNotFound { col_name } if col_name == "missing")
        );
        assert!(matches!(
            Error::from(WireError::IndexOutOfBounds { index: 7, len: 2 }),
            Error::RowIndexOutOfBounds {
                row_idx: 7,
                row_count: 2
            }
        ));
        assert!(
            matches!(Error::from(WireError::UnsupportedColumnType {type_name: "Tuple(UInt64)".into()}), Error::UnsupportedColumnType {type_name} if type_name == "Tuple(UInt64)")
        );
        let mismatch = WireError::TypeMismatch {
            column: "price".into(),
            expected: "UInt64".into(),
            actual: "String".into(),
        };
        let diagnostic = mismatch.to_string();
        assert!(
            matches!(Error::from(mismatch), Error::SchemaMismatch { reason, .. } if reason == diagnostic)
        );
        for error in [
            WireError::Alignment,
            WireError::InvalidLength {
                len: 7,
                element_size: 4,
            },
        ] {
            let diagnostic = error.to_string();
            assert!(
                matches!(Error::from(error), Error::ZeroCopyCast { col_idx: usize::MAX, reason, .. } if reason == diagnostic)
            );
        }
        let utf8 = WireError::Utf8("label".into());
        let diagnostic = utf8.to_string();
        assert!(matches!(Error::from(utf8), Error::Protocol(reason) if reason == diagnostic));
        assert!(
            matches!(Error::from(WireError::Unsupported("shape".into())), Error::Unsupported(reason) if reason == "shape")
        );
    }
}
