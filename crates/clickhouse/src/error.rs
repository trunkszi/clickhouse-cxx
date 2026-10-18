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

/// Construct a schema error without allocating for static diagnostics.
pub(crate) fn schema(reason: impl Into<Cow<'static, str>>) -> Error {
    Error::SchemaMismatch {
        table: None,
        reason: reason.into(),
    }
}
