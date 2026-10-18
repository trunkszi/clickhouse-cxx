//! Structured schema, transport, and borrowed projection errors.

use std::borrow::Cow;

/// A rejected configuration, schema, transport, or checked projection.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
    /// The native client rejected or failed the operation.
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
    /// A serde value could not be encoded as RowBinary or a block column.
    #[error("serde bridge failure: {message}")]
    Serde {
        /// Diagnostic text for the rejected serde shape.
        message: Cow<'static, str>,
    },
}

impl Error {
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

/// A synchronous SDK operation result.
pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Construct a schema error without allocating for static diagnostics.
pub(crate) fn schema(reason: impl Into<Cow<'static, str>>) -> Error {
    Error::SchemaMismatch {
        table: None,
        reason: reason.into(),
    }
}
