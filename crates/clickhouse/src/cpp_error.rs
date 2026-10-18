//! Errors returned by the C++ column and block wrappers.

use std::borrow::Cow;

/// Errors from the native `clickhouse-cpp` column matrix.
#[derive(Debug, thiserror::Error)]
pub enum CppError {
    /// An error raised by the C++ bridge.
    #[error("clickhouse-cpp error: {0}")]
    Cpp(String),
    /// A column name was not found in the block.
    #[error("unknown column: {0}")]
    UnknownColumn(String),
    /// A requested Rust type does not match the ClickHouse column type.
    #[error("column '{column}' has type '{actual}', expected '{expected}'")]
    TypeMismatch {
        /// Column name.
        column: Cow<'static, str>,
        /// Expected ClickHouse type.
        expected: Cow<'static, str>,
        /// Actual ClickHouse type.
        actual: String,
    },
    /// A String or JSON value was not valid UTF-8.
    #[error("column '{0}' contains invalid UTF-8")]
    Utf8(String),
    /// The bridge does not support this operation for the column type.
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    /// A row index was out of bounds.
    #[error("row index {index} is out of bounds for length {len}")]
    IndexOutOfBounds {
        /// Requested row index.
        index: usize,
        /// Column length.
        len: usize,
    },
    /// The C++ buffer was not aligned for the requested Rust type.
    #[error("column buffer is not aligned for the requested element type")]
    Alignment,
    /// A byte buffer had an invalid length for the element type.
    #[error("byte buffer length {len} is not a multiple of element size {element_size}")]
    InvalidLength {
        /// Buffer length in bytes.
        len: usize,
        /// Element size in bytes.
        element_size: usize,
    },
}

impl From<cxx::Exception> for CppError {
    fn from(error: cxx::Exception) -> Self {
        Self::Cpp(error.what().to_owned())
    }
}

/// Result alias for the native column matrix.
pub type CppResult<T> = std::result::Result<T, CppError>;
