//! Serde adapters for RowBinary payloads and owned block columns.
//!
//! Types that derive only `serde::Serialize` and `serde::Deserialize` can be
//! written and read without `#[derive(Row)]`. RowBinary is a positional wire
//! format; block conversion uses serde field names as column names.

mod block_read;
mod block_write;
mod rowbinary;
mod varint;

use std::borrow::Cow;

use crate::{block::Block, error::Error};

/// Adapts serde structs to RowBinary payloads and owned block columns.
pub struct SerdeBridge;

impl SerdeBridge {
    /// Encode rows as concatenated ClickHouse RowBinary values.
    ///
    /// Lengths use unsigned LEB128, `Option` uses a tag byte where `1` means
    /// NULL and `0` means present, and fixed-width values are little-endian.
    /// The returned names are the struct field names captured from the first
    /// row.
    ///
    /// # Errors
    ///
    /// A value outside the supported serde shapes, or a row whose fields
    /// disagree with the first row, returns an error.
    pub fn to_rowbinary<T: serde::Serialize>(
        rows: &[T],
    ) -> Result<(Vec<Cow<'static, str>>, Vec<u8>), Error> {
        rowbinary::to_rowbinary(rows)
    }

    /// Decode concatenated RowBinary rows into owned values.
    ///
    /// # Errors
    ///
    /// Truncated input, trailing partial rows, or a value that does not match
    /// `T` return an error.
    pub fn from_rowbinary<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
        rowbinary::from_rowbinary(bytes)
    }

    /// Build a block whose columns are inferred from serde struct fields.
    ///
    /// Field names become column names. `Option<T>` becomes `Nullable(T)` and
    /// `Vec<T>` becomes `Array(T)`; byte strings share `String` storage.
    /// Fields that only ever hold `None` or empty sequences have no element
    /// type to infer, so they fall back to `Nullable(String)` or `Array(String)`.
    ///
    /// # Errors
    ///
    /// Unsupported field shapes, or rows that disagree on field names or
    /// types, return an error.
    pub fn rows_to_block<T: serde::Serialize>(rows: &[T]) -> Result<Block, Error> {
        block_write::rows_to_block(rows)
    }

    /// Deserialize every block row by matching column names to struct fields.
    ///
    /// # Errors
    ///
    /// Missing fields, incompatible column storage or types, invalid null
    /// maps, and invalid array offsets return an error.
    pub fn block_to_rows<T: serde::de::DeserializeOwned>(block: &Block) -> Result<Vec<T>, Error> {
        block_read::block_to_rows(block)
    }
}

/// A serde error carrying the diagnostic text for the bridge.
#[derive(Debug)]
pub(crate) struct SerError(pub String);

impl SerError {
    pub(crate) fn custom(message: impl std::fmt::Display) -> Self {
        Self(format!("{message}"))
    }
}

impl std::fmt::Display for SerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SerError {}

impl serde::ser::Error for SerError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self(format!("{message}"))
    }
}

impl serde::de::Error for SerError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self(format!("{message}"))
    }
}

impl From<SerError> for Error {
    fn from(error: SerError) -> Self {
        Error::serde(error.0)
    }
}
