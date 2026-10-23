//! Serde adapters for RowBinary payloads and owned block columns.
//!
//! Types that derive only `serde::Serialize` and `serde::Deserialize` can be
//! written and read without `#[derive(Row)]`. RowBinary is a positional wire
//! format; block conversion uses serde field names as column names.

pub(crate) mod column_buffer;
mod fingerprint;
pub use fingerprint::{deduplication_token, fingerprint_block, fnv1a64};
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

    /// Decode RowBinary rows one at a time and pass each owned value to
    /// `on_row`.
    ///
    /// No intermediate `Vec` is allocated. The returned count is the number of
    /// values delivered, and a callback error stops decoding and is returned
    /// unchanged. Rows already delivered stay delivered.
    ///
    /// # Errors
    ///
    /// Truncated input, trailing partial rows, a value that does not match
    /// `T`, or a callback error returns an error.
    pub fn from_rowbinary_with<T, F>(bytes: &[u8], on_row: F) -> Result<usize, Error>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(T) -> Result<(), Error>,
    {
        rowbinary::from_rowbinary_with(bytes, on_row)
    }

    /// Decode concatenated RowBinary rows and append them to `dst`.
    ///
    /// Existing elements are kept and no intermediate `Vec` is allocated. The
    /// returned count is the number of rows appended; on a decoding error the
    /// rows decoded before the error remain appended.
    ///
    /// # Errors
    ///
    /// Truncated input, trailing partial rows, or a value that does not match
    /// `T` returns an error.
    pub fn from_rowbinary_into<T: serde::de::DeserializeOwned>(
        bytes: &[u8],
        dst: &mut Vec<T>,
    ) -> Result<usize, Error> {
        rowbinary::from_rowbinary_into(bytes, dst)
    }

    /// Decode RowBinary rows into `dst` starting at index zero.
    ///
    /// No intermediate `Vec` is allocated. Every written slot is overwritten
    /// and its previous value dropped; slots past the returned count are left
    /// untouched. The input must not contain more rows than `dst.len()`; when
    /// that limit is exceeded, slots filled before the error stay overwritten.
    ///
    /// # Errors
    ///
    /// Input with more rows than `dst.len()`, truncated input, trailing
    /// partial rows, or a value that does not match `T` returns an error.
    pub fn from_rowbinary_into_slice<T: serde::de::DeserializeOwned>(
        bytes: &[u8],
        dst: &mut [T],
    ) -> Result<usize, Error> {
        rowbinary::from_rowbinary_into_slice(bytes, dst)
    }

    /// Build a block whose columns are inferred from serde struct fields.
    ///
    /// Field names become column names. `Option<T>` becomes `Nullable(T)` and
    /// `Vec<T>` becomes `Array(T)`; byte strings share `String` storage.
    /// Fields containing only nulls or empty collections lack an inferable leaf
    /// type and are rejected instead of inventing a storage schema.
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

    /// Deserialize block rows one at a time and pass each owned value to
    /// `on_row`.
    ///
    /// No intermediate `Vec` is allocated. The returned count is the number of
    /// values delivered, and a callback error stops decoding and is returned
    /// unchanged.
    ///
    /// # Errors
    ///
    /// Missing fields, incompatible column shapes, or a callback error returns
    /// an error.
    pub fn block_to_rows_with<T, F>(block: &Block, on_row: F) -> Result<usize, Error>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(T) -> Result<(), Error>,
    {
        block_read::block_to_rows_with(block, on_row)
    }

    /// Deserialize block rows and append them to `dst`.
    ///
    /// Existing elements are kept and no intermediate `Vec` is allocated. The
    /// returned count is the number of rows appended; on a decoding error the
    /// rows decoded before the error remain appended.
    ///
    /// # Errors
    ///
    /// Missing fields or incompatible column shapes return an error.
    pub fn block_to_rows_into<T: serde::de::DeserializeOwned>(
        block: &Block,
        dst: &mut Vec<T>,
    ) -> Result<usize, Error> {
        block_read::block_to_rows_into(block, dst)
    }

    /// Deserialize block rows into `dst` starting at index zero.
    ///
    /// No intermediate `Vec` is allocated. Every written slot is overwritten
    /// and its previous value dropped; slots past the returned count are left
    /// untouched. A block with more rows than `dst.len()` is rejected before
    /// any slot is written.
    ///
    /// # Errors
    ///
    /// A block with more rows than `dst.len()`, missing fields, or
    /// incompatible column shapes returns an error.
    pub fn block_to_rows_into_slice<T: serde::de::DeserializeOwned>(
        block: &Block,
        dst: &mut [T],
    ) -> Result<usize, Error> {
        block_read::block_to_rows_into_slice(block, dst)
    }
    /// Hash arbitrary bytes with FNV-1a.
    pub fn fnv1a64(bytes: &[u8]) -> u64 {
        fnv1a64(bytes)
    }
    /// Fingerprint the canonical HTTP Native block encoding.
    /// # Errors
    /// Invalid storage or unsupported types reject encoding.
    pub fn fingerprint_block(block: &Block) -> Result<u64, Error> {
        fingerprint_block(block)
    }
    /// Return a hexadecimal insert deduplication token.
    /// # Errors
    /// Invalid storage or unsupported types reject encoding.
    pub fn deduplication_token(block: &Block) -> Result<String, Error> {
        deduplication_token(block)
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
