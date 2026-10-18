//! Variable-length String, FixedString, and JSON columns.

use clickhouse_sys as sys;
use cxx::UniquePtr;

use crate::cpp_error::{CppError, CppResult};

/// A String or JSON column; both store variable-length UTF-8 payloads.
pub struct StringColumn {
    handle: UniquePtr<sys::ColumnHandle>,
}

impl StringColumn {
    /// Create an empty String column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new() -> CppResult<Self> {
        Self::from_name("String")
    }

    /// Create an empty JSON column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new_json() -> CppResult<Self> {
        Self::from_name("JSON")
    }

    fn from_name(name: &str) -> CppResult<Self> {
        Ok(Self {
            handle: sys::new_column(name)?,
        })
    }

    /// Append one UTF-8 string.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_str(&mut self, value: &str) -> CppResult<()> {
        self.append_bytes(value.as_bytes())
    }

    /// Append one byte string.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_bytes(&mut self, value: &[u8]) -> CppResult<()> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("String column handle is null".to_owned()))?;
        handle.append_string(value).map_err(CppError::from)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    /// Borrow one row as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn at(&self, index: usize) -> CppResult<&str> {
        let bytes = self.bytes_at(index)?;
        core::str::from_utf8(bytes).map_err(|_| CppError::Utf8(self.type_name()))
    }

    /// Borrow one row as bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn bytes_at(&self, index: usize) -> CppResult<&[u8]> {
        if index >= self.len() {
            return Err(CppError::IndexOutOfBounds {
                index,
                len: self.len(),
            });
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("String column handle is null".to_owned()))?;
        Ok(handle.string_at(index))
    }

    pub(crate) fn cpp_handle(&self) -> &sys::ColumnHandle {
        // The handle is created in the constructors and never reset.
        self.handle.as_ref().expect("String column handle")
    }
}

/// A fixed-length byte string column.
pub struct FixedStringColumn {
    handle: UniquePtr<sys::ColumnHandle>,
    width: usize,
}

impl FixedStringColumn {
    /// Create an empty FixedString column with the given byte width.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(width: usize) -> CppResult<Self> {
        let name = format!("FixedString({width})");
        let handle = sys::new_column(&name)?;
        Ok(Self { handle, width })
    }

    /// Append one value; shorter values are zero-padded to the fixed width.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append(&mut self, value: &[u8]) -> CppResult<()> {
        let mut padded = vec![0_u8; self.width];
        let copied = value.len().min(self.width);
        padded[..copied].copy_from_slice(&value[..copied]);
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("FixedString column handle is null".to_owned()))?;
        handle.append_fixed(&padded).map_err(CppError::from)
    }

    /// Append one UTF-8 value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_str(&mut self, value: &str) -> CppResult<()> {
        self.append(value.as_bytes())
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fixed byte width.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    /// Borrow one row.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn at(&self, index: usize) -> CppResult<&[u8]> {
        if index >= self.len() {
            return Err(CppError::IndexOutOfBounds {
                index,
                len: self.len(),
            });
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("FixedString column handle is null".to_owned()))?;
        Ok(handle.string_at(index))
    }

    pub(crate) fn cpp_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("FixedString column handle")
    }
}
