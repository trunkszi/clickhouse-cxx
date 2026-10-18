//! Native transport blocks with zero-copy fixed-width projections.

use std::marker::PhantomData;

use cxx::{SharedPtr, UniquePtr};
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::{WireColumn, error::Error, sys};

const KIND_U8: u32 = 1;
const KIND_U16: u32 = 2;
const KIND_U32: u32 = 3;
const KIND_U64: u32 = 4;
const KIND_I8: u32 = 5;
const KIND_I16: u32 = 6;
const KIND_I32: u32 = 7;
const KIND_I64: u32 = 8;
const KIND_F32: u32 = 9;
const KIND_F64: u32 = 10;

/// A fixed-width primitive accepted by [`crate::Block::column_slice`].
pub trait FixedColumn: FromBytes + Immutable + KnownLayout + Copy {
    /// Kind tag shared with the C++ bridge.
    const KIND: u32;
}

macro_rules! fixed_column {
    ($type:ty, $kind:expr) => {
        impl FixedColumn for $type {
            const KIND: u32 = $kind;
        }
    };
}

fixed_column!(u8, KIND_U8);
fixed_column!(u16, KIND_U16);
fixed_column!(u32, KIND_U32);
fixed_column!(u64, KIND_U64);
fixed_column!(i8, KIND_I8);
fixed_column!(i16, KIND_I16);
fixed_column!(i32, KIND_I32);
fixed_column!(i64, KIND_I64);
fixed_column!(f32, KIND_F32);
fixed_column!(f64, KIND_F64);

macro_rules! push_column {
    ($block:ident, $ffi:ident, $type:ty, $name:ident, $values:ident) => {{
        let values: &[$type] = $values;
        if $block.rows != 0 && $block.rows != values.len() {
            return Err(Error::block(format!(
                "column `{}` has {} rows, expected {}",
                $name,
                values.len(),
                $block.rows
            )));
        }
        sys::ffi::$ffi($block.inner.pin_mut(), $name, values)
            .map_err(|error| Error::block(format!("append column `{}`: {error}", $name)))?;
        if $block.rows == 0 {
            $block.rows = values.len();
        }
        Ok(())
    }};
}

/// A column-oriented block owned by the native transport.
pub struct NativeBlock {
    inner: UniquePtr<sys::ffi::Block>,
    rows: usize,
    /// The block owns C++ columns that must stay on one thread.
    _not_send: PhantomData<*const ()>,
}

impl NativeBlock {
    /// Create an empty block.
    pub fn new() -> Self {
        Self {
            inner: sys::ffi::block_new(),
            rows: 0,
            _not_send: PhantomData,
        }
    }

    pub(crate) fn from_inner(inner: UniquePtr<sys::ffi::Block>) -> Self {
        let rows = sys::ffi::block_rows(&inner);
        Self {
            inner,
            rows,
            _not_send: PhantomData,
        }
    }

    pub(crate) fn inner(&self) -> &sys::ffi::Block {
        &self.inner
    }

    /// Number of rows tracked by this builder.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns.
    pub fn columns(&self) -> usize {
        sys::ffi::block_columns(&self.inner)
    }

    /// Name of a column by index.
    pub fn column_name(&self, index: usize) -> String {
        sys::ffi::block_column_name(&self.inner, index)
    }

    /// Kind tag of a column by index.
    pub fn column_kind(&self, index: usize) -> u32 {
        sys::ffi::block_column_kind(&self.inner, index)
    }

    /// Server type name of a column by index.
    ///
    /// # Errors
    /// Returns [`Error::Block`] when the index is outside the schema.
    pub fn column_type(&self, index: usize) -> Result<String, Error> {
        sys::ffi::block_column_type(&self.inner, index)
            .map_err(|error| Error::block(format!("column type failed: {error}")))
    }

    /// Append a `UInt8` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_u8(&mut self, name: &str, values: &[u8]) -> Result<(), Error> {
        push_column!(self, block_append_u8, u8, name, values)
    }

    /// Append a `UInt16` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_u16(&mut self, name: &str, values: &[u16]) -> Result<(), Error> {
        push_column!(self, block_append_u16, u16, name, values)
    }

    /// Append a `UInt32` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_u32(&mut self, name: &str, values: &[u32]) -> Result<(), Error> {
        push_column!(self, block_append_u32, u32, name, values)
    }

    /// Append a `UInt64` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_u64(&mut self, name: &str, values: &[u64]) -> Result<(), Error> {
        push_column!(self, block_append_u64, u64, name, values)
    }

    /// Append an `Int8` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_i8(&mut self, name: &str, values: &[i8]) -> Result<(), Error> {
        push_column!(self, block_append_i8, i8, name, values)
    }

    /// Append an `Int16` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_i16(&mut self, name: &str, values: &[i16]) -> Result<(), Error> {
        push_column!(self, block_append_i16, i16, name, values)
    }

    /// Append an `Int32` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_i32(&mut self, name: &str, values: &[i32]) -> Result<(), Error> {
        push_column!(self, block_append_i32, i32, name, values)
    }

    /// Append an `Int64` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_i64(&mut self, name: &str, values: &[i64]) -> Result<(), Error> {
        push_column!(self, block_append_i64, i64, name, values)
    }

    /// Append a `Float32` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_f32(&mut self, name: &str, values: &[f32]) -> Result<(), Error> {
        push_column!(self, block_append_f32, f32, name, values)
    }

    /// Append a `Float64` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_f64(&mut self, name: &str, values: &[f64]) -> Result<(), Error> {
        push_column!(self, block_append_f64, f64, name, values)
    }

    /// Append a `UInt64` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn append_u64(&mut self, name: &str, values: &[u64]) -> Result<(), Error> {
        self.push_u64(name, values)
    }

    /// Append an `Int64` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn append_i64(&mut self, name: &str, values: &[i64]) -> Result<(), Error> {
        self.push_i64(name, values)
    }

    /// Append a `Float64` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn append_f64(&mut self, name: &str, values: &[f64]) -> Result<(), Error> {
        self.push_f64(name, values)
    }

    /// Append a `String` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column or the native append fails.
    pub fn push_string(&mut self, name: &str, values: &[String]) -> Result<(), Error> {
        push_column!(self, block_append_string, String, name, values)
    }

    /// Append a `String` column from validated flat offsets and bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column, the offsets are invalid, or the native append fails.
    pub fn push_string_flat(
        &mut self,
        name: &str,
        offsets: &[u64],
        bytes: &[u8],
    ) -> Result<(), Error> {
        if self.rows != 0 && self.rows != offsets.len() {
            return Err(Error::block(format!(
                "column `{name}` has {} rows, expected {}",
                offsets.len(),
                self.rows
            )));
        }
        sys::ffi::block_append_string_flat(self.inner.pin_mut(), name, offsets, bytes)
            .map_err(|error| Error::block(format!("append column `{name}`: {error}")))?;
        if self.rows == 0 {
            self.rows = offsets.len();
        }
        Ok(())
    }

    /// Append a column from the 17-family C++ matrix.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the row count conflicts with an earlier
    /// column, the column has no C++ handle, or the native append fails.
    pub fn push_wire_column(&mut self, name: &str, column: &WireColumn) -> Result<(), Error> {
        if self.rows != 0 && self.rows != column.len() {
            return Err(Error::block(format!(
                "column `{name}` has {} rows, expected {}",
                column.len(),
                self.rows
            )));
        }
        let handle = column.wire_handle().ok_or_else(|| {
            Error::block(format!("column `{name}` has no native C++ representation"))
        })?;
        sys::ffi::block_append_column_handle(self.inner.pin_mut(), name, handle)
            .map_err(|error| Error::block(format!("append column `{name}`: {error}")))?;
        if self.rows == 0 {
            self.rows = column.len();
        }
        Ok(())
    }

    /// Borrow one native column as a matrix handle.
    pub(crate) fn wire_column(&self, index: usize) -> SharedPtr<sys::ColumnHandle> {
        sys::ffi::block_column_handle(&self.inner, index)
    }

    /// Drop all rows while keeping allocated column capacity.
    pub fn clear(&mut self) {
        sys::ffi::block_clear(self.inner.pin_mut());
        self.rows = 0;
    }

    /// Recompute the native row count before sending the block.
    pub(crate) fn refresh(&mut self) -> Result<(), Error> {
        sys::ffi::block_refresh(self.inner.pin_mut())
            .map_err(|error| Error::block(format!("row refresh failed: {error}")))
    }

    /// Retained element capacity of a fixed-width column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the column is missing or variable-length.
    pub fn column_capacity(&self, name: &str) -> Result<usize, Error> {
        let index = self.column_index(name)?;
        Ok(sys::ffi::block_column_capacity(&self.inner, index))
    }

    /// Project a fixed-width column as a zero-copy slice.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the column is missing, its kind does not
    /// match `T`, or the byte buffer is not a valid `T` slice.
    pub fn column_slice<T: FixedColumn>(&self, name: &str) -> Result<&[T], Error> {
        let index = self.column_index(name)?;
        self.column_slice_at(index)
    }

    /// Project a validated column index without materializing its name.
    fn column_slice_at<T: FixedColumn>(&self, index: usize) -> Result<&[T], Error> {
        let columns = self.columns();
        if index >= columns {
            return Err(Error::block(format!(
                "column index {index} is outside {columns} columns"
            )));
        }
        let kind = sys::ffi::block_column_kind(&self.inner, index);
        if kind != T::KIND {
            return Err(Error::block(format!(
                "column index {index} has kind {kind}, expected {}",
                T::KIND
            )));
        }
        let bytes = sys::ffi::block_column_bytes(&self.inner, index);
        if bytes.is_empty() {
            return Ok(&[]);
        }
        let address = bytes.as_ptr() as usize;
        let required = std::mem::align_of::<T>();
        if !address.is_multiple_of(required) {
            return Err(Error::block(format!(
                "column index {index} data pointer 0x{address:x} is not {required}-byte aligned \
                 (kind {kind}, bytes {})",
                bytes.len()
            )));
        }
        <[T]>::ref_from_bytes(bytes)
            .map_err(|error| Error::block(format!("column index {index} cast failed: {error:?}")))
    }

    /// Copy a fixed-width column into a caller-provided slice.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the index is out of range, the column kind
    /// does not match `T`, or `dst` is smaller than the column.
    pub fn read_into_slice<T: FixedColumn + Copy>(
        &self,
        col_idx: usize,
        dst: &mut [T],
    ) -> Result<usize, Error> {
        let src = self.column_slice_at::<T>(col_idx)?;
        if dst.len() < src.len() {
            return Err(Error::block(format!(
                "destination slice for column index {col_idx} has {} elements, needs {}",
                dst.len(),
                src.len()
            )));
        }
        dst[..src.len()].copy_from_slice(src);
        Ok(src.len())
    }

    /// Copy one String/FixedString row into a caller-provided byte buffer.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the column or row is out of range, the
    /// column is not a byte string, or the native handle is missing.
    pub fn read_bytes_into(
        &self,
        col_idx: usize,
        row_idx: usize,
        dst: &mut Vec<u8>,
    ) -> Result<usize, Error> {
        if col_idx >= self.columns() {
            return Err(Error::block(format!(
                "column index {col_idx} is out of range ({} columns)",
                self.columns()
            )));
        }
        if row_idx >= self.rows() {
            return Err(Error::block(format!(
                "row index {row_idx} is out of range ({} rows)",
                self.rows()
            )));
        }
        let name = self.column_name(col_idx);
        let kind = self.column_kind(col_idx);
        if kind != 11 && kind != 12 {
            return Err(Error::block(format!(
                "column `{name}` is not a String or FixedString column"
            )));
        }
        let handle = self.wire_column(col_idx);
        let handle = handle
            .as_ref()
            .ok_or_else(|| Error::block(format!("column `{name}` handle is null")))?;
        let bytes = handle.string_at(row_idx);
        dst.clear();
        dst.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    /// Copy one fixed-width column into caller-supplied storage.
    ///
    /// Copies `min(rows, dst.len())` elements and returns the number copied.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the column index is outside the schema,
    /// the column has a different fixed-width kind, or its native bytes cannot
    /// be viewed as `T`.
    pub fn copy_column_into_slice<T: FixedColumn>(
        &self,
        index: usize,
        dst: &mut [T],
    ) -> Result<usize, Error> {
        let source = self.column_slice_at::<T>(index)?;
        let copied = source.len().min(dst.len());
        dst[..copied].copy_from_slice(&source[..copied]);
        Ok(copied)
    }

    /// Materialize a `String` column.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Block`] when the column is missing or is not a
    /// `String` column.
    pub fn column_strings(&self, name: &str) -> Result<Vec<String>, Error> {
        let index = self.column_index(name)?;
        let kind = sys::ffi::block_column_kind(&self.inner, index);
        if kind != 11 {
            return Err(Error::block(format!(
                "column `{name}` is not a String column"
            )));
        }
        Ok(sys::ffi::block_column_strings(&self.inner, index))
    }

    /// Exclusive end offsets for a `String` column by index.
    pub fn column_string_offsets(&self, index: usize) -> Vec<u64> {
        sys::ffi::block_column_string_offsets(&self.inner, index)
    }

    /// Flat bytes for a `String` column by index.
    pub fn column_string_flat(&self, index: usize) -> Vec<u8> {
        sys::ffi::block_column_string_flat(&self.inner, index)
    }

    fn column_index(&self, name: &str) -> Result<usize, Error> {
        for index in 0..self.columns() {
            if self.column_name(index) == name {
                return Ok(index);
            }
        }
        Err(Error::block(format!("column `{name}` is missing")))
    }
}

impl Default for NativeBlock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_slices_keep_full_and_partial_copy_contracts() {
        let mut block = NativeBlock::new();
        block.push_u64("id", &[11, 22, 33]).unwrap();
        let mut full = [0_u64; 4];
        assert_eq!(block.read_into_slice(0, &mut full).unwrap(), 3);
        assert_eq!(full, [11, 22, 33, 0]);
        let mut short = [99_u64; 2];
        assert!(block.read_into_slice(0, &mut short).is_err());
        assert_eq!(short, [99, 99]);
        assert_eq!(block.copy_column_into_slice(0, &mut short).unwrap(), 2);
        assert_eq!(short, [11, 22]);
    }

    #[test]
    fn both_copy_paths_reject_invalid_columns_before_writing() {
        let mut block = NativeBlock::new();
        block.push_u64("id", &[11]).unwrap();
        let mut destination = [99_u32];
        assert!(block.read_into_slice(0, &mut destination).is_err());
        assert!(block.copy_column_into_slice(0, &mut destination).is_err());
        assert!(block.read_into_slice(1, &mut destination).is_err());
        assert!(block.copy_column_into_slice(1, &mut destination).is_err());
        assert_eq!(destination, [99]);
    }
}
