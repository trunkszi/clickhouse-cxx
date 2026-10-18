//! A column-oriented ClickHouse block backed by the native client.

use std::{cell::RefCell, marker::PhantomData};

use clickhouse_sys as sys;
use cxx::{SharedPtr, UniquePtr};
use zerocopy::FromBytes;

use crate::{
    column::{FixedKind, FixedType, TypeCode, WireColumn},
    wire_error::{WireError, WireResult},
};

/// A column-oriented chunk of ClickHouse data.
pub struct WireBlock {
    handle: UniquePtr<sys::BlockHandle>,
    names: Vec<String>,
    /// Cached column wrappers keep their aligned projection caches alive for
    /// as long as the returned slices borrow this block.
    handles: RefCell<Vec<Option<SharedPtr<sys::ColumnHandle>>>>,
    /// The block and its column handles are single-threaded.
    _not_send: PhantomData<*const ()>,
}

impl WireBlock {
    /// Create an empty WireBlock.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new() -> WireResult<Self> {
        Self::from_wire(sys::new_block())
    }

    pub(crate) fn from_wire(handle: UniquePtr<sys::BlockHandle>) -> WireResult<Self> {
        let count = handle.column_count();
        let mut names = Vec::with_capacity(count);
        for index in 0..count {
            names.push(handle.column_name(index).map_err(WireError::from)?);
        }
        Ok(Self {
            handle,
            names,
            handles: RefCell::new(vec![None; count]),
            _not_send: PhantomData,
        })
    }

    pub(crate) fn wire_handle(&self) -> WireResult<&sys::BlockHandle> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("block handle is null".to_owned()))
    }

    /// Append a named column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append(&mut self, name: &str, column: WireColumn) -> WireResult<()> {
        let handle = column.wire_handle().ok_or_else(|| {
            WireError::Unsupported(format!(
                "column type '{}' cannot be appended to a native wire block",
                column.type_name()
            ))
        })?;
        self.handle
            .pin_mut()
            .append_column(name, handle)
            .map_err(WireError::from)?;
        self.names.push(name.to_owned());
        self.handles.borrow_mut().push(None);
        Ok(())
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.handle.column_count()
    }

    /// Number of rows.
    pub fn row_count(&self) -> usize {
        self.handle.row_count()
    }

    /// WireColumn names in insertion order.
    pub fn column_names(&self) -> &[String] {
        &self.names
    }

    /// Reset all columns while retaining their capacity.
    pub fn clear(&mut self) {
        self.handle.pin_mut().clear();
    }

    /// Reserve capacity for all columns.
    pub fn reserve(&mut self, capacity: usize) {
        self.handle.pin_mut().reserve(capacity);
    }

    fn index_of(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|candidate| candidate == name)
    }

    /// Shared handle for one column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub(crate) fn column_handle(&self, name: &str) -> WireResult<SharedPtr<sys::ColumnHandle>> {
        let index = self
            .index_of(name)
            .ok_or_else(|| WireError::UnknownColumn(name.to_owned()))?;
        let mut handles = self.handles.borrow_mut();
        if handles[index].is_none() {
            handles[index] = Some(self.handle.column(index).map_err(WireError::from)?);
        }
        handles[index]
            .as_ref()
            .cloned()
            .ok_or_else(|| WireError::Unsupported("column handle is null".to_owned()))
    }

    /// ClickHouse type code of one column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_type_code(&self, name: &str) -> WireResult<TypeCode> {
        let index = self
            .index_of(name)
            .ok_or_else(|| WireError::UnknownColumn(name.to_owned()))?;
        TypeCode::from_i32(self.handle.column_type_code(index)?)
    }

    /// ClickHouse type name of one column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_type_name(&self, name: &str) -> WireResult<String> {
        let index = self
            .index_of(name)
            .ok_or_else(|| WireError::UnknownColumn(name.to_owned()))?;
        self.handle.column_type_name(index).map_err(WireError::from)
    }

    /// Project a fixed-width column as a zero-copy slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_slice<T: FixedType>(&self, name: &str) -> WireResult<&[T]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("column handle is null".to_owned()))?;
        let actual = TypeCode::from_i32(column.type_code())?;
        let actual_name = column.type_name();
        if !T::KIND.matches(actual, &actual_name) {
            return Err(WireError::TypeMismatch {
                column: name.to_owned().into(),
                expected: format!("{:?}", T::KIND).into(),
                actual: actual_name,
            });
        }
        let bytes = column.fixed_bytes();
        // SAFETY: the C++ WireBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        let bytes: &[u8] = unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) };
        <[T]>::ref_from_bytes(bytes).map_err(|_| WireError::Alignment)
    }

    /// Copy a fixed-width column into an aligned Rust vector.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_to_vec<T: FixedType>(&self, name: &str) -> WireResult<Vec<T>> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("column handle is null".to_owned()))?;
        let actual = TypeCode::from_i32(column.type_code())?;
        let actual_name = column.type_name();
        if !T::KIND.matches(actual, &actual_name) {
            return Err(WireError::TypeMismatch {
                column: name.to_owned().into(),
                expected: format!("{:?}", T::KIND).into(),
                actual: actual_name,
            });
        }
        let bytes = column.fixed_bytes();
        let size = core::mem::size_of::<T>();
        if size == 0 || bytes.len() % size != 0 {
            return Err(WireError::InvalidLength {
                len: bytes.len(),
                element_size: size,
            });
        }
        let mut values = Vec::with_capacity(bytes.len() / size);
        for chunk in bytes.chunks_exact(size) {
            values.push(T::read_from_bytes(chunk).map_err(|_| WireError::Alignment)?);
        }
        Ok(values)
    }

    /// Read one item as bytes from a variable or fixed-width column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_bytes(&self, name: &str, index: usize) -> WireResult<&[u8]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("column handle is null".to_owned()))?;
        let len = column.size();
        if index >= len {
            return Err(WireError::IndexOutOfBounds { index, len });
        }
        let bytes = column.string_at(index);
        // SAFETY: the C++ WireBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        Ok(unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) })
    }

    /// Read one item as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_str(&self, name: &str, index: usize) -> WireResult<&str> {
        let bytes = self.column_bytes(name, index)?;
        core::str::from_utf8(bytes).map_err(|_| WireError::Utf8(name.to_owned()))
    }

    /// Read the null bitmap of a Nullable column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_nulls(&self, name: &str) -> WireResult<&[u8]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("column handle is null".to_owned()))?;
        if TypeCode::from_i32(column.type_code())? != TypeCode::Nullable {
            return Err(WireError::Unsupported(format!(
                "column '{name}' is not Nullable"
            )));
        }
        let nulls = column.nulls();
        // SAFETY: the C++ WireBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        Ok(unsafe { core::mem::transmute::<&[u8], &[u8]>(nulls) })
    }

    /// Read the offsets of an Array column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_offsets(&self, name: &str) -> WireResult<&[u64]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("column handle is null".to_owned()))?;
        if TypeCode::from_i32(column.type_code())? != TypeCode::Array {
            return Err(WireError::Unsupported(format!(
                "column '{name}' is not Array"
            )));
        }
        let offsets = column.array_offsets();
        // SAFETY: the C++ WireBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        Ok(unsafe { core::mem::transmute::<&[u64], &[u64]>(offsets) })
    }

    /// Fixed-width kind of one column, if it has one.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn fixed_kind(&self, name: &str) -> WireResult<FixedKind> {
        let type_name = self.column_type_name(name)?;
        crate::type_layout::fixed_kind(&type_name).ok_or_else(|| {
            WireError::Unsupported(format!("column '{name}' has non-fixed type '{type_name}'"))
        })
    }
}
