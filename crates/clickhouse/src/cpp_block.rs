//! A column-oriented ClickHouse CppBlock backed by `clickhouse-cpp`.

use std::cell::RefCell;

use clickhouse_sys as sys;
use cxx::{SharedPtr, UniquePtr};
use zerocopy::FromBytes;

use crate::{
    cpp_column::{CppColumn, FixedKind, FixedType, TypeCode},
    cpp_error::{CppError, CppResult},
};

/// A column-oriented chunk of ClickHouse data.
pub struct CppBlock {
    handle: UniquePtr<sys::BlockHandle>,
    names: Vec<String>,
    /// Cached column wrappers keep their aligned projection caches alive for
    /// as long as the returned slices borrow this block.
    handles: RefCell<Vec<Option<SharedPtr<sys::ColumnHandle>>>>,
}

impl CppBlock {
    /// Create an empty CppBlock.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new() -> CppResult<Self> {
        Self::from_cpp(sys::new_block())
    }

    pub(crate) fn from_cpp(handle: UniquePtr<sys::BlockHandle>) -> CppResult<Self> {
        let count = handle.column_count();
        let mut names = Vec::with_capacity(count);
        for index in 0..count {
            names.push(handle.column_name(index).map_err(CppError::from)?);
        }
        Ok(Self {
            handle,
            names,
            handles: RefCell::new(vec![None; count]),
        })
    }

    pub(crate) fn cpp_handle(&self) -> &sys::BlockHandle {
        self.handle.as_ref().expect("block handle")
    }

    /// Append a named column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append(&mut self, name: &str, column: CppColumn) -> CppResult<()> {
        let handle = column.cpp_handle().ok_or_else(|| {
            CppError::Unsupported(format!(
                "column type '{}' cannot be appended to a clickhouse-cpp CppBlock",
                column.type_name()
            ))
        })?;
        self.handle
            .pin_mut()
            .append_column(name, handle)
            .map_err(CppError::from)?;
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

    /// CppColumn names in insertion order.
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
    pub fn column_handle(&self, name: &str) -> CppResult<SharedPtr<sys::ColumnHandle>> {
        let index = self
            .index_of(name)
            .ok_or_else(|| CppError::UnknownColumn(name.to_owned()))?;
        let mut handles = self.handles.borrow_mut();
        if handles[index].is_none() {
            handles[index] = Some(self.handle.column(index).map_err(CppError::from)?);
        }
        handles[index]
            .as_ref()
            .cloned()
            .ok_or_else(|| CppError::Unsupported("column handle is null".to_owned()))
    }

    /// ClickHouse type code of one column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_type_code(&self, name: &str) -> CppResult<TypeCode> {
        let index = self
            .index_of(name)
            .ok_or_else(|| CppError::UnknownColumn(name.to_owned()))?;
        TypeCode::from_i32(self.handle.column_type_code(index)?)
    }

    /// ClickHouse type name of one column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_type_name(&self, name: &str) -> CppResult<String> {
        let index = self
            .index_of(name)
            .ok_or_else(|| CppError::UnknownColumn(name.to_owned()))?;
        self.handle.column_type_name(index).map_err(CppError::from)
    }

    /// Project a fixed-width column as a zero-copy slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_slice<T: FixedType>(&self, name: &str) -> CppResult<&[T]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("column handle is null".to_owned()))?;
        let actual = TypeCode::from_i32(column.type_code())?;
        let actual_name = column.type_name();
        if !T::KIND.matches(actual, &actual_name) {
            return Err(CppError::TypeMismatch {
                column: name.to_owned().into(),
                expected: format!("{:?}", T::KIND).into(),
                actual: actual_name,
            });
        }
        let bytes = column.fixed_bytes();
        // SAFETY: the C++ CppBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        let bytes: &[u8] = unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) };
        <[T]>::ref_from_bytes(bytes).map_err(|_| CppError::Alignment)
    }

    /// Copy a fixed-width column into an aligned Rust vector.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_to_vec<T: FixedType>(&self, name: &str) -> CppResult<Vec<T>> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("column handle is null".to_owned()))?;
        let actual = TypeCode::from_i32(column.type_code())?;
        let actual_name = column.type_name();
        if !T::KIND.matches(actual, &actual_name) {
            return Err(CppError::TypeMismatch {
                column: name.to_owned().into(),
                expected: format!("{:?}", T::KIND).into(),
                actual: actual_name,
            });
        }
        let bytes = column.fixed_bytes();
        let size = core::mem::size_of::<T>();
        if size == 0 || bytes.len() % size != 0 {
            return Err(CppError::InvalidLength {
                len: bytes.len(),
                element_size: size,
            });
        }
        let mut values = Vec::with_capacity(bytes.len() / size);
        for chunk in bytes.chunks_exact(size) {
            values.push(T::read_from_bytes(chunk).map_err(|_| CppError::Alignment)?);
        }
        Ok(values)
    }

    /// Read one item as bytes from a variable or fixed-width column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_bytes(&self, name: &str, index: usize) -> CppResult<&[u8]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("column handle is null".to_owned()))?;
        let len = column.size();
        if index >= len {
            return Err(CppError::IndexOutOfBounds { index, len });
        }
        let bytes = column.string_at(index);
        // SAFETY: the C++ CppBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        Ok(unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) })
    }

    /// Read one item as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_str(&self, name: &str, index: usize) -> CppResult<&str> {
        let bytes = self.column_bytes(name, index)?;
        core::str::from_utf8(bytes).map_err(|_| CppError::Utf8(name.to_owned()))
    }

    /// Read the null bitmap of a Nullable column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_nulls(&self, name: &str) -> CppResult<&[u8]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("column handle is null".to_owned()))?;
        if TypeCode::from_i32(column.type_code())? != TypeCode::Nullable {
            return Err(CppError::Unsupported(format!(
                "column '{name}' is not Nullable"
            )));
        }
        let nulls = column.nulls();
        // SAFETY: the C++ CppBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        Ok(unsafe { core::mem::transmute::<&[u8], &[u8]>(nulls) })
    }

    /// Read the offsets of an Array column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn column_offsets(&self, name: &str) -> CppResult<&[u64]> {
        let handle = self.column_handle(name)?;
        let column = handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("column handle is null".to_owned()))?;
        if TypeCode::from_i32(column.type_code())? != TypeCode::Array {
            return Err(CppError::Unsupported(format!(
                "column '{name}' is not Array"
            )));
        }
        let offsets = column.array_offsets();
        // SAFETY: the C++ CppBlock owns the column and `&self` prevents mutation or
        // reallocation for the lifetime of the returned slice.
        Ok(unsafe { core::mem::transmute::<&[u64], &[u64]>(offsets) })
    }

    /// Fixed-width kind of one column, if it has one.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn fixed_kind(&self, name: &str) -> CppResult<FixedKind> {
        let code = self.column_type_code(name)?;
        match code {
            TypeCode::UInt8 => Ok(FixedKind::UInt8),
            TypeCode::UInt16 => Ok(FixedKind::UInt16),
            TypeCode::UInt32 => Ok(FixedKind::UInt32),
            TypeCode::UInt64 => Ok(FixedKind::UInt64),
            TypeCode::UInt128 => Ok(FixedKind::UInt128),
            TypeCode::Int8 => Ok(FixedKind::Int8),
            TypeCode::Int16 => Ok(FixedKind::Int16),
            TypeCode::Int32 => Ok(FixedKind::Int32),
            TypeCode::Int64 => Ok(FixedKind::Int64),
            TypeCode::Int128 => Ok(FixedKind::Int128),
            TypeCode::Float32 => Ok(FixedKind::Float32),
            TypeCode::Float64 => Ok(FixedKind::Float64),
            TypeCode::Bool => Ok(FixedKind::Bool),
            TypeCode::Date => Ok(FixedKind::Date),
            TypeCode::Date32 => Ok(FixedKind::Date32),
            TypeCode::DateTime => Ok(FixedKind::DateTime),
            TypeCode::DateTime64 => Ok(FixedKind::DateTime64),
            TypeCode::Time => Ok(FixedKind::Time),
            TypeCode::Time64 => Ok(FixedKind::Time64),
            TypeCode::Decimal => {
                let type_name = self.column_type_name(name)?;
                crate::cpp_column::decimal_kind(&type_name).ok_or_else(|| {
                    CppError::Unsupported(format!("unknown decimal type '{type_name}'"))
                })
            }
            TypeCode::Decimal32 => Ok(FixedKind::Decimal32),
            TypeCode::Decimal64 => Ok(FixedKind::Decimal64),
            TypeCode::Decimal128 => Ok(FixedKind::Decimal128),
            TypeCode::Uuid => Ok(FixedKind::Uuid),
            TypeCode::Ipv4 => Ok(FixedKind::Ipv4),
            TypeCode::Ipv6 => Ok(FixedKind::Ipv6),
            TypeCode::Enum8 => Ok(FixedKind::Enum8),
            TypeCode::Enum16 => Ok(FixedKind::Enum16),
            other => Err(CppError::Unsupported(format!(
                "column '{name}' has non-fixed type {other:?}"
            ))),
        }
    }
}
