//! Read-only byte views over the children of composite columns.
//!
//! Each child column lives inside its parent, so the returned views are tied
//! to `&self` of the parent and cannot outlive it.

use clickhouse_sys as sys;

use super::{ArrayColumn, NullableColumn, TupleColumn, TypeCode};
use crate::wire_error::{WireError, WireResult};

/// Bytes of a fixed-width column whose storage belongs to the column itself.
///
/// Bridge-cached kinds (Bool, Enum, Decimal, UUID, IP, FixedString, DateTime64)
/// answer from a reusable scratch buffer and are deliberately excluded.
fn projected_fixed_bytes(column: &sys::ColumnHandle) -> Option<&[u8]> {
    let code = TypeCode::from_i32(column.type_code()).ok()?;
    if !matches!(
        code,
        TypeCode::Int8
            | TypeCode::Int16
            | TypeCode::Int32
            | TypeCode::Int64
            | TypeCode::Int128
            | TypeCode::UInt8
            | TypeCode::UInt16
            | TypeCode::UInt32
            | TypeCode::UInt64
            | TypeCode::UInt128
            | TypeCode::Float32
            | TypeCode::Float64
            | TypeCode::Date
            | TypeCode::Date32
            | TypeCode::DateTime
            | TypeCode::Time
            | TypeCode::Time64
    ) {
        return None;
    }
    Some(column.fixed_bytes())
}

impl NullableColumn {
    /// Fixed-width bytes of the nested column.
    ///
    /// Returns empty when the nested column is absent or uses a bridge-cached layout.
    pub fn nested_fixed_bytes(&self) -> &[u8] {
        let Some(nested) = self.nested() else {
            return &[];
        };
        let Some(column) = nested.as_ref() else {
            return &[];
        };
        let Some(bytes) = projected_fixed_bytes(column) else {
            return &[];
        };
        // SAFETY: the nested column is owned by this nullable column behind `&self`;
        // mutating methods require `&mut self`, so the view cannot be invalidated.
        unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) }
    }
}

impl ArrayColumn {
    /// Fixed-width bytes of the nested values column.
    ///
    /// Returns empty when the nested column is absent or uses a bridge-cached layout.
    pub fn values_fixed_bytes(&self) -> &[u8] {
        let Some(nested) = self.nested() else {
            return &[];
        };
        let Some(column) = nested.as_ref() else {
            return &[];
        };
        let Some(bytes) = projected_fixed_bytes(column) else {
            return &[];
        };
        // SAFETY: the nested column is owned by this array column behind `&self`;
        // mutating methods require `&mut self`, so the view cannot be invalidated.
        unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) }
    }
}

impl TupleColumn {
    /// Fixed-width bytes of one tuple element.
    ///
    /// # Errors
    /// Returns an error for an out-of-range index, a null child, or a
    /// bridge-cached child layout.
    pub fn child_fixed_bytes(&self, index: usize) -> WireResult<&[u8]> {
        let len = self.tuple_len();
        if index >= len {
            return Err(WireError::IndexOutOfBounds { index, len });
        }
        let handle = self
            .handle()
            .ok_or_else(|| WireError::Unsupported("Tuple handle is null".to_owned()))?;
        let child = handle.tuple_at(index);
        let child = child
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("tuple child handle is null".to_owned()))?;
        let bytes = projected_fixed_bytes(child).ok_or_else(|| {
            WireError::Unsupported(
                "tuple child is not a directly stored fixed-width column".to_owned(),
            )
        })?;
        // SAFETY: the child column is owned by this tuple behind `&self`;
        // mutating methods require `&mut self`, so the view cannot be invalidated.
        Ok(unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) })
    }

    /// Bytes of one string value in a tuple element.
    ///
    /// # Errors
    /// Returns an error for an out-of-range element or row, a null child, or
    /// a child column that does not store byte strings.
    pub fn child_string_at(&self, index: usize, row: usize) -> WireResult<&[u8]> {
        let len = self.tuple_len();
        if index >= len {
            return Err(WireError::IndexOutOfBounds { index, len });
        }
        let handle = self
            .handle()
            .ok_or_else(|| WireError::Unsupported("Tuple handle is null".to_owned()))?;
        let child = handle.tuple_at(index);
        let child = child
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("tuple child handle is null".to_owned()))?;
        let rows = child.size();
        if row >= rows {
            return Err(WireError::IndexOutOfBounds {
                index: row,
                len: rows,
            });
        }
        let code = TypeCode::from_i32(child.type_code())?;
        if !matches!(
            code,
            TypeCode::String | TypeCode::FixedString | TypeCode::Json
        ) {
            return Err(WireError::Unsupported(format!(
                "tuple child `{}` is not a byte string",
                child.type_name()
            )));
        }
        let bytes = child.string_at(row);
        // SAFETY: string views for these type codes point into the child column's
        // own buffers, owned by this tuple behind `&self`.
        Ok(unsafe { core::mem::transmute::<&[u8], &[u8]>(bytes) })
    }
}
