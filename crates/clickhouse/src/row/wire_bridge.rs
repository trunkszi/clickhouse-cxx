//! Conversions between pure-Rust row columns and the C++ column matrix.

use std::borrow::Cow;

use uarch::prelude::*;

use crate::{
    ArrayColumn, Column, MapColumn, NullableColumn, StringColumn, TypeCode, WireColumn,
    column::{
        AlignedBytes, ColumnData, prefetch_offsets, valid_null_map, validate_child_type,
        validate_offsets,
    },
    error::{Error, Result, schema},
    sys,
    type_layout::{Layout, fixed_width, layout},
};

#[cold_sink]
fn unsupported(type_name: &str) -> Error {
    Error::SchemaMismatch {
        table: None,
        reason: Cow::Owned(format!(
            "native transport does not support column type `{type_name}`"
        )),
    }
}

#[hot_inline]
fn string_row<'a>(offsets: &[u64], bytes: &'a [u8], row: usize) -> Result<&'a [u8]> {
    prefetch_offsets(offsets, row);
    let begin = if row == 0 {
        0
    } else {
        *offsets
            .get(row - 1)
            .ok_or_else(|| schema("string offset out of bounds"))? as usize
    };
    let finish = *offsets
        .get(row)
        .ok_or_else(|| schema("string offset out of bounds"))? as usize;
    bytes
        .get(begin..finish)
        .ok_or_else(|| schema("string value out of bounds"))
}

fn checked_fixed_width(column: &Column, bytes: &[u8]) -> Result<usize> {
    let width = fixed_width(&column.type_name).ok_or_else(|| unsupported(&column.type_name))?;
    let expected = column
        .len()
        .checked_mul(width)
        .ok_or_else(|| schema("fixed column byte size overflow"))?;
    if bytes.len() != expected {
        return Err(schema("fixed column length does not match row count"));
    }
    Ok(width)
}

fn validate_scalar_storage(column: &Column) -> Result<()> {
    match &column.data {
        ColumnData::Fixed(bytes) if fixed_width(&column.type_name).is_some() => {
            checked_fixed_width(column, bytes.as_bytes())?;
        }
        ColumnData::Strings { offsets, bytes } => {
            validate_offsets(offsets, bytes.len())?;
            if offsets.len() != column.len() {
                return Err(schema("string column length mismatch"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_array(column: &Column, offsets: &[u64], values: &Column) -> Result<()> {
    validate_offsets(offsets, values.len())?;
    if offsets.len() != column.len() {
        return Err(schema("array column length mismatch"));
    }
    let Layout::Array(inner) = layout(&column.type_name)? else {
        return Err(schema("array field type mismatch"));
    };
    validate_child_type(inner, values)?;
    validate_scalar_storage(values)
}

fn validate_nullable(column: &Column, null_map: &[u8], values: &Column) -> Result<()> {
    if !ilp_all!(null_map.len() == column.len(), values.len() == column.len()) {
        return Err(schema("nullable column length mismatch"));
    }
    if !valid_null_map(null_map) {
        return Err(schema("invalid nullable null map"));
    }
    let Layout::Nullable(inner) = layout(&column.type_name)? else {
        return Err(schema("nullable field type mismatch"));
    };
    validate_child_type(inner, values)
}

fn validate_map(column: &Column, offsets: &[u64], keys: &Column, values: &Column) -> Result<()> {
    validate_offsets(offsets, keys.len())?;
    if !ilp_all!(keys.len() == values.len(), offsets.len() == column.len()) {
        return Err(schema("map column length mismatch"));
    }
    let Layout::Map(key_type, value_type) = layout(&column.type_name)? else {
        return Err(schema("map field type mismatch"));
    };
    validate_child_type(key_type, keys)?;
    validate_child_type(value_type, values)?;
    validate_scalar_storage(keys)?;
    validate_scalar_storage(values)
}

fn scalar_from_slice(column: &Column, start: usize, end: usize) -> Result<WireColumn> {
    match &column.data {
        ColumnData::Fixed(bytes) => {
            let kind = crate::type_layout::fixed_kind(&column.type_name)
                .ok_or_else(|| unsupported(&column.type_name))?;
            let raw = bytes.as_bytes();
            let width =
                fixed_width(&column.type_name).ok_or_else(|| unsupported(&column.type_name))?;
            let begin = start
                .checked_mul(width)
                .ok_or_else(|| schema("array byte offset overflow"))?;
            let finish = end
                .checked_mul(width)
                .ok_or_else(|| schema("array byte offset overflow"))?;
            let slice = raw
                .get(begin..finish)
                .ok_or_else(|| schema("array value slice out of bounds"))?;
            Ok(WireColumn::fixed_bytes(kind, &column.type_name, slice).map_err(Error::from)?)
        }
        ColumnData::Strings { offsets, bytes } => {
            let mut column = StringColumn::new().map_err(Error::from)?;
            for row in start..end {
                let value = string_row(offsets, bytes, row)?;
                column.append_bytes(value).map_err(Error::from)?;
            }
            Ok(WireColumn::String(column))
        }
        _ => Err(unsupported(&column.type_name)),
    }
}

/// Convert a pure-Rust row column into a C++ matrix column.
///
/// # Errors
///
/// Returns an error for unsupported column shapes or bridge failures.
pub(crate) fn to_wire(column: &Column) -> Result<WireColumn> {
    match &column.data {
        ColumnData::Fixed(bytes) => {
            let kind = crate::type_layout::fixed_kind(&column.type_name)
                .ok_or_else(|| unsupported(&column.type_name))?;
            checked_fixed_width(column, bytes.as_bytes())?;
            WireColumn::fixed_bytes(kind, &column.type_name, bytes.as_bytes()).map_err(Error::from)
        }
        ColumnData::Strings { .. } => {
            validate_scalar_storage(column)?;
            scalar_from_slice(column, 0, column.len())
        }
        ColumnData::Array { offsets, values } => {
            let mut array = ArrayColumn::new(&values.type_name).map_err(Error::from)?;
            validate_array(column, offsets, values)?;
            let mut previous = 0_u64;
            for (row_index, &offset) in offsets.iter().enumerate() {
                prefetch_offsets(offsets, row_index);
                let end = offset as usize;
                let row = scalar_from_slice(values, previous as usize, end)?;
                array.append_row(&row).map_err(Error::from)?;
                previous = offset;
            }
            Ok(WireColumn::Array(Box::new(array)))
        }
        ColumnData::Nullable { null_map, values } => {
            let inner = to_wire(values)?;
            let mut nullable = NullableColumn::new(&inner).map_err(Error::from)?;
            validate_nullable(column, null_map, values)?;
            nullable.append_nested_column(&inner).map_err(Error::from)?;
            for flag in null_map {
                nullable.append_null_flag(*flag == 1).map_err(Error::from)?;
            }
            Ok(WireColumn::Nullable(Box::new(nullable)))
        }
        ColumnData::Map {
            offsets,
            keys,
            values,
        } => {
            let mut map =
                MapColumn::new(&keys.type_name, &values.type_name).map_err(Error::from)?;
            validate_map(column, offsets, keys, values)?;
            let mut previous = 0_u64;
            for (row_index, &offset) in offsets.iter().enumerate() {
                prefetch_offsets(offsets, row_index);
                let end = offset as usize;
                let row_keys = scalar_from_slice(keys, previous as usize, end)?;
                let row_values = scalar_from_slice(values, previous as usize, end)?;
                map.append_row(&row_keys, &row_values)
                    .map_err(Error::from)?;
                previous = offset;
            }
            Ok(WireColumn::Map(map))
        }
    }
}

fn fixed_column_from_handle(handle: &sys::ColumnHandle, name: &str) -> Result<Column> {
    let bytes = handle.fixed_bytes();
    if ilp_all!(handle.size() > 0, bytes.is_empty()) {
        return Err(unsupported_fixed_handle(handle));
    }
    Ok(Column {
        name: Cow::Owned(name.to_owned()),
        type_name: Cow::Owned(handle.type_name()),
        data: ColumnData::Fixed(AlignedBytes::from_bytes(bytes)),
        rows: handle.size(),
    })
}

#[cold_sink]
fn unsupported_fixed_handle(handle: &sys::ColumnHandle) -> Error {
    Error::UnsupportedColumnType {
        type_name: handle.type_name(),
    }
}

fn string_column_from_handle(handle: &sys::ColumnHandle, name: &str) -> Column {
    let mut offsets = Vec::with_capacity(handle.size());
    let mut bytes = Vec::new();
    for row in 0..handle.size() {
        let value = handle.string_at(row);
        bytes.extend_from_slice(value);
        offsets.push(bytes.len() as u64);
    }
    Column {
        name: Cow::Owned(name.to_owned()),
        type_name: Cow::Owned(handle.type_name()),
        data: ColumnData::Strings { offsets, bytes },
        rows: handle.size(),
    }
}

fn map_scalar_column(type_name: String, bytes: Vec<u8>, offsets: Vec<u64>) -> Column {
    let rows = offsets.len();
    if type_name == "String" {
        Column {
            name: "values".into(),
            type_name: Cow::Owned(type_name),
            data: ColumnData::Strings { offsets, bytes },
            rows,
        }
    } else {
        Column {
            name: "values".into(),
            type_name: Cow::Owned(type_name),
            data: ColumnData::Fixed(AlignedBytes::from_bytes(&bytes)),
            rows,
        }
    }
}

fn map_column_from_handle(handle: &sys::ColumnHandle, name: &str) -> Result<Column> {
    let rows = handle.size();
    let mut offsets = Vec::with_capacity(rows);
    let mut key_bytes = Vec::new();
    let mut key_offsets = Vec::new();
    let mut value_bytes = Vec::new();
    let mut value_offsets = Vec::new();
    let type_name = handle.type_name();
    let (mut key_type, mut value_type) = match crate::type_layout::layout(&type_name)? {
        crate::type_layout::Layout::Map(keys, values) => (keys.to_owned(), values.to_owned()),
        _ => return Err(schema("map handle has a non-Map type name")),
    };
    for row in 0..rows {
        let tuple = handle.map_row(row);
        let tuple = tuple
            .as_ref()
            .ok_or_else(|| schema("map row handle is null"))?;
        let keys = tuple.tuple_at(0);
        let values = tuple.tuple_at(1);
        let keys = keys
            .as_ref()
            .ok_or_else(|| schema("map keys handle is null"))?;
        let values = values
            .as_ref()
            .ok_or_else(|| schema("map values handle is null"))?;
        key_type = keys.type_name();
        value_type = values.type_name();
        for index in 0..keys.size() {
            key_bytes.extend_from_slice(keys.item_bytes(index));
            key_offsets.push(key_bytes.len() as u64);
        }
        for index in 0..values.size() {
            value_bytes.extend_from_slice(values.item_bytes(index));
            value_offsets.push(value_bytes.len() as u64);
        }
        offsets.push(key_offsets.len() as u64);
    }
    let keys = map_scalar_column(key_type, key_bytes, key_offsets);
    let values = map_scalar_column(value_type, value_bytes, value_offsets);
    Ok(Column {
        name: Cow::Owned(name.to_owned()),
        type_name: Cow::Owned(type_name),
        data: ColumnData::Map {
            offsets,
            keys: Box::new(keys),
            values: Box::new(values),
        },
        rows,
    })
}

/// Convert a C++ matrix column into a pure-Rust row column.
///
/// # Errors
///
/// Returns an error for unsupported column kinds or bridge failures.
pub(crate) fn from_wire(handle: &sys::ColumnHandle, name: &str) -> Result<Column> {
    match TypeCode::from_i32(handle.type_code()).map_err(Error::from)? {
        TypeCode::String | TypeCode::Json => Ok(string_column_from_handle(handle, name)),
        TypeCode::Array => {
            let offsets = handle.array_offsets().to_vec();
            let nested = handle.nested();
            let nested = nested
                .as_ref()
                .ok_or_else(|| schema("array nested handle is null"))?;
            let values = from_wire(nested, "values")?;
            Ok(Column {
                name: Cow::Owned(name.to_owned()),
                type_name: Cow::Owned(handle.type_name()),
                data: ColumnData::Array {
                    offsets,
                    values: Box::new(values),
                },
                rows: handle.size(),
            })
        }
        TypeCode::Nullable => {
            let null_map = handle.nulls().to_vec();
            let nested = handle.nested();
            let nested = nested
                .as_ref()
                .ok_or_else(|| schema("nullable nested handle is null"))?;
            let values = from_wire(nested, "values")?;
            Ok(Column {
                name: Cow::Owned(name.to_owned()),
                type_name: Cow::Owned(handle.type_name()),
                data: ColumnData::Nullable {
                    null_map,
                    values: Box::new(values),
                },
                rows: handle.size(),
            })
        }
        TypeCode::Map => map_column_from_handle(handle, name),
        _ => fixed_column_from_handle(handle, name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_native_map_retains_declared_child_types() {
        let map = MapColumn::new("String", "UInt64").unwrap();
        let handle = WireColumn::Map(map);
        let result = from_wire(handle.wire_handle().unwrap(), "attributes").unwrap();
        assert!(crate::type_layout::same_type(
            &result.type_name,
            "Map(String, UInt64)"
        ));
        let ColumnData::Map { keys, values, .. } = result.data else {
            panic!("expected map")
        };
        assert_eq!(keys.type_name, "String");
        assert_eq!(values.type_name, "UInt64");
    }

    #[test]
    fn unsupported_nonempty_native_column_reports_its_type() {
        let mut tuple = crate::TupleColumn::new(&["UInt8", "String"]).unwrap();
        tuple.append_fixed(0, &[7]).unwrap();
        tuple.append_str(1, "value").unwrap();
        let error = match from_wire(tuple.wire_handle().unwrap(), "tuple") {
            Ok(_) => panic!("unsupported tuple projection succeeded"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::UnsupportedColumnType {type_name} if type_name.contains("Tuple"))
        );
    }
}
