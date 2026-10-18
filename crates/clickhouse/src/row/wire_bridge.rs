//! Conversions between pure-Rust row columns and the C++ column matrix.

use std::borrow::Cow;

use crate::{
    ArrayColumn, Column, MapColumn, NullableColumn, StringColumn, TypeCode, WireColumn,
    column::{AlignedBytes, ColumnData},
    error::{Error, Result, schema},
    sys,
};

fn unsupported(type_name: &str) -> Error {
    Error::SchemaMismatch {
        table: None,
        reason: Cow::Owned(format!(
            "native transport does not support column type `{type_name}`"
        )),
    }
}

fn scalar_from_slice(column: &Column, start: usize, end: usize) -> Result<WireColumn> {
    match &column.data {
        ColumnData::Fixed(bytes) => {
            let kind = crate::type_layout::fixed_kind(&column.type_name)
                .ok_or_else(|| unsupported(&column.type_name))?;
            let raw = bytes.as_bytes();
            let width = if column.is_empty() {
                0
            } else {
                raw.len() / column.len()
            };
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
                let begin = if row == 0 {
                    0
                } else {
                    *offsets
                        .get(row - 1)
                        .ok_or_else(|| schema("string offset out of bounds"))?
                        as usize
                };
                let finish = *offsets
                    .get(row)
                    .ok_or_else(|| schema("string offset out of bounds"))?
                    as usize;
                let value = bytes
                    .get(begin..finish)
                    .ok_or_else(|| schema("string value out of bounds"))?;
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
            WireColumn::fixed_bytes(kind, &column.type_name, bytes.as_bytes()).map_err(Error::from)
        }
        ColumnData::Strings { .. } => scalar_from_slice(column, 0, column.len()),
        ColumnData::Array { offsets, values } => {
            let mut array = ArrayColumn::new(&values.type_name).map_err(Error::from)?;
            let mut previous = 0_u64;
            for &offset in offsets {
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
            let mut previous = 0_u64;
            for &offset in offsets {
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
    if handle.size() > 0 && bytes.is_empty() {
        return Err(Error::UnsupportedColumnType {
            type_name: handle.type_name(),
        });
    }
    Ok(Column {
        name: Cow::Owned(name.to_owned()),
        type_name: Cow::Owned(handle.type_name()),
        data: ColumnData::Fixed(AlignedBytes::from_bytes(bytes)),
        rows: handle.size(),
    })
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
        let error = match from_wire(tuple.wire_handle(), "tuple") {
            Ok(_) => panic!("unsupported tuple projection succeeded"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::UnsupportedColumnType {type_name} if type_name.contains("Tuple"))
        );
    }
}
