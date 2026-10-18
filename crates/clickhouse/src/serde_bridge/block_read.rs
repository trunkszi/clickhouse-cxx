//! Deserialize serde values from owned block columns.
//!
//! Each row is presented as a map from column name to value, so serde matches
//! fields by name in any column order. Fixed-width columns are read by their
//! declared ClickHouse type, and nested storage follows `Nullable` and
//! `Array` wrappers.

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::SerError;
use crate::{
    block::Block,
    column::{Column, ColumnData},
    error::Error,
};

pub(crate) fn block_to_rows<T: serde::de::DeserializeOwned>(
    block: &Block,
) -> Result<Vec<T>, Error> {
    let mut rows = Vec::with_capacity(block.row_count());
    for row in 0..block.row_count() {
        rows.push(T::deserialize(&mut RowDe { block, row })?);
    }
    Ok(rows)
}

struct RowDe<'a> {
    block: &'a Block,
    row: usize,
}

impl<'de> Deserializer<'de> for &mut RowDe<'de> {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom(
            "block rows must deserialize into a struct with named fields",
        ))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, SerError> {
        visitor.visit_map(ColumnMap {
            block: self.block,
            row: self.row,
            index: 0,
        })
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, SerError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct seq tuple tuple_struct map enum
        identifier
    }
}

struct ColumnMap<'a> {
    block: &'a Block,
    row: usize,
    index: usize,
}

impl<'de> MapAccess<'de> for ColumnMap<'de> {
    type Error = SerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, SerError> {
        if self.index >= self.block.column_count() {
            return Ok(None);
        }
        let column = self
            .block
            .column(self.index)
            .map_err(|error| SerError::custom(format!("{error}")))?;
        seed.deserialize(KeyDe(&column.name)).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, SerError> {
        let column = self
            .block
            .column(self.index)
            .map_err(|error| SerError::custom(format!("{error}")))?;
        self.index += 1;
        seed.deserialize(ValueDe {
            column,
            row: self.row,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.block.column_count() - self.index)
    }
}

struct KeyDe<'a>(&'a str);

impl<'de> Deserializer<'de> for KeyDe<'de> {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_borrowed_str(self.0)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_borrowed_str(self.0)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_borrowed_str(self.0)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_borrowed_str(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum ignored_any
    }
}

#[derive(Clone, Copy)]
struct ValueDe<'a> {
    column: &'a Column,
    row: usize,
}

impl<'a> ValueDe<'a> {
    fn expected(&self, expected: &str) -> SerError {
        SerError::custom(format!(
            "column `{}` is `{}`, expected {expected}",
            self.column.name, self.column.type_name
        ))
    }

    fn fixed<const N: usize>(&self, expected: &'static str) -> Result<[u8; N], SerError> {
        self.fixed_if::<N>(expected, |name| name == expected)
    }

    /// Read a fixed-width row when the declared type is one of `accept`.
    ///
    /// Logical ClickHouse types share storage with a Rust scalar or byte
    /// string, so the width is checked against the declared type rather than
    /// the Rust type alone.
    fn fixed_if<const N: usize>(
        &self,
        expected: &str,
        accept: impl Fn(&str) -> bool,
    ) -> Result<[u8; N], SerError> {
        if !accept(&self.column.type_name) {
            return Err(self.expected(expected));
        }
        let ColumnData::Fixed(data) = &self.column.data else {
            return Err(self.expected(expected));
        };
        let start = self
            .row
            .checked_mul(N)
            .ok_or_else(|| SerError::custom("column byte offset overflow"))?;
        let end = start
            .checked_add(N)
            .ok_or_else(|| SerError::custom("column byte offset overflow"))?;
        let bytes = data.as_bytes().get(start..end).ok_or_else(|| {
            SerError::custom(format!(
                "column `{}` row is out of bounds",
                self.column.name
            ))
        })?;
        let mut value = [0_u8; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    /// Borrow the 16 fixed bytes of a `UUID` or `IPv6` row.
    fn logical_bytes(&self) -> Result<&'a [u8], SerError> {
        let expected = "`UUID` or `IPv6`";
        let ColumnData::Fixed(data) = &self.column.data else {
            return Err(self.expected(expected));
        };
        let start = self
            .row
            .checked_mul(16)
            .ok_or_else(|| SerError::custom("column byte offset overflow"))?;
        let end = start
            .checked_add(16)
            .ok_or_else(|| SerError::custom("column byte offset overflow"))?;
        data.as_bytes().get(start..end).ok_or_else(|| {
            SerError::custom(format!(
                "column `{}` row is out of bounds",
                self.column.name
            ))
        })
    }

    fn string_bytes(&self) -> Result<&'a [u8], SerError> {
        let ColumnData::Strings { offsets, bytes } = &self.column.data else {
            return Err(self.expected("`String`"));
        };
        let (start, end) = row_span(offsets, self.row)?;
        bytes.get(start..end).ok_or_else(|| {
            SerError::custom(format!(
                "column `{}` has invalid string bounds",
                self.column.name
            ))
        })
    }
}

impl<'de> Deserializer<'de> for ValueDe<'de> {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom(
            "a block column needs a concrete value type",
        ))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_bool(self.fixed::<1>("Bool")?[0] != 0)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i8(i8::from_le_bytes(self.fixed("Int8")?))
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i16(i16::from_le_bytes(self.fixed("Int16")?))
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i32(i32::from_le_bytes(
            self.fixed_if("`Int32`, `Date32`, or a 4-byte `Decimal`", is_i32_source)?,
        ))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(i64::from_le_bytes(self.fixed_if(
            "`Int64`, `DateTime64`, or an 8-byte `Decimal`",
            is_i64_source,
        )?))
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i128(i128::from_le_bytes(
            self.fixed_if("`Int128` or a 16-byte `Decimal`", is_i128_source)?,
        ))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u8(self.fixed::<1>("UInt8")?[0])
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u16(u16::from_le_bytes(
            self.fixed_if("`UInt16` or `Date`", is_u16_source)?,
        ))
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u32(u32::from_le_bytes(
            self.fixed_if("`UInt32`, `DateTime`, or `IPv4`", is_u32_source)?,
        ))
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u64(u64::from_le_bytes(self.fixed("UInt64")?))
    }

    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u128(u128::from_le_bytes(self.fixed("UInt128")?))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_f32(f32::from_le_bytes(self.fixed("Float32")?))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_f64(f64::from_le_bytes(self.fixed("Float64")?))
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("char is not a column value"))
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let bytes = self.string_bytes()?;
        let text = std::str::from_utf8(bytes).map_err(|_| {
            SerError::custom(format!("column `{}` is not valid UTF-8", self.column.name))
        })?;
        visitor.visit_borrowed_str(text)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match &self.column.data {
            ColumnData::Fixed(_) if is_byte_logical(&self.column.type_name) => {
                visitor.visit_borrowed_bytes(self.logical_bytes()?)
            }
            ColumnData::Strings { .. } => visitor.visit_borrowed_bytes(self.string_bytes()?),
            _ => Err(self.expected("`String`, `UUID`, or `IPv6`")),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let ColumnData::Nullable { null_map, values } = &self.column.data else {
            return visitor.visit_some(self);
        };
        match null_map.get(self.row) {
            Some(0) => visitor.visit_some(ValueDe {
                column: values.as_ref(),
                row: self.row,
            }),
            Some(1) => visitor.visit_none(),
            Some(_) => Err(SerError::custom(format!(
                "column `{}` has an invalid null flag",
                self.column.name
            ))),
            None => Err(SerError::custom(format!(
                "column `{}` row is out of bounds",
                self.column.name
            ))),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("unit is not a column value"))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom("unit struct is not a column value"))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, SerError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let ColumnData::Array { offsets, values } = &self.column.data else {
            return Err(self.expected("`Array`"));
        };
        let (start, end) = row_span(offsets, self.row)?;
        if end > values.len() {
            return Err(SerError::custom(format!(
                "column `{}` array offsets exceed its values",
                self.column.name
            )));
        }
        visitor.visit_seq(ArraySeq {
            values: values.as_ref(),
            next: start,
            end,
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom(
            "tuple values are not supported by the block bridge",
        ))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom(
            "tuple structs are not supported by the block bridge",
        ))
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let ColumnData::Map {
            offsets,
            keys,
            values,
        } = &self.column.data
        else {
            return Err(self.expected("`Map`"));
        };
        let name = format!("Map({},{})", keys.type_name, values.type_name);
        if self.column.type_name.as_ref() != name.as_str() {
            return Err(self.expected(&format!("`{name}`")));
        }
        if keys.len() != values.len() {
            return Err(SerError::custom(format!(
                "column `{}` has unequal map key and value counts",
                self.column.name
            )));
        }
        let (start, end) = row_span(offsets, self.row)?;
        if end > keys.len() || end > values.len() {
            return Err(SerError::custom(format!(
                "column `{}` map offsets exceed its keys or values",
                self.column.name
            )));
        }
        visitor.visit_map(MapPairs {
            keys: keys.as_ref(),
            values: values.as_ref(),
            next: start,
            end,
            key_pending: false,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom(format!(
            "nested struct `{name}` is not supported by the block bridge"
        )))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom("enum is not a column value"))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("identifier is not a column value"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_unit()
    }
}

struct ArraySeq<'a> {
    values: &'a Column,
    next: usize,
    end: usize,
}

impl<'de> SeqAccess<'de> for ArraySeq<'de> {
    type Error = SerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, SerError> {
        if self.next >= self.end {
            return Ok(None);
        }
        let row = self.next;
        self.next += 1;
        seed.deserialize(ValueDe {
            column: self.values,
            row,
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.end - self.next)
    }
}

struct MapPairs<'a> {
    keys: &'a Column,
    values: &'a Column,
    next: usize,
    end: usize,
    key_pending: bool,
}

impl<'de> MapAccess<'de> for MapPairs<'de> {
    type Error = SerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, SerError> {
        if self.next >= self.end {
            return Ok(None);
        }
        if self.key_pending {
            return Err(SerError::custom("map value is missing"));
        }
        self.key_pending = true;
        seed.deserialize(ValueDe {
            column: self.keys,
            row: self.next,
        })
        .map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, SerError> {
        if !self.key_pending {
            return Err(SerError::custom("map value has no preceding key"));
        }
        self.key_pending = false;
        let row = self.next;
        self.next += 1;
        seed.deserialize(ValueDe {
            column: self.values,
            row,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.end - self.next)
    }
}

/// Storage width of a ClickHouse `Decimal` type name.
///
/// `Decimal(P, S)` selects 4, 8, or 16 bytes from the precision; resolutions
/// above 38 digits need the 32-byte `Decimal256` storage this bridge does not
/// read.
fn decimal_width(type_name: &str) -> Option<usize> {
    if type_name == "Decimal32" || type_name.starts_with("Decimal32(") {
        return Some(4);
    }
    if type_name == "Decimal64" || type_name.starts_with("Decimal64(") {
        return Some(8);
    }
    if type_name == "Decimal128" || type_name.starts_with("Decimal128(") {
        return Some(16);
    }
    let rest = type_name.strip_prefix("Decimal(")?;
    let precision = rest.split(',').next()?.trim().parse::<u32>().ok()?;
    match precision {
        0..=9 => Some(4),
        10..=18 => Some(8),
        19..=38 => Some(16),
        _ => None,
    }
}

fn is_u16_source(type_name: &str) -> bool {
    matches!(type_name, "UInt16" | "Date")
}

fn is_i32_source(type_name: &str) -> bool {
    matches!(type_name, "Int32" | "Date32") || decimal_width(type_name) == Some(4)
}

fn is_u32_source(type_name: &str) -> bool {
    matches!(type_name, "UInt32" | "DateTime" | "IPv4") || type_name.starts_with("DateTime(")
}

fn is_i64_source(type_name: &str) -> bool {
    matches!(type_name, "Int64" | "DateTime64")
        || type_name.starts_with("DateTime64(")
        || decimal_width(type_name) == Some(8)
}

fn is_i128_source(type_name: &str) -> bool {
    matches!(type_name, "Int128") || decimal_width(type_name) == Some(16)
}

fn is_byte_logical(type_name: &str) -> bool {
    matches!(type_name, "UUID" | "IPv6")
}

fn row_span(offsets: &[u64], row: usize) -> Result<(usize, usize), SerError> {
    let end = *offsets
        .get(row)
        .ok_or_else(|| SerError::custom("array or string row is out of bounds"))?;
    let start = if row == 0 { 0 } else { offsets[row - 1] };
    let start = usize::try_from(start).map_err(|_| SerError::custom("offset overflows usize"))?;
    let end = usize::try_from(end).map_err(|_| SerError::custom("offset overflows usize"))?;
    if start > end {
        return Err(SerError::custom("invalid descending row offsets"));
    }
    Ok((start, end))
}
