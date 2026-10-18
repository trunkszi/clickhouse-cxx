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
        if &*self.column.type_name != expected {
            return Err(self.expected(&format!("`{expected}`")));
        }
        let ColumnData::Fixed(data) = &self.column.data else {
            return Err(self.expected(&format!("`{expected}`")));
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
        visitor.visit_i32(i32::from_le_bytes(self.fixed("Int32")?))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(i64::from_le_bytes(self.fixed("Int64")?))
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i128(i128::from_le_bytes(self.fixed("Int128")?))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u8(self.fixed::<1>("UInt8")?[0])
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u16(u16::from_le_bytes(self.fixed("UInt16")?))
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u32(u32::from_le_bytes(self.fixed("UInt32")?))
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
        visitor.visit_borrowed_bytes(self.string_bytes()?)
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

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("map is not a supported column value"))
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
