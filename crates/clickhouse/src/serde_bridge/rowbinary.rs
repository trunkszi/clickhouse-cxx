//! ClickHouse RowBinary serializer and deserializer.
//!
//! Values are appended into one output buffer. Nullable values carry a leading
//! tag byte: `1` means NULL and `0` means the value follows. Sequences are
//! length-prefixed with unsigned LEB128, matching `Array` RowBinary encoding.

use std::borrow::Cow;

use serde::{
    de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor},
    ser::{Serialize, SerializeMap, SerializeSeq, SerializeStruct, Serializer},
};

use super::{
    SerError,
    varint::{read_len, write_varu64},
};
use crate::error::Error;

struct Writer<'a> {
    out: &'a mut Vec<u8>,
    names: &'a mut Vec<Cow<'static, str>>,
    capture: bool,
    field_i: usize,
    in_row: bool,
}

pub(crate) fn to_rowbinary<T: Serialize>(
    rows: &[T],
) -> Result<(Vec<Cow<'static, str>>, Vec<u8>), Error> {
    let mut names = Vec::new();
    let mut out = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let mut writer = Writer {
            out: &mut out,
            names: &mut names,
            capture: index == 0,
            field_i: 0,
            in_row: false,
        };
        row.serialize(&mut writer)?;
    }
    Ok((names, out))
}

pub(crate) fn from_rowbinary<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<Vec<T>, Error> {
    let mut input = bytes;
    let mut rows = Vec::new();
    while !input.is_empty() {
        let mut decoder = Decoder {
            input,
            in_row: false,
        };
        rows.push(T::deserialize(&mut decoder)?);
        input = decoder.input;
    }
    Ok(rows)
}

impl Serializer for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, value: bool) -> Result<(), SerError> {
        self.out.push(u8::from(value));
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_i128(self, value: i128) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<(), SerError> {
        self.out.push(value);
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_u128(self, value: u128) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<(), SerError> {
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn serialize_char(self, _value: char) -> Result<(), SerError> {
        Err(SerError::custom("char is not a RowBinary value"))
    }

    fn serialize_str(self, value: &str) -> Result<(), SerError> {
        write_varu64(self.out, value.len() as u64);
        self.out.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<(), SerError> {
        write_varu64(self.out, value.len() as u64);
        self.out.extend_from_slice(value);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), SerError> {
        self.out.push(1);
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), SerError> {
        self.out.push(0);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), SerError> {
        Err(SerError::custom("unit is not a RowBinary value"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), SerError> {
        Err(SerError::custom("unit struct is not a RowBinary value"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), SerError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self, SerError> {
        let len = len.ok_or_else(|| SerError::custom("sequence length is required"))?;
        write_varu64(
            self.out,
            u64::try_from(len).map_err(|_| SerError::custom("sequence length overflows u64"))?,
        );
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self, SerError> {
        Err(SerError::custom(
            "tuple values are not supported by the RowBinary bridge",
        ))
    }

    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self, SerError> {
        Err(SerError::custom(
            "tuple structs are not supported by the RowBinary bridge",
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self, SerError> {
        let len = len.ok_or_else(|| SerError::custom("map length is required"))?;
        write_varu64(
            self.out,
            u64::try_from(len).map_err(|_| SerError::custom("map length overflows u64"))?,
        );
        Ok(self)
    }

    fn serialize_struct(self, name: &'static str, _len: usize) -> Result<Self, SerError> {
        if self.in_row {
            return Err(SerError::custom(format!(
                "nested struct `{name}` is not supported by the RowBinary bridge"
            )));
        }
        self.in_row = true;
        self.field_i = 0;
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }
}

impl SerializeSeq for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), SerError> {
        Ok(())
    }
}

impl serde::ser::SerializeTuple for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(SerError::custom(
            "tuple values are not supported by the RowBinary bridge",
        ))
    }

    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom(
            "tuple values are not supported by the RowBinary bridge",
        ))
    }
}

impl serde::ser::SerializeTupleStruct for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(SerError::custom(
            "tuple structs are not supported by the RowBinary bridge",
        ))
    }

    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom(
            "tuple structs are not supported by the RowBinary bridge",
        ))
    }
}

impl serde::ser::SerializeTupleVariant for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }

    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }
}

impl SerializeMap for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), SerError> {
        key.serialize(&mut **self)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), SerError> {
        Ok(())
    }
}

impl SerializeStruct for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), SerError> {
        if self.capture {
            self.names.push(Cow::Borrowed(key));
        } else {
            let expected = self
                .names
                .get(self.field_i)
                .ok_or_else(|| SerError::custom("row has more fields than the first row"))?;
            if &**expected != key {
                return Err(SerError::custom(format!(
                    "row field `{key}` does not match `{expected}` from the first row"
                )));
            }
        }
        self.field_i += 1;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), SerError> {
        if !self.capture && self.field_i != self.names.len() {
            return Err(SerError::custom(
                "row is missing fields declared by the first row",
            ));
        }
        self.in_row = false;
        Ok(())
    }
}

impl serde::ser::SerializeStructVariant for &mut Writer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }

    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom("enum variant is not a RowBinary value"))
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    in_row: bool,
}

impl Decoder<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], SerError> {
        if self.input.len() < len {
            return Err(SerError::custom("truncated RowBinary value"));
        }
        let (head, tail) = self.input.split_at(len);
        self.input = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SerError> {
        let bytes = self.take(N)?;
        let mut array = [0_u8; N];
        array.copy_from_slice(bytes);
        Ok(array)
    }

    fn read_bytes(&mut self) -> Result<&[u8], SerError> {
        let len = read_len(&mut self.input)?;
        self.take(len)
    }
}

impl<'de> Deserializer<'de> for &mut Decoder<'de> {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("RowBinary needs a concrete type"))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let byte = self.array::<1>()?[0];
        visitor.visit_bool(byte != 0)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i8(i8::from_le_bytes(self.array()?))
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i16(i16::from_le_bytes(self.array()?))
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i32(i32::from_le_bytes(self.array()?))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(i64::from_le_bytes(self.array()?))
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i128(i128::from_le_bytes(self.array()?))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u8(self.array::<1>()?[0])
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u16(u16::from_le_bytes(self.array()?))
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u32(u32::from_le_bytes(self.array()?))
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u64(u64::from_le_bytes(self.array()?))
    }

    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u128(u128::from_le_bytes(self.array()?))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_f32(f32::from_le_bytes(self.array()?))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_f64(f64::from_le_bytes(self.array()?))
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("char is not a RowBinary value"))
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let bytes = self.read_bytes()?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| SerError::custom("RowBinary string is not UTF-8"))?;
        visitor.visit_str(text)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_bytes(self.read_bytes()?)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.array::<1>()?[0] {
            1 => visitor.visit_none(),
            0 => visitor.visit_some(self),
            _ => Err(SerError::custom("invalid nullable tag byte")),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("unit is not a RowBinary value"))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom("unit struct is not a RowBinary value"))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, SerError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let left = read_len(&mut self.input)?;
        visitor.visit_seq(CountedSeq { de: self, left })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom(
            "tuple values are not supported by the RowBinary bridge",
        ))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom(
            "tuple structs are not supported by the RowBinary bridge",
        ))
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        let left = read_len(&mut self.input)?;
        visitor.visit_map(CountedMap {
            de: self,
            left,
            key_pending: false,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, SerError> {
        if self.in_row {
            return Err(SerError::custom(format!(
                "nested struct `{name}` is not supported by the RowBinary bridge"
            )));
        }
        self.in_row = true;
        let value = visitor.visit_seq(OpenSeq { de: &mut *self })?;
        self.in_row = false;
        Ok(value)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError::custom("enum is not a RowBinary value"))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError::custom("identifier is not a RowBinary value"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_unit()
    }
}

struct CountedSeq<'a, 'de> {
    de: &'a mut Decoder<'de>,
    left: usize,
}

impl<'de> SeqAccess<'de> for CountedSeq<'_, 'de> {
    type Error = SerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, SerError> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.left)
    }
}

struct OpenSeq<'a, 'de> {
    de: &'a mut Decoder<'de>,
}

impl<'de> SeqAccess<'de> for OpenSeq<'_, 'de> {
    type Error = SerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, SerError> {
        seed.deserialize(&mut *self.de).map(Some)
    }
}

struct CountedMap<'a, 'de> {
    de: &'a mut Decoder<'de>,
    left: usize,
    key_pending: bool,
}

impl<'de> MapAccess<'de> for CountedMap<'_, 'de> {
    type Error = SerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, SerError> {
        if self.left == 0 {
            return Ok(None);
        }
        if self.key_pending {
            return Err(SerError::custom("map value is missing"));
        }
        self.key_pending = true;
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, SerError> {
        if !self.key_pending {
            return Err(SerError::custom("map value has no preceding key"));
        }
        self.key_pending = false;
        self.left -= 1;
        seed.deserialize(&mut *self.de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.left)
    }
}
