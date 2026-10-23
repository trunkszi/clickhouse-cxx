//! Dispatch serde struct fields into owned block columns.
//!
//! Field names become column names. Column kinds are inferred from the first
//! value seen for each field, and every later row must agree. `Option<T>`
//! becomes `Nullable(T)`, `Vec<T>` becomes `Array(T)`, `Map<K, V>` becomes
//! `Map(K,V)`, and byte strings use the same flat storage as `String`.

mod arena;

use std::borrow::Cow;

use arena::{Arena, MapFrame};
use serde::ser::{Serialize, SerializeSeq, SerializeStruct, Serializer};

use super::SerError;
use crate::{Block, error::Error};

fn unsupported(shape: &'static str) -> SerError {
    SerError::custom(format!("unsupported block bridge shape: {shape}"))
}

struct BlockSer<'a> {
    arena: &'a mut Arena,
    tops: &'a mut Vec<usize>,
    capture: bool,
    stack: Vec<usize>,
    maps: Vec<MapFrame>,
    current: Option<usize>,
    field_i: usize,
}

impl BlockSer<'_> {
    fn current(&self) -> Result<usize, SerError> {
        self.current
            .ok_or_else(|| SerError::custom("serde rows must be structs with named fields"))
    }

    fn field(&mut self, key: &'static str) -> Result<usize, SerError> {
        if self.capture {
            let index = self.arena.alloc(Cow::Borrowed(key));
            self.tops.push(index);
        }
        let index = *self
            .tops
            .get(self.field_i)
            .ok_or_else(|| SerError::custom("row has more fields than the first row"))?;
        if self.arena.name(index) != key {
            return Err(SerError::custom(format!(
                "row field `{key}` does not match `{}` from the first row",
                self.arena.name(index)
            )));
        }
        self.field_i += 1;
        Ok(index)
    }
}

pub(crate) fn rows_to_block<T: Serialize>(rows: &[T]) -> Result<Block, Error> {
    if rows.is_empty() {
        return Ok(Block::new());
    }
    let mut arena = Arena::default();
    let mut tops = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let mut serializer = BlockSer {
            arena: &mut arena,
            tops: &mut tops,
            capture: index == 0,
            stack: Vec::new(),
            maps: Vec::new(),
            current: None,
            field_i: 0,
        };
        row.serialize(&mut serializer)?;
        if index == 0 && tops.is_empty() {
            return Err(Error::serde("serde row has no fields"));
        }
    }
    for index in 0..arena.len() {
        arena.resolve(index)?;
    }
    let mut block = Block::new();
    for index in &tops {
        block.append_column(arena.take_column(*index)?)?;
    }
    Ok(block)
}

impl Serializer for &mut BlockSer<'_> {
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
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Bool", 1, &[u8::from(value)])
    }

    fn serialize_i8(self, value: i8) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Int8", 1, &value.to_le_bytes())
    }

    fn serialize_i16(self, value: i16) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Int16", 2, &value.to_le_bytes())
    }

    fn serialize_i32(self, value: i32) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Int32", 4, &value.to_le_bytes())
    }

    fn serialize_i64(self, value: i64) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Int64", 8, &value.to_le_bytes())
    }

    fn serialize_i128(self, value: i128) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Int128", 16, &value.to_le_bytes())
    }

    fn serialize_u8(self, value: u8) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena.append_fixed(index, "UInt8", 1, &[value])
    }

    fn serialize_u16(self, value: u16) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "UInt16", 2, &value.to_le_bytes())
    }

    fn serialize_u32(self, value: u32) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "UInt32", 4, &value.to_le_bytes())
    }

    fn serialize_u64(self, value: u64) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "UInt64", 8, &value.to_le_bytes())
    }

    fn serialize_u128(self, value: u128) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "UInt128", 16, &value.to_le_bytes())
    }

    fn serialize_f32(self, value: f32) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Float32", 4, &value.to_le_bytes())
    }

    fn serialize_f64(self, value: f64) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena
            .append_fixed(index, "Float64", 8, &value.to_le_bytes())
    }

    fn serialize_char(self, _value: char) -> Result<(), SerError> {
        Err(SerError::custom("char is not a column value"))
    }

    fn serialize_str(self, value: &str) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena.append_string(index, value.as_bytes())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena.append_string(index, value)
    }

    fn serialize_none(self) -> Result<(), SerError> {
        let index = self.current()?;
        self.arena.note_none(index)
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), SerError> {
        let index = self.current()?;
        let inner = self.arena.begin_some(index)?;
        self.stack.push(index);
        self.current = Some(inner);
        value.serialize(&mut *self)?;
        self.current = self.stack.pop();
        Ok(())
    }

    fn serialize_unit(self) -> Result<(), SerError> {
        Err(SerError::custom("unit is not a column value"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), SerError> {
        Err(SerError::custom("unit struct is not a column value"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), SerError> {
        Err(SerError::custom("enum is not a column value"))
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
        Err(SerError::custom("enum is not a column value"))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self, SerError> {
        if len.is_none() {
            return Err(SerError::custom("sequence length is required"));
        }
        let index = self.current()?;
        let inner = self.arena.begin_array(index)?;
        self.stack.push(index);
        self.current = Some(inner);
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self, SerError> {
        Err(unsupported("tuple values"))
    }

    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self, SerError> {
        Err(unsupported("tuple structs"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, SerError> {
        Err(SerError::custom("enum is not a column value"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self, SerError> {
        if len.is_none() {
            return Err(SerError::custom("map length is required"));
        }
        let index = self.current()?;
        let (keys, values) = self.arena.begin_map(index)?;
        self.maps.push(MapFrame {
            index,
            keys,
            values,
        });
        self.current = None;
        Ok(self)
    }

    fn serialize_struct(self, name: &'static str, _len: usize) -> Result<Self, SerError> {
        if self.current.is_some() {
            return Err(SerError::custom(format!(
                "nested struct `{name}` is not supported by the block bridge"
            )));
        }
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
        Err(SerError::custom("enum is not a column value"))
    }
}

impl SerializeSeq for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), SerError> {
        let index = self
            .stack
            .pop()
            .ok_or_else(|| SerError::custom("array stack underflow"))?;
        self.arena.end_array(index)?;
        self.current = Some(index);
        Ok(())
    }
}

impl serde::ser::SerializeTuple for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(unsupported("tuple values"))
    }

    fn end(self) -> Result<(), SerError> {
        Err(unsupported("tuple values"))
    }
}

impl serde::ser::SerializeTupleStruct for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(unsupported("tuple structs"))
    }

    fn end(self) -> Result<(), SerError> {
        Err(unsupported("tuple structs"))
    }
}

impl serde::ser::SerializeTupleVariant for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(SerError::custom("enum is not a column value"))
    }
    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom("enum is not a column value"))
    }
}

impl serde::ser::SerializeMap for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), SerError> {
        let keys = self
            .maps
            .last()
            .map(|frame| frame.keys)
            .ok_or_else(|| SerError::custom("map stack underflow"))?;
        self.current = Some(keys);
        let result = key.serialize(&mut **self);
        self.current = None;
        result
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        let values = self
            .maps
            .last()
            .map(|frame| frame.values)
            .ok_or_else(|| SerError::custom("map stack underflow"))?;
        self.current = Some(values);
        let result = value.serialize(&mut **self);
        self.current = None;
        result
    }

    fn end(self) -> Result<(), SerError> {
        let frame = self
            .maps
            .pop()
            .ok_or_else(|| SerError::custom("map stack underflow"))?;
        self.arena.end_map(frame.index)?;
        self.current = Some(frame.index);
        Ok(())
    }
}

impl SerializeStruct for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), SerError> {
        let index = self.field(key)?;
        self.current = Some(index);
        value.serialize(&mut **self)?;
        self.current = None;
        Ok(())
    }

    fn end(self) -> Result<(), SerError> {
        if !self.capture && self.field_i != self.tops.len() {
            return Err(SerError::custom(
                "row is missing fields declared by the first row",
            ));
        }
        Ok(())
    }
}

impl serde::ser::SerializeStructVariant for &mut BlockSer<'_> {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), SerError> {
        Err(SerError::custom("enum is not a column value"))
    }
    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom("enum is not a column value"))
    }
}
