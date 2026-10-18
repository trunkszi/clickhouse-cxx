//! Dispatch serde struct fields into owned block columns.
//!
//! Field names become column names. Column kinds are inferred from the first
//! value seen for each field, and every later row must agree. `Option<T>`
//! becomes `Nullable(T)`, `Vec<T>` becomes `Array(T)`, and byte strings use
//! the same flat storage as `String`.

use std::borrow::Cow;

use serde::ser::{Serialize, SerializeSeq, SerializeStruct, Serializer};

use super::SerError;
use crate::{
    Block,
    column::{AlignedBytes, Column, ColumnData, validate_offsets},
    error::Error,
};

/// One intermediate column while rows are serialized.
struct Node {
    name: Cow<'static, str>,
    kind: Kind,
    rows: usize,
}

enum Kind {
    /// No value has fixed the type yet; `pending` values were placeholders.
    Unknown {
        pending: usize,
    },
    Fixed {
        type_name: &'static str,
        width: usize,
        bytes: Vec<u8>,
    },
    String {
        offsets: Vec<u64>,
        bytes: Vec<u8>,
    },
    Nullable {
        null_map: Vec<u8>,
        inner: usize,
    },
    Array {
        offsets: Vec<u64>,
        inner: usize,
    },
}

#[derive(Default)]
struct Arena {
    nodes: Vec<Node>,
}

impl Arena {
    fn alloc(&mut self, name: Cow<'static, str>) -> usize {
        let index = self.nodes.len();
        self.nodes.push(Node {
            name,
            kind: Kind::Unknown { pending: 0 },
            rows: 0,
        });
        index
    }

    fn ensure_fixed(
        &mut self,
        index: usize,
        type_name: &'static str,
        width: usize,
    ) -> Result<(), SerError> {
        let pending = match &self.nodes[index].kind {
            Kind::Fixed {
                type_name: existing,
                width: existing_width,
                ..
            } => {
                if *existing == type_name && *existing_width == width {
                    return Ok(());
                }
                return Err(SerError::custom(format!(
                    "column `{}` changes type from `{existing}` to `{type_name}`",
                    self.nodes[index].name
                )));
            }
            Kind::Unknown { pending } => *pending,
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` is not `{type_name}`",
                    self.nodes[index].name
                )));
            }
        };
        self.nodes[index].kind = Kind::Fixed {
            type_name,
            width,
            bytes: Vec::new(),
        };
        self.flush_pending(index, pending)
    }

    fn ensure_string(&mut self, index: usize) -> Result<(), SerError> {
        let pending = match &self.nodes[index].kind {
            Kind::String { .. } => return Ok(()),
            Kind::Unknown { pending } => *pending,
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` changes type to String",
                    self.nodes[index].name
                )));
            }
        };
        self.nodes[index].kind = Kind::String {
            offsets: Vec::new(),
            bytes: Vec::new(),
        };
        self.flush_pending(index, pending)
    }

    fn ensure_nullable(&mut self, index: usize) -> Result<usize, SerError> {
        let pending = match &self.nodes[index].kind {
            Kind::Nullable { inner, .. } => return Ok(*inner),
            Kind::Unknown { pending } => *pending,
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` is not nullable",
                    self.nodes[index].name
                )));
            }
        };
        let inner = self.alloc(Cow::Borrowed(""));
        self.nodes[index].kind = Kind::Nullable {
            null_map: Vec::new(),
            inner,
        };
        for _ in 0..pending {
            if let Kind::Nullable { null_map, .. } = &mut self.nodes[index].kind {
                null_map.push(1);
            }
            self.push_placeholder(inner)?;
        }
        Ok(inner)
    }

    fn ensure_array(&mut self, index: usize) -> Result<usize, SerError> {
        let pending = match &self.nodes[index].kind {
            Kind::Array { inner, .. } => return Ok(*inner),
            Kind::Unknown { pending } => *pending,
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` is not an array",
                    self.nodes[index].name
                )));
            }
        };
        let inner = self.alloc(Cow::Borrowed(""));
        self.nodes[index].kind = Kind::Array {
            offsets: Vec::new(),
            inner,
        };
        for _ in 0..pending {
            let end = self.nodes[inner].rows as u64;
            if let Kind::Array { offsets, .. } = &mut self.nodes[index].kind {
                offsets.push(end);
            }
        }
        Ok(inner)
    }

    /// Append the default value for the current kind without counting a row.
    fn write_placeholder(&mut self, index: usize) -> Result<(), SerError> {
        let action = match &self.nodes[index].kind {
            Kind::Fixed { width, .. } => Placeholder::Fixed(*width),
            Kind::String { bytes, .. } => Placeholder::String(bytes.len() as u64),
            Kind::Array { inner, .. } => Placeholder::Array(self.nodes[*inner].rows as u64),
            Kind::Nullable { inner, .. } => Placeholder::Nullable(*inner),
            Kind::Unknown { .. } => Placeholder::Unknown,
        };
        match action {
            Placeholder::Fixed(width) => {
                if let Kind::Fixed { bytes, .. } = &mut self.nodes[index].kind {
                    bytes.resize(bytes.len() + width, 0);
                }
            }
            Placeholder::String(end) => {
                if let Kind::String { offsets, .. } = &mut self.nodes[index].kind {
                    offsets.push(end);
                }
            }
            Placeholder::Array(end) => {
                if let Kind::Array { offsets, .. } = &mut self.nodes[index].kind {
                    offsets.push(end);
                }
            }
            Placeholder::Nullable(inner) => {
                if let Kind::Nullable { null_map, .. } = &mut self.nodes[index].kind {
                    null_map.push(1);
                }
                self.write_placeholder(inner)?;
            }
            Placeholder::Unknown => {
                if let Kind::Unknown { pending } = &mut self.nodes[index].kind {
                    *pending += 1;
                }
            }
        }
        Ok(())
    }

    /// Append a default value and count it as one row.
    fn push_placeholder(&mut self, index: usize) -> Result<(), SerError> {
        self.nodes[index].rows += 1;
        self.write_placeholder(index)
    }

    fn flush_pending(&mut self, index: usize, pending: usize) -> Result<(), SerError> {
        for _ in 0..pending {
            self.write_placeholder(index)?;
        }
        Ok(())
    }

    fn append_fixed(
        &mut self,
        index: usize,
        type_name: &'static str,
        width: usize,
        raw: &[u8],
    ) -> Result<(), SerError> {
        self.ensure_fixed(index, type_name, width)?;
        if let Kind::Fixed { bytes, .. } = &mut self.nodes[index].kind {
            bytes.extend_from_slice(raw);
        }
        self.nodes[index].rows += 1;
        Ok(())
    }

    fn append_string(&mut self, index: usize, value: &[u8]) -> Result<(), SerError> {
        self.ensure_string(index)?;
        if let Kind::String { offsets, bytes } = &mut self.nodes[index].kind {
            bytes.extend_from_slice(value);
            offsets.push(bytes.len() as u64);
        }
        self.nodes[index].rows += 1;
        Ok(())
    }

    fn note_none(&mut self, index: usize) -> Result<(), SerError> {
        let inner = self.ensure_nullable(index)?;
        if let Kind::Nullable { null_map, .. } = &mut self.nodes[index].kind {
            null_map.push(1);
        }
        self.nodes[index].rows += 1;
        self.push_placeholder(inner)
    }

    fn begin_some(&mut self, index: usize) -> Result<usize, SerError> {
        let inner = self.ensure_nullable(index)?;
        if let Kind::Nullable { null_map, .. } = &mut self.nodes[index].kind {
            null_map.push(0);
        }
        self.nodes[index].rows += 1;
        Ok(inner)
    }

    fn begin_array(&mut self, index: usize) -> Result<usize, SerError> {
        self.ensure_array(index)
    }

    fn end_array(&mut self, index: usize) -> Result<(), SerError> {
        let inner = match &self.nodes[index].kind {
            Kind::Array { inner, .. } => *inner,
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` is not an array",
                    self.nodes[index].name
                )));
            }
        };
        let end = self.nodes[inner].rows as u64;
        if let Kind::Array { offsets, .. } = &mut self.nodes[index].kind {
            offsets.push(end);
        }
        self.nodes[index].rows += 1;
        Ok(())
    }

    /// Turn placeholder-only columns into empty strings so every row has a value.
    fn resolve(&mut self, index: usize) -> Result<(), SerError> {
        let (inner, pending, unresolved) = match &self.nodes[index].kind {
            Kind::Nullable { inner, .. } | Kind::Array { inner, .. } => (Some(*inner), 0, false),
            Kind::Unknown { pending } => (None, *pending, true),
            _ => (None, 0, false),
        };
        if unresolved {
            self.nodes[index].kind = Kind::String {
                offsets: Vec::new(),
                bytes: Vec::new(),
            };
            self.flush_pending(index, pending)?;
        }
        if let Some(inner) = inner {
            self.resolve(inner)?;
        }
        Ok(())
    }

    fn take_column(&mut self, index: usize) -> Result<Column, Error> {
        let (name, rows, kind) = {
            let node = &mut self.nodes[index];
            let kind = std::mem::replace(&mut node.kind, Kind::Unknown { pending: 0 });
            (node.name.clone(), node.rows, kind)
        };
        let (type_name, data) = match kind {
            Kind::Fixed {
                type_name,
                width,
                bytes,
            } => {
                if rows.checked_mul(width) != Some(bytes.len()) {
                    return Err(Error::serde(format!(
                        "column `{name}` has an inconsistent fixed-width payload"
                    )));
                }
                (
                    Cow::Borrowed(type_name),
                    ColumnData::Fixed(AlignedBytes::from_bytes(&bytes)),
                )
            }
            Kind::String { offsets, bytes } => {
                if offsets.len() != rows {
                    return Err(Error::serde(format!(
                        "column `{name}` has an inconsistent string row count"
                    )));
                }
                validate_offsets(&offsets, bytes.len()).map_err(|error| {
                    Error::serde(format!("column `{name}` has invalid offsets: {error}"))
                })?;
                (
                    Cow::Borrowed("String"),
                    ColumnData::Strings { offsets, bytes },
                )
            }
            Kind::Nullable { null_map, inner } => {
                if null_map.len() != rows {
                    return Err(Error::serde(format!(
                        "column `{name}` has an inconsistent null map"
                    )));
                }
                let values = self.take_column(inner)?;
                if values.len() != rows {
                    return Err(Error::serde(format!(
                        "column `{name}` has inconsistent nullable values"
                    )));
                }
                if matches!(&values.data, ColumnData::Nullable { .. }) {
                    return Err(Error::serde(format!(
                        "column `{name}` nests Nullable inside Nullable"
                    )));
                }
                let type_name = Cow::Owned(format!("Nullable({})", values.type_name));
                (
                    type_name,
                    ColumnData::Nullable {
                        null_map,
                        values: Box::new(values),
                    },
                )
            }
            Kind::Array { offsets, inner } => {
                if offsets.len() != rows {
                    return Err(Error::serde(format!(
                        "column `{name}` has an inconsistent array row count"
                    )));
                }
                let values = self.take_column(inner)?;
                validate_offsets(&offsets, values.len()).map_err(|error| {
                    Error::serde(format!("column `{name}` has invalid offsets: {error}"))
                })?;
                let type_name = Cow::Owned(format!("Array({})", values.type_name));
                (
                    type_name,
                    ColumnData::Array {
                        offsets,
                        values: Box::new(values),
                    },
                )
            }
            Kind::Unknown { .. } => {
                return Err(Error::serde(format!(
                    "column `{name}` has no inferred type"
                )));
            }
        };
        Ok(Column {
            name,
            type_name,
            data,
            rows,
        })
    }
}

enum Placeholder {
    Fixed(usize),
    String(u64),
    Array(u64),
    Nullable(usize),
    Unknown,
}

fn unsupported(shape: &'static str) -> SerError {
    SerError::custom(format!("unsupported block bridge shape: {shape}"))
}

struct BlockSer<'a> {
    arena: &'a mut Arena,
    tops: &'a mut Vec<usize>,
    capture: bool,
    stack: Vec<usize>,
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
        if &*self.arena.nodes[index].name != key {
            return Err(SerError::custom(format!(
                "row field `{key}` does not match `{}` from the first row",
                self.arena.nodes[index].name
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
            current: None,
            field_i: 0,
        };
        row.serialize(&mut serializer)?;
        if index == 0 && tops.is_empty() {
            return Err(Error::serde("serde row has no fields"));
        }
    }
    for index in 0..arena.nodes.len() {
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

    fn serialize_map(self, _len: Option<usize>) -> Result<Self, SerError> {
        Err(SerError::custom("map is not a column value"))
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

    fn serialize_key<T: ?Sized + Serialize>(&mut self, _key: &T) -> Result<(), SerError> {
        Err(SerError::custom("map is not a column value"))
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, _value: &T) -> Result<(), SerError> {
        Err(SerError::custom("map is not a column value"))
    }
    fn end(self) -> Result<(), SerError> {
        Err(SerError::custom("map is not a column value"))
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
