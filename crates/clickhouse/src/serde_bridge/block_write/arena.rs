//! Intermediate column arena for the serde block writer.
//!
//! Field names become column names. Column kinds are inferred from the first
//! value seen for each field, and every later row must agree. `Option<T>`
//! becomes `Nullable(T)`, `Vec<T>` becomes `Array(T)`, `Map<K, V>` becomes
//! `Map(K,V)`, and byte strings use the same flat storage as `String`.

use std::borrow::Cow;

use super::super::SerError;
use crate::{
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
    Map {
        offsets: Vec<u64>,
        keys: usize,
        values: usize,
    },
}

/// One map currently being serialized.
pub(super) struct MapFrame {
    pub(super) index: usize,
    pub(super) keys: usize,
    pub(super) values: usize,
}

#[derive(Default)]
pub(super) struct Arena {
    nodes: Vec<Node>,
}

impl Arena {
    pub(super) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub(super) fn name(&self, index: usize) -> &str {
        &self.nodes[index].name
    }

    pub(super) fn alloc(&mut self, name: Cow<'static, str>) -> usize {
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

    /// Start one map value and return the key and value columns.
    pub(super) fn begin_map(&mut self, index: usize) -> Result<(usize, usize), SerError> {
        let pending = match &self.nodes[index].kind {
            Kind::Map { keys, values, .. } => return Ok((*keys, *values)),
            Kind::Unknown { pending } => *pending,
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` is not a map",
                    self.nodes[index].name
                )));
            }
        };
        let keys = self.alloc(Cow::Borrowed(""));
        let values = self.alloc(Cow::Borrowed(""));
        self.nodes[index].kind = Kind::Map {
            offsets: Vec::new(),
            keys,
            values,
        };
        for _ in 0..pending {
            let end = self.nodes[keys].rows as u64;
            if let Kind::Map { offsets, .. } = &mut self.nodes[index].kind {
                offsets.push(end);
            }
        }
        Ok((keys, values))
    }

    /// Finish one map value after every key/value pair has been appended.
    pub(super) fn end_map(&mut self, index: usize) -> Result<(), SerError> {
        let (keys, values) = match &self.nodes[index].kind {
            Kind::Map { keys, values, .. } => (*keys, *values),
            _ => {
                return Err(SerError::custom(format!(
                    "column `{}` is not a map",
                    self.nodes[index].name
                )));
            }
        };
        if self.nodes[keys].rows != self.nodes[values].rows {
            return Err(SerError::custom(format!(
                "column `{}` has unequal map key and value counts",
                self.nodes[index].name
            )));
        }
        let end = self.nodes[keys].rows as u64;
        if let Kind::Map { offsets, .. } = &mut self.nodes[index].kind {
            offsets.push(end);
        }
        self.nodes[index].rows += 1;
        Ok(())
    }

    /// Append the default value for the current kind without counting a row.
    fn write_placeholder(&mut self, index: usize) -> Result<(), SerError> {
        let action = match &self.nodes[index].kind {
            Kind::Fixed { width, .. } => Placeholder::Fixed(*width),
            Kind::String { bytes, .. } => Placeholder::String(bytes.len() as u64),
            Kind::Array { inner, .. } => Placeholder::Array(self.nodes[*inner].rows as u64),
            Kind::Map { keys, .. } => Placeholder::Map(self.nodes[*keys].rows as u64),
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
            Placeholder::Map(end) => {
                if let Kind::Map { offsets, .. } = &mut self.nodes[index].kind {
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

    pub(super) fn append_fixed(
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

    pub(super) fn append_string(&mut self, index: usize, value: &[u8]) -> Result<(), SerError> {
        self.ensure_string(index)?;
        if let Kind::String { offsets, bytes } = &mut self.nodes[index].kind {
            bytes.extend_from_slice(value);
            offsets.push(bytes.len() as u64);
        }
        self.nodes[index].rows += 1;
        Ok(())
    }

    pub(super) fn note_none(&mut self, index: usize) -> Result<(), SerError> {
        let inner = self.ensure_nullable(index)?;
        if let Kind::Nullable { null_map, .. } = &mut self.nodes[index].kind {
            null_map.push(1);
        }
        self.nodes[index].rows += 1;
        self.push_placeholder(inner)
    }

    pub(super) fn begin_some(&mut self, index: usize) -> Result<usize, SerError> {
        let inner = self.ensure_nullable(index)?;
        if let Kind::Nullable { null_map, .. } = &mut self.nodes[index].kind {
            null_map.push(0);
        }
        self.nodes[index].rows += 1;
        Ok(inner)
    }

    pub(super) fn begin_array(&mut self, index: usize) -> Result<usize, SerError> {
        self.ensure_array(index)
    }

    pub(super) fn end_array(&mut self, index: usize) -> Result<(), SerError> {
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
    pub(super) fn resolve(&mut self, index: usize) -> Result<(), SerError> {
        let action = match &self.nodes[index].kind {
            Kind::Nullable { inner, .. } | Kind::Array { inner, .. } => Action::One(*inner),
            Kind::Map { keys, values, .. } => Action::Pair(*keys, *values),
            Kind::Unknown { pending } => Action::Fallback(*pending),
            Kind::Fixed { .. } | Kind::String { .. } => Action::Done,
        };
        match action {
            Action::One(inner) => self.resolve(inner),
            Action::Pair(keys, values) => {
                self.resolve(keys)?;
                self.resolve(values)
            }
            Action::Fallback(pending) => {
                self.nodes[index].kind = Kind::String {
                    offsets: Vec::new(),
                    bytes: Vec::new(),
                };
                self.flush_pending(index, pending)
            }
            Action::Done => Ok(()),
        }
    }

    pub(super) fn take_column(&mut self, index: usize) -> Result<Column, Error> {
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
            Kind::Map {
                offsets,
                keys,
                values,
            } => {
                if offsets.len() != rows {
                    return Err(Error::serde(format!(
                        "column `{name}` has an inconsistent map row count"
                    )));
                }
                let keys = self.take_column(keys)?;
                let values = self.take_column(values)?;
                if keys.len() != values.len() {
                    return Err(Error::serde(format!(
                        "column `{name}` has unequal map key and value counts"
                    )));
                }
                validate_offsets(&offsets, keys.len()).map_err(|error| {
                    Error::serde(format!("column `{name}` has invalid offsets: {error}"))
                })?;
                let type_name = Cow::Owned(format!("Map({},{})", keys.type_name, values.type_name));
                (
                    type_name,
                    ColumnData::Map {
                        offsets,
                        keys: Box::new(keys),
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
    Map(u64),
    Nullable(usize),
    Unknown,
}

/// How one node resolves once every row has been visited.
enum Action {
    One(usize),
    Pair(usize, usize),
    Fallback(usize),
    Done,
}
