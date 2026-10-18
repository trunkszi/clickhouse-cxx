//! Column storage: owned aligned buffers and the native column family.

#[cfg(feature = "tcp")]
mod composite;
mod fixed;
#[cfg(feature = "tcp")]
mod variable;
#[cfg(feature = "tcp")]
mod wire;

use std::borrow::Cow;

#[cfg(feature = "tcp")]
pub use composite::{
    ArrayColumn, Decimal256Column, GeoColumn, GeoKind, LowCardinalityColumn, MapColumn,
    NothingColumn, NullableColumn, TupleColumn,
};
#[cfg(feature = "tcp")]
pub use fixed::FixedWireColumn;
pub use fixed::{
    ChBool, Date, Date32, DateTime, DateTime64, Decimal32, Decimal64, Decimal128, Enum8, Enum16,
    FixedKind, FixedType, Ipv4, Ipv6, Time, Time64, Uuid, decimal_kind,
};
#[cfg(feature = "tcp")]
pub use variable::{FixedStringColumn, StringColumn};
#[cfg(feature = "tcp")]
pub use wire::{TypeCode, WireColumn};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{BufferPolicy, MemoryBuffer, Result, buffer_policy::Retention, error::schema};
/// Aligned storage preserves at least the alignment of all numeric primitives.
#[derive(Default)]
pub struct AlignedBytes {
    words: Vec<u128>,
    len: usize,
}
impl AlignedBytes {
    /// Copy bytes into a single contiguous aligned allocation.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut words = vec![0u128; bytes.len().div_ceil(16)];
        words.as_mut_slice().as_mut_bytes()[..bytes.len()].copy_from_slice(bytes);
        Self {
            words,
            len: bytes.len(),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.words.as_slice().as_bytes()[..self.len]
    }
    pub fn clear(&mut self) {
        self.len = 0;
        self.words.clear();
    }
}
/// Recursive column storage; variable-length rows share flat value buffers.
pub enum ColumnData {
    Fixed(AlignedBytes),
    Strings {
        offsets: Vec<u64>,
        bytes: Vec<u8>,
    },
    Array {
        offsets: Vec<u64>,
        values: Box<Column>,
    },
    Nullable {
        null_map: Vec<u8>,
        values: Box<Column>,
    },
    Map {
        offsets: Vec<u64>,
        keys: Box<Column>,
        values: Box<Column>,
    },
}
/// A named typed contiguous column.
pub struct Column {
    pub name: Cow<'static, str>,
    pub type_name: Cow<'static, str>,
    pub data: ColumnData,
    pub(crate) rows: usize,
}
impl Column {
    pub(crate) fn same_storage(&self, other: &Self) -> bool {
        if self.name != other.name || self.type_name != other.type_name {
            return false;
        }
        match (&self.data, &other.data) {
            (ColumnData::Fixed(_), ColumnData::Fixed(_))
            | (ColumnData::Strings { .. }, ColumnData::Strings { .. }) => true,
            (ColumnData::Array { values: a, .. }, ColumnData::Array { values: b, .. })
            | (ColumnData::Nullable { values: a, .. }, ColumnData::Nullable { values: b, .. }) => {
                a.same_storage(b)
            }
            (
                ColumnData::Map {
                    keys: a, values: b, ..
                },
                ColumnData::Map {
                    keys: c, values: d, ..
                },
            ) => a.same_storage(c) && b.same_storage(d),
            _ => false,
        }
    }
    pub(crate) fn copy_values(&mut self, source: &Self) {
        match (&mut self.data, &source.data) {
            (ColumnData::Fixed(a), ColumnData::Fixed(b)) => {
                a.words.clear();
                a.words.extend_from_slice(&b.words);
                a.len = b.len;
            }
            (
                ColumnData::Strings {
                    offsets: a,
                    bytes: b,
                },
                ColumnData::Strings {
                    offsets: c,
                    bytes: d,
                },
            ) => {
                a.clear();
                a.extend_from_slice(c);
                b.clear();
                b.extend_from_slice(d);
            }
            (
                ColumnData::Array {
                    offsets: a,
                    values: b,
                },
                ColumnData::Array {
                    offsets: c,
                    values: d,
                },
            ) => {
                a.clear();
                a.extend_from_slice(c);
                b.copy_values(d);
            }
            (
                ColumnData::Nullable {
                    null_map: a,
                    values: b,
                },
                ColumnData::Nullable {
                    null_map: c,
                    values: d,
                },
            ) => {
                a.clear();
                a.extend_from_slice(c);
                b.copy_values(d);
            }
            (
                ColumnData::Map {
                    offsets: a,
                    keys: b,
                    values: c,
                },
                ColumnData::Map {
                    offsets: d,
                    keys: e,
                    values: f,
                },
            ) => {
                a.clear();
                a.extend_from_slice(d);
                b.copy_values(e);
                c.copy_values(f);
            }
            _ => unreachable!("storage compatibility is checked before copying"),
        }
        self.rows = source.rows;
    }

    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    /// Project a fixed-width buffer without copying.
    /// # Errors
    /// Wrong storage, length, or alignment rejects the cast.
    pub fn slice<T: FromBytes + Immutable + KnownLayout>(&self) -> Result<&[T]> {
        let ColumnData::Fixed(bytes) = &self.data else {
            return Err(schema("expected a fixed-width column"));
        };
        let values = <[T]>::ref_from_bytes(bytes.as_bytes())
            .map_err(|_| schema("fixed column alignment or width mismatch"))?;
        if values.len() != self.rows {
            return Err(schema("fixed column length does not match row count"));
        }
        Ok(values)
    }
    /// Copy a fixed-width column into a caller-provided slice.
    /// # Errors
    /// Wrong storage, width, alignment, or a short destination rejects the copy.
    pub fn read_into_slice<T: FromBytes + Immutable + KnownLayout + Copy>(
        &self,
        dst: &mut [T],
    ) -> Result<usize> {
        let src = self.slice::<T>()?;
        if dst.len() < src.len() {
            return Err(schema("destination slice is too small for the column"));
        }
        dst[..src.len()].copy_from_slice(src);
        Ok(src.len())
    }
    /// Copy one string row into a caller-provided byte buffer.
    /// # Errors
    /// Wrong storage, invalid offsets, or an out-of-range row rejects the copy.
    pub fn read_bytes_into(&self, row_idx: usize, dst: &mut Vec<u8>) -> Result<usize> {
        let ColumnData::Strings { offsets, bytes } = &self.data else {
            return Err(schema("expected string storage"));
        };
        let end = *offsets
            .get(row_idx)
            .ok_or_else(|| schema("string row is out of bounds"))? as usize;
        let start = if row_idx == 0 {
            0
        } else {
            offsets[row_idx - 1] as usize
        };
        let row = bytes
            .get(start..end)
            .ok_or_else(|| schema("invalid string row bounds"))?;
        dst.clear();
        dst.extend_from_slice(row);
        Ok(row.len())
    }
    /// Exact capacity in bytes of every nested value Vec.
    pub fn allocated_bytes(&self) -> usize {
        MemoryBuffer::allocated_bytes(self)
    }
    /// Clear rows using the balanced default retention budget.
    pub fn clear(&mut self) {
        MemoryBuffer::clear(self);
    }
    /// Clear rows with one budget shared across nested value buffers.
    pub fn clear_with_policy(&mut self, policy: BufferPolicy) {
        MemoryBuffer::clear_with_policy(self, policy);
    }
    /// Clear rows without reclaiming warmed capacity.
    pub fn clear_unbounded(&mut self) {
        MemoryBuffer::clear_unbounded(self);
    }
    /// Release unused value capacity while preserving current rows.
    pub fn shrink_to_fit(&mut self) {
        MemoryBuffer::shrink_to_fit(self);
    }
}
/// Check complete offsets before any row projection uses them.
/// # Errors
/// Nonmonotone offsets or a mismatching terminal offset reject the column.
pub fn validate_offsets(offsets: &[u64], values: usize) -> Result<()> {
    if offsets.windows(2).any(|p| p[0] > p[1])
        || offsets.last().copied().unwrap_or(0) != values as u64
    {
        return Err(schema("invalid flat column offsets"));
    }
    Ok(())
}

impl MemoryBuffer for Column {
    fn allocated_bytes(&self) -> usize {
        match &self.data {
            ColumnData::Fixed(b) => b.words.allocated_bytes(),
            ColumnData::Strings { offsets, bytes } => {
                offsets.allocated_bytes() + bytes.allocated_bytes()
            }
            ColumnData::Array { offsets, values } => {
                offsets.allocated_bytes() + values.allocated_bytes()
            }
            ColumnData::Nullable { null_map, values } => {
                null_map.allocated_bytes() + values.allocated_bytes()
            }
            ColumnData::Map {
                offsets,
                keys,
                values,
            } => offsets.allocated_bytes() + keys.allocated_bytes() + values.allocated_bytes(),
        }
    }
    fn row_capacity(&self) -> usize {
        match &self.data {
            ColumnData::Fixed(b) => {
                let width = match self.type_name.as_ref() {
                    "UInt8" | "Int8" | "Bool" => 1,
                    "UInt16" | "Int16" => 2,
                    "UInt32" | "Int32" | "Float32" => 4,
                    "UInt64" | "Int64" | "Float64" => 8,
                    _ => 16,
                };
                b.words.allocated_bytes() / width
            }
            ColumnData::Strings { offsets, .. }
            | ColumnData::Array { offsets, .. }
            | ColumnData::Map { offsets, .. } => offsets.capacity(),
            ColumnData::Nullable { null_map, .. } => null_map.capacity(),
        }
    }
    fn clear_retained(&mut self, retention: Retention) {
        match &mut self.data {
            ColumnData::Fixed(b) => {
                b.words.clear_retained(retention);
                b.len = 0;
            }
            ColumnData::Strings { offsets, bytes } => {
                offsets.clear_retained(retention);
                bytes.clear_retained(retention);
            }
            ColumnData::Array { offsets, values } => {
                offsets.clear_retained(retention);
                values.clear_retained(retention);
            }
            ColumnData::Nullable { null_map, values } => {
                null_map.clear_retained(retention);
                values.clear_retained(retention);
            }
            ColumnData::Map {
                offsets,
                keys,
                values,
            } => {
                offsets.clear_retained(retention);
                keys.clear_retained(retention);
                values.clear_retained(retention);
            }
        }
        self.rows = 0;
    }
    fn shrink_to_fit(&mut self) {
        match &mut self.data {
            ColumnData::Fixed(b) => b.words.shrink_to_fit(),
            ColumnData::Strings { offsets, bytes } => {
                offsets.shrink_to_fit();
                bytes.shrink_to_fit();
            }
            ColumnData::Array { offsets, values } => {
                offsets.shrink_to_fit();
                values.shrink_to_fit();
            }
            ColumnData::Nullable { null_map, values } => {
                null_map.shrink_to_fit();
                values.shrink_to_fit();
            }
            ColumnData::Map {
                offsets,
                keys,
                values,
            } => {
                offsets.shrink_to_fit();
                keys.shrink_to_fit();
                values.shrink_to_fit();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AlignedBytes, Column, ColumnData};

    #[test]
    fn slice_rejects_storage_shorter_than_row_count() {
        let column = Column {
            name: "value".into(),
            type_name: "UInt64".into(),
            data: ColumnData::Fixed(AlignedBytes::from_bytes(&7_u64.to_le_bytes())),
            rows: 3,
        };
        assert!(column.slice::<u64>().is_err());
    }

    #[test]
    fn slice_accepts_storage_matching_row_count() {
        let column = Column {
            name: "value".into(),
            type_name: "UInt64".into(),
            data: ColumnData::Fixed(AlignedBytes::from_bytes(&7_u64.to_le_bytes())),
            rows: 1,
        };
        assert_eq!(column.slice::<u64>().expect("one row"), [7]);
    }

    #[test]
    fn read_bytes_into_checks_only_the_requested_row() {
        let column = Column {
            name: "value".into(),
            type_name: "String".into(),
            data: ColumnData::Strings {
                offsets: vec![5, 9, 12],
                bytes: b"helloworld!!".to_vec(),
            },
            rows: 3,
        };
        let mut dst = Vec::new();

        assert_eq!(column.read_bytes_into(0, &mut dst).expect("row 0"), 5);
        assert_eq!(dst, b"hello".to_vec());

        dst.extend_from_slice(b"stale");
        assert_eq!(column.read_bytes_into(1, &mut dst).expect("row 1"), 4);
        assert_eq!(dst, b"worl".to_vec());

        assert!(column.read_bytes_into(3, &mut dst).is_err());

        let nonmonotonic = Column {
            name: "value".into(),
            type_name: "String".into(),
            data: ColumnData::Strings {
                offsets: vec![5, 3],
                bytes: b"helloworld".to_vec(),
            },
            rows: 2,
        };
        assert_eq!(nonmonotonic.read_bytes_into(0, &mut dst).expect("row 0"), 5);
        assert!(nonmonotonic.read_bytes_into(1, &mut dst).is_err());
    }
}
