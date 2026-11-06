//! Column storage: owned aligned buffers and the native column family.

#[cfg(feature = "tcp")]
mod composite;
#[cfg(feature = "tcp")]
mod composite_bytes;
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
use uarch::prelude::*;
#[cfg(feature = "tcp")]
pub use variable::{FixedStringColumn, StringColumn};
#[cfg(feature = "tcp")]
pub use wire::{TypeCode, WireColumn};
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::{BufferPolicy, MemoryBuffer, Result, buffer_policy::Retention, error::schema};

#[cfg(feature = "tcp")]
pub(super) fn borrowed_column(
    handle: &cxx::UniquePtr<crate::sys::ColumnHandle>,
) -> crate::wire_error::WireResult<&crate::sys::ColumnHandle> {
    handle
        .as_ref()
        .ok_or_else(|| crate::wire_error::WireError::Unsupported("column handle is null".into()))
}

#[cfg(feature = "tcp")]
pub(super) fn pinned_column(
    handle: &mut cxx::UniquePtr<crate::sys::ColumnHandle>,
) -> crate::wire_error::WireResult<std::pin::Pin<&mut crate::sys::ColumnHandle>> {
    handle
        .as_mut()
        .ok_or_else(|| crate::wire_error::WireError::Unsupported("column handle is null".into()))
}

type AlignedChunk = CacheAligned<[u8; 64]>;
assert_layout!(AlignedChunk, size = PowerOfTwo::<6>::VALUE, align = PowerOfTwo::<6>::VALUE, fields = { value: 0 });

/// Contiguous cache-aligned storage for zero-copy numeric projections.
#[derive(Default)]
pub struct AlignedBytes {
    words: Vec<AlignedChunk>,
    len: usize,
}
impl AlignedBytes {
    /// Copy bytes into a single contiguous aligned allocation.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let chunks = PowerOfTwo::<6>::div_usize(bytes.len())
            + usize::from(PowerOfTwo::<6>::rem_usize(bytes.len()) != 0);
        let mut storage = Self {
            words: vec![CacheAligned::new([0; 64]); chunks],
            len: bytes.len(),
        };
        // SAFETY: each chunk contains exactly 64 initialized bytes, without
        // inter-chunk padding (proved above). This slice is confined to len and
        // the mutable owner prevents aliased access during the copy.
        let destination = unsafe {
            core::slice::from_raw_parts_mut(storage.words.as_mut_ptr().cast::<u8>(), bytes.len())
        };
        destination.copy_from_slice(bytes);
        storage
    }
    pub(crate) fn allocation_size(bytes: usize) -> Option<usize> {
        PowerOfTwo::<6>::checked_align_up_usize(bytes)
    }
    #[hot_inline]
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: the exact chunk size proves contiguous initialized bytes; len
        // never exceeds their extent. Empty Vec pointers remain non-null/aligned.
        unsafe { core::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }
    pub fn clear(&mut self) {
        self.len = 0;
        self.words.clear();
    }
}

/// Validate byte flags in batches without a data-dependent branch per flag.
#[hot_inline]
pub(crate) fn valid_null_map(flags: &[u8]) -> bool {
    for chunk in flags.chunks(64) {
        let (groups, tail) = chunk.as_chunks::<4>();
        let mut invalid = [Bitmap64::EMPTY; 4];
        for (group, flags) in groups.iter().enumerate() {
            let bit = group * 4;
            invalid[0] |= Bitmap64::new(u64::from(flags[0] > 1) << bit);
            invalid[1] |= Bitmap64::new(u64::from(flags[1] > 1) << (bit + 1));
            invalid[2] |= Bitmap64::new(u64::from(flags[2] > 1) << (bit + 2));
            invalid[3] |= Bitmap64::new(u64::from(flags[3] > 1) << (bit + 3));
        }
        for (lane, &flag) in tail.iter().enumerate() {
            invalid[lane] |= Bitmap64::new(u64::from(flag > 1) << (groups.len() * 4 + lane));
        }
        let invalid = reduce_lanes::<4, _, _>(
            &invalid,
            Bitmap64::EMPTY,
            Bitmap64::EMPTY,
            |left, right| left.union(*right),
            Bitmap64::union,
        );
        if invalid.count_ones() != 0 {
            return false;
        }
    }
    true
}

/// Adjacent offset comparisons are independent, including chunk boundaries.
#[hot_inline]
pub(crate) fn monotonic_offsets(offsets: &[u64]) -> bool {
    let (chunks, tail) = offsets.as_chunks::<4>();
    let ordered = reduce_lanes::<4, _, _>(
        chunks,
        true,
        true,
        |valid, chunk| {
            valid
                & ilp_all!(
                    chunk[0] <= chunk[1],
                    chunk[1] <= chunk[2],
                    chunk[2] <= chunk[3]
                )
        },
        |a, b| a & b,
    );
    let boundaries = chunks.windows(2).all(|pair| pair[0][3] <= pair[1][0]);
    let terminal = match (chunks.last(), tail.first()) {
        (Some(chunk), Some(first)) => chunk[3] <= *first,
        _ => true,
    };
    ilp_all!(
        ordered,
        boundaries,
        terminal,
        tail.windows(2).all(|pair| pair[0] <= pair[1])
    )
}

/// Hint the next offset cache line during repeated row access.
#[hot_inline]
pub(crate) fn prefetch_offsets(offsets: &[u64], row: usize) {
    if let Some(next) = row.checked_add(8).and_then(|next| offsets.get(next)) {
        prefetch_read::<L1, _>(next);
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

    #[hot_inline]
    pub fn len(&self) -> usize {
        self.rows
    }
    #[hot_inline]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    /// Project a fixed-width buffer without copying.
    /// # Errors
    /// Wrong storage, length, or alignment rejects the cast.
    #[hot_inline]
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
    #[hot_inline]
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
    #[hot_inline]
    pub fn read_bytes_into(&self, row_idx: usize, dst: &mut Vec<u8>) -> Result<usize> {
        let ColumnData::Strings { offsets, bytes } = &self.data else {
            return Err(schema("expected string storage"));
        };
        prefetch_offsets(offsets, row_idx);
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
#[hot_inline]
pub fn validate_offsets(offsets: &[u64], values: usize) -> Result<()> {
    if !ilp_all!(
        monotonic_offsets(offsets),
        offsets.last().copied().unwrap_or(0) == values as u64
    ) {
        return Err(schema("invalid flat column offsets"));
    }
    Ok(())
}

pub(crate) fn validate_child_type(expected: &str, child: &Column) -> Result<()> {
    if !crate::type_layout::same_type(expected, &child.type_name) {
        return Err(schema("nested column type mismatch"));
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
                let width = crate::type_layout::fixed_width(&self.type_name).unwrap_or(16);
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
#[path = "../../tests/column/storage.rs"]
mod tests;
