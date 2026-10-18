//! Owned aligned column storage for checked zero-copy projections.
use std::borrow::Cow;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{Result, error::schema};
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
        <[T]>::ref_from_bytes(bytes.as_bytes())
            .map_err(|_| schema("fixed column alignment or width mismatch"))
    }
    pub fn clear(&mut self) {
        self.rows = 0;
        match &mut self.data {
            ColumnData::Fixed(bytes) => bytes.clear(),
            ColumnData::Strings { offsets, bytes } => {
                offsets.clear();
                bytes.clear();
            }
            ColumnData::Array { offsets, values } => {
                offsets.clear();
                values.clear();
            }
            ColumnData::Nullable { null_map, values } => {
                null_map.clear();
                values.clear();
            }
            ColumnData::Map {
                offsets,
                keys,
                values,
            } => {
                offsets.clear();
                keys.clear();
                values.clear();
            }
        }
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
