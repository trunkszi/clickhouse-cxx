//! Flat variable-length values shared by batch writes and borrowed rows.
use uarch::prelude::*;

use super::{Field, Fixed};
use crate::{
    Column, Result,
    column::{ColumnData, prefetch_offsets, validate_offsets},
    error::schema,
};
/// String bytes and exclusive end offsets; no per-row allocation.
#[derive(Default)]
pub struct FlatBytes {
    pub offsets: Vec<u64>,
    pub bytes: Vec<u8>,
}
/// Primitive array values and exclusive end offsets.
pub struct FlatArray<T> {
    pub offsets: Vec<u64>,
    pub values: Vec<T>,
}
impl<T> Default for FlatArray<T> {
    fn default() -> Self {
        Self {
            offsets: Vec::new(),
            values: Vec::new(),
        }
    }
}
/// A validated borrowed UTF-8 column.
#[derive(Clone, Copy)]
pub struct StringView<'a> {
    offsets: &'a [u64],
    bytes: &'a [u8],
}
impl<'a> StringView<'a> {
    pub fn offsets(&self) -> &'a [u64] {
        self.offsets
    }
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    #[hot_inline]
    pub fn get(&self, i: usize) -> Option<&'a str> {
        prefetch_offsets(self.offsets, i);
        let end = *self.offsets.get(i)? as usize;
        let start = if i == 0 {
            0
        } else {
            self.offsets[i - 1] as usize
        };
        let bytes = self.bytes.get(start..end)?;
        // SAFETY: `String::view` is the only constructor and it rejects the column
        // unless every row is UTF-8. ASCII columns are accepted by a word scan;
        // any high bit is checked per row before this view exists.
        Some(unsafe { std::str::from_utf8_unchecked(bytes) })
    }
}
fn bytes_are_ascii(bytes: &[u8]) -> bool {
    let (chunks, tail) = bytes.as_chunks::<8>();
    let high_bits = reduce_lanes::<4, _, _>(
        chunks,
        0,
        0,
        |bits, chunk| bits | u64::from_le_bytes(*chunk),
        |a, b| a | b,
    );
    ilp_all!(
        high_bits & 0x8080_8080_8080_8080 == 0,
        tail.iter().all(|byte| byte & 0x80 == 0)
    )
}
fn rows_are_utf8(offsets: &[u64], bytes: &[u8]) -> bool {
    if bytes_are_ascii(bytes) {
        return true;
    }
    let mut start = 0usize;
    for &end in offsets {
        let end = end as usize;
        let Some(row) = bytes.get(start..end) else {
            return false;
        };
        if std::str::from_utf8(row).is_err() {
            return false;
        }
        start = end;
    }
    true
}
impl Field for String {
    type Input<'a> = &'a str;
    type Buffer = FlatBytes;
    type View<'a> = StringView<'a>;
    fn reserve(b: &mut FlatBytes, n: usize) {
        b.offsets.reserve(n);
    }
    #[hot_inline]
    fn push(b: &mut FlatBytes, s: &str) {
        b.bytes.extend_from_slice(s.as_bytes());
        b.offsets.push(b.bytes.len() as u64);
    }
    fn len(b: &FlatBytes) -> usize {
        b.offsets.len()
    }
    fn capacity(b: &FlatBytes) -> usize {
        b.offsets.capacity()
    }
    fn empty() -> &'static str {
        ""
    }
    fn borrow(&self) -> &str {
        self.as_str()
    }
    fn view(c: &Column) -> Result<StringView<'_>> {
        if c.type_name != "String" {
            return Err(schema("string field type mismatch"));
        }
        let ColumnData::Strings { offsets, bytes } = &c.data else {
            return Err(schema("expected string storage"));
        };
        validate_offsets(offsets, bytes.len())?;
        if offsets.len() != c.len() {
            return Err(schema("string row count mismatch"));
        }
        if !rows_are_utf8(offsets, bytes) {
            return Err(schema("invalid UTF-8 string row"));
        }
        Ok(StringView { offsets, bytes })
    }
    #[hot_inline]
    fn view_len(v: StringView<'_>) -> usize {
        v.offsets.len()
    }
    #[hot_inline]
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<Self::Input<'a>> {
        v.get(i)
    }
    fn column(b: &FlatBytes, name: &'static str) -> Result<Column> {
        validate_offsets(&b.offsets, b.bytes.len())?;
        Ok(Column {
            name: name.into(),
            type_name: "String".into(),
            rows: b.offsets.len(),
            data: ColumnData::Strings {
                offsets: b.offsets.clone(),
                bytes: b.bytes.clone(),
            },
        })
    }
}
/// Primitive arrays borrow their value buffer directly.
#[derive(Clone, Copy)]
pub struct ArrayView<'a, T> {
    pub offsets: &'a [u64],
    pub values: &'a [T],
}
impl<T: Fixed> Field for Vec<T> {
    type Input<'a> = &'a [T];
    type Buffer = FlatArray<T>;
    type View<'a> = ArrayView<'a, T>;
    fn reserve(b: &mut Self::Buffer, n: usize) {
        b.offsets.reserve(n);
    }
    #[hot_inline]
    fn push(b: &mut Self::Buffer, v: &[T]) {
        b.values.extend_from_slice(v);
        b.offsets.push(b.values.len() as u64);
    }
    fn len(b: &Self::Buffer) -> usize {
        b.offsets.len()
    }
    fn capacity(b: &Self::Buffer) -> usize {
        b.offsets.capacity()
    }
    fn empty() -> &'static [T] {
        &[]
    }
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
    fn view(c: &Column) -> Result<Self::View<'_>> {
        if c.type_name
            .strip_prefix("Array(")
            .and_then(|s| s.strip_suffix(')'))
            != Some(T::TYPE_NAME)
        {
            return Err(schema("array field type mismatch"));
        }
        let ColumnData::Array { offsets, values } = &c.data else {
            return Err(schema("expected array storage"));
        };
        let values = T::view(values)?;
        validate_offsets(offsets, T::view_len(values))?;
        if offsets.len() != c.len() {
            return Err(schema("array row count mismatch"));
        }
        // Fixed field views are represented by a checked native slice.
        let ColumnData::Array { values: inner, .. } = &c.data else {
            return Err(schema("expected array storage"));
        };
        Ok(ArrayView {
            offsets,
            values: inner.slice::<T>()?,
        })
    }
    #[hot_inline]
    fn view_len(v: Self::View<'_>) -> usize {
        v.offsets.len()
    }
    #[hot_inline]
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<&'a [T]> {
        prefetch_offsets(v.offsets, i);
        let end = *v.offsets.get(i)? as usize;
        let start = if i == 0 { 0 } else { v.offsets[i - 1] as usize };
        v.values.get(start..end)
    }
    fn column(b: &Self::Buffer, name: &'static str) -> Result<Column> {
        validate_offsets(&b.offsets, b.values.len())?;
        Ok(Column {
            name: name.into(),
            type_name: format!("Array({})", T::TYPE_NAME).into(),
            rows: b.offsets.len(),
            data: ColumnData::Array {
                offsets: b.offsets.clone(),
                values: Box::new(T::column(&b.values, "values")?),
            },
        })
    }
}
/// A binary ClickHouse String without a UTF-8 requirement.
pub struct Bytes;
impl Field for Bytes {
    type Input<'a> = &'a [u8];
    type Buffer = FlatBytes;
    type View<'a> = BinaryView<'a>;
    fn reserve(b: &mut FlatBytes, n: usize) {
        b.offsets.reserve(n);
    }
    #[hot_inline]
    fn push(b: &mut FlatBytes, v: &[u8]) {
        b.bytes.extend_from_slice(v);
        b.offsets.push(b.bytes.len() as u64);
    }
    fn len(b: &FlatBytes) -> usize {
        b.offsets.len()
    }
    fn capacity(b: &FlatBytes) -> usize {
        b.offsets.capacity()
    }
    fn empty() -> &'static [u8] {
        &[]
    }
    fn borrow(&self) -> &[u8] {
        &[]
    }
    fn view(c: &Column) -> Result<BinaryView<'_>> {
        if c.type_name != "String" {
            return Err(schema("binary field type mismatch"));
        }
        let ColumnData::Strings { offsets, bytes } = &c.data else {
            return Err(schema("expected binary string storage"));
        };
        validate_offsets(offsets, bytes.len())?;
        if offsets.len() != c.len() {
            return Err(schema("binary row count mismatch"));
        }
        Ok(BinaryView { offsets, bytes })
    }
    #[hot_inline]
    fn view_len(v: BinaryView<'_>) -> usize {
        v.offsets.len()
    }
    #[hot_inline]
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<Self::Input<'a>> {
        prefetch_offsets(v.offsets, i);
        let end = *v.offsets.get(i)? as usize;
        let start = if i == 0 { 0 } else { v.offsets[i - 1] as usize };
        v.bytes.get(start..end)
    }
    fn column(b: &FlatBytes, name: &'static str) -> Result<Column> {
        String::column(b, name)
    }
}
/// A validated binary string projection.
#[derive(Clone, Copy)]
pub struct BinaryView<'a> {
    pub offsets: &'a [u64],
    pub bytes: &'a [u8],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_views_validate_ascii_words_unicode_rows_and_split_codepoints() {
        let mut flat = FlatBytes::default();
        for value in ["", "a", "12345678", "abcdefgh", "你好", "tail"] {
            <String as Field>::push(&mut flat, value);
        }
        let column = String::column(&flat, "text").unwrap();
        let view = String::view(&column).unwrap();
        for (index, value) in ["", "a", "12345678", "abcdefgh", "你好", "tail"]
            .into_iter()
            .enumerate()
        {
            assert_eq!(view.get(index), Some(value));
        }
        assert_eq!(view.get(usize::MAX), None);

        let column = Column {
            name: "text".into(),
            type_name: "String".into(),
            rows: 2,
            data: ColumnData::Strings {
                offsets: vec![1, 2],
                bytes: vec![0xc3, 0xa9],
            },
        };
        assert!(String::view(&column).is_err());
        assert_eq!(Bytes::view(&column).unwrap().bytes, &[0xc3, 0xa9]);
    }
}
