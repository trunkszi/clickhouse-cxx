//! Flat variable-length values shared by batch writes and borrowed rows.
use super::{Field, Fixed};
use crate::{
    Column, Result,
    column::{ColumnData, validate_offsets},
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
    pub fn get(&self, i: usize) -> Option<&'a str> {
        let end = *self.offsets.get(i)? as usize;
        let start = if i == 0 {
            0
        } else {
            self.offsets[i - 1] as usize
        };
        std::str::from_utf8(self.bytes.get(start..end)?).ok()
    }
}
impl Field for String {
    type Input<'a> = &'a str;
    type Buffer = FlatBytes;
    type View<'a> = StringView<'a>;
    fn reserve(b: &mut FlatBytes, n: usize) {
        b.offsets.reserve(n);
    }
    fn push(b: &mut FlatBytes, s: &str) {
        b.bytes.extend_from_slice(s.as_bytes());
        b.offsets.push(b.bytes.len() as u64);
    }
    fn clear(b: &mut FlatBytes) {
        b.offsets.clear();
        b.bytes.clear();
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
        let v = StringView { offsets, bytes };
        if (0..offsets.len()).any(|i| v.get(i).is_none()) {
            return Err(schema("invalid UTF-8 string row"));
        }
        Ok(v)
    }
    fn view_len(v: StringView<'_>) -> usize {
        v.offsets.len()
    }
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
    fn push(b: &mut Self::Buffer, v: &[T]) {
        b.values.extend_from_slice(v);
        b.offsets.push(b.values.len() as u64);
    }
    fn clear(b: &mut Self::Buffer) {
        b.offsets.clear();
        b.values.clear();
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
    fn view_len(v: Self::View<'_>) -> usize {
        v.offsets.len()
    }
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<&'a [T]> {
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
    fn push(b: &mut FlatBytes, v: &[u8]) {
        b.bytes.extend_from_slice(v);
        b.offsets.push(b.bytes.len() as u64);
    }
    fn clear(b: &mut FlatBytes) {
        b.offsets.clear();
        b.bytes.clear();
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
    fn view_len(v: BinaryView<'_>) -> usize {
        v.offsets.len()
    }
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<Self::Input<'a>> {
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
