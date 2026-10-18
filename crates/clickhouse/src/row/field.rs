//! Typed reusable field buffers and checked borrowed column access.
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    Column, Result,
    column::{AlignedBytes, ColumnData},
    error::schema,
};
/// A field schema used by generated SoA buffers and borrowed projections.
pub trait Field: Sized + 'static {
    type Input<'a>: Copy;
    type Buffer: Default + crate::MemoryBuffer;
    type View<'a>: Copy;
    fn reserve(buffer: &mut Self::Buffer, rows: usize);
    fn push(buffer: &mut Self::Buffer, value: Self::Input<'_>);
    /// Clear a field using the balanced default retention budget.
    fn clear(buffer: &mut Self::Buffer) {
        crate::MemoryBuffer::clear(buffer);
    }
    fn len(buffer: &Self::Buffer) -> usize;
    fn capacity(buffer: &Self::Buffer) -> usize;
    fn empty() -> Self::Input<'static>;
    fn borrow(&self) -> Self::Input<'_>;
    /// Validate and project a column without copying its values.
    /// # Errors
    /// Invalid type, layout, offsets, or values reject the view.
    fn view(column: &Column) -> Result<Self::View<'_>>;
    fn view_len(view: Self::View<'_>) -> usize;
    fn get<'a>(view: Self::View<'a>, row: usize) -> Option<Self::Input<'a>>;
    /// Copy a reusable buffer into an owned block column.
    /// # Errors
    /// Invalid internal column state rejects materialization.
    fn column(buffer: &Self::Buffer, name: &'static str) -> Result<Column>;
}
/// A fixed-width field with all bit patterns valid and native wire layout.
pub trait Fixed:
    Field<Buffer = Vec<Self>> + FromBytes + IntoBytes + Immutable + KnownLayout + Copy + Default
{
    const TYPE_NAME: &'static str;
}
macro_rules! fixed {
    ($($ty:ty => $name:literal),* $(,)?) => {$ (
        impl Fixed for $ty { const TYPE_NAME: &'static str = $name; }
        impl Field for $ty {
            type Input<'a> = Self;
            type Buffer = Vec<Self>;
            type View<'a> = &'a [Self];
            fn reserve(b: &mut Self::Buffer, n: usize) { b.reserve(n); }
            fn push(b: &mut Self::Buffer, value: Self) { b.push(value); }
            fn len(b: &Self::Buffer) -> usize { b.len() }
            fn capacity(b: &Self::Buffer) -> usize { b.capacity() }
            fn empty() -> Self { Self::default() }
            fn borrow(&self) -> Self { *self }
            fn view(c: &Column) -> Result<&[Self]> {
                if c.type_name != $name { return Err(schema("fixed field type mismatch")); }
                let view = c.slice::<Self>()?;
                if view.len() != c.len() { return Err(schema("fixed field row count mismatch")); }
                Ok(view)
            }
            fn view_len(v: &[Self]) -> usize { v.len() }
            fn get<'a>(v: Self::View<'a>, i: usize) -> Option<Self::Input<'a>> { v.get(i).copied() }
            fn column(b: &Self::Buffer, name: &'static str) -> Result<Column> {
                Ok(Column { name: name.into(), type_name: $name.into(), rows: b.len(), data: ColumnData::Fixed(AlignedBytes::from_bytes(b.as_slice().as_bytes())) })
            }
        }
    )*};
}
fixed! { u8=>"UInt8",u16=>"UInt16",u32=>"UInt32",u64=>"UInt64",u128=>"UInt128",i8=>"Int8",i16=>"Int16",i32=>"Int32",i64=>"Int64",i128=>"Int128",f32=>"Float32",f64=>"Float64" }
impl Field for bool {
    type Input<'a> = bool;
    type Buffer = Vec<u8>;
    type View<'a> = &'a [u8];
    fn reserve(b: &mut Vec<u8>, n: usize) {
        b.reserve(n);
    }
    fn push(b: &mut Vec<u8>, value: bool) {
        b.push(u8::from(value));
    }
    fn len(b: &Vec<u8>) -> usize {
        b.len()
    }
    fn capacity(b: &Vec<u8>) -> usize {
        b.capacity()
    }
    fn empty() -> bool {
        false
    }
    fn borrow(&self) -> bool {
        *self
    }
    fn view(c: &Column) -> Result<&[u8]> {
        if c.type_name != "Bool" {
            return Err(schema("bool field type mismatch"));
        }
        let values = c.slice::<u8>()?;
        if values.len() != c.len() || values.iter().any(|v| *v > 1) {
            return Err(schema("invalid bool column"));
        }
        Ok(values)
    }
    fn view_len(v: &[u8]) -> usize {
        v.len()
    }
    fn get<'a>(v: Self::View<'a>, i: usize) -> Option<Self::Input<'a>> {
        v.get(i).map(|v| *v == 1)
    }
    fn column(b: &Vec<u8>, name: &'static str) -> Result<Column> {
        Ok(Column {
            name: name.into(),
            type_name: "Bool".into(),
            rows: b.len(),
            data: ColumnData::Fixed(AlignedBytes::from_bytes(b)),
        })
    }
}
