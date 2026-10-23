//! Row schemas, reusable field buffers, and checked borrowed projections.
mod client;
mod composite;
mod variable;
#[cfg(feature = "tcp")]
mod wire_bridge;

use std::collections::{BTreeMap, HashMap};

pub use client::RowClient;
pub use composite::{MapBuffer, MapRef, MapView, NullableBuffer, NullableView};
pub use variable::{ArrayView, BinaryView, Bytes, FlatArray, FlatBytes, StringView};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[cfg(feature = "tcp")]
use crate::NativeBlock;
use crate::{
    Batch, Block, BufferPolicy, Column, MemoryBuffer, Result,
    buffer_policy::Retention,
    column::{AlignedBytes, ColumnData},
    error::schema,
};

/// A table schema with reusable writes and checked borrowed reads.
///
/// ```
/// use clickhouse::Row;
/// #[derive(Row)]
/// struct Event {
///     id: u64,
///     name: String,
/// }
/// let mut batch = EventBatch::with_capacity(4);
/// batch.push_ref(EventRef {
///     id: 1,
///     name: "borrowed",
/// });
/// let block = batch.to_block()?;
/// let view = EventBlockView::try_from_block(&block)?;
/// assert_eq!(view.get(0).map(|row| row.name), Some("borrowed"));
/// # Ok::<(), clickhouse::Error>(())
/// ```
///
/// Borrowed row values cannot outlive the block:
/// ```compile_fail
/// use clickhouse::Row;
/// #[derive(Row)]
/// struct Event { id: u64, name: String }
/// let text = {
///     let mut batch = EventBatch::default();
///     batch.push_ref(EventRef { id: 1, name: "value" });
///     let block = batch.to_block().unwrap();
///     EventBlockView::try_from_block(&block).unwrap().get(0).unwrap().name
/// };
/// println!("{text}");
/// ```
///
/// Unsupported generic schemas fail at the derive site:
/// ```compile_fail
/// use clickhouse::Row;
/// #[derive(Row)]
/// struct Generic<T> { value: T }
/// ```
pub trait Row: Sized {
    type Batch: Batch<Row = Self>;
    type View<'a>: BlockView<'a, Row = Self>;
    const TABLE_NAME: Option<&'static str> = None;
    const COLUMN_NAMES: &'static [&'static str];
}
/// A checked lifetime-bound projection into an owned block.
pub trait BlockView<'a>: Sized {
    type Row: Row;
    type Ref: Copy;
    /// Validate all column types and buffers before exposing rows.
    /// # Errors
    /// Missing columns, invalid encodings, or incompatible types reject the view.
    fn try_from_block(block: &'a Block) -> Result<Self>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get(&self, row_idx: usize) -> Option<Self::Ref>;
}

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

/// Materialize a field from its borrowed projection, allocating only for owned values.
pub trait OwnedField: Field {
    fn owned(value: Self::Input<'_>) -> Self;
}
macro_rules! owned_fixed {($($ty:ty),*)=>{$(impl OwnedField for $ty{fn owned(v:Self)->Self{v}})*};}
owned_fixed!(
    u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, bool
);
impl OwnedField for String {
    fn owned(v: &str) -> Self {
        String::from(v)
    }
}
impl<T: Fixed> OwnedField for Vec<T> {
    fn owned(v: &[T]) -> Self {
        v.to_vec()
    }
}
impl<T: OwnedField> OwnedField for Option<T> {
    fn owned(v: Self::Input<'_>) -> Self {
        v.map(T::owned)
    }
}
impl<K: OwnedField + Ord, V: OwnedField> OwnedField for BTreeMap<K, V> {
    fn owned(v: Self::Input<'_>) -> Self {
        v.iter().map(|(k, v)| (K::owned(k), V::owned(v))).collect()
    }
}
impl<K: OwnedField + Eq + std::hash::Hash, V: OwnedField> OwnedField for HashMap<K, V> {
    fn owned(v: Self::Input<'_>) -> Self {
        v.iter().map(|(k, v)| (K::owned(k), V::owned(v))).collect()
    }
}
/// A derived row that does not require borrowed input lifetimes.
pub trait OwnedRow: Row {
    /// Materialize a row from checked borrowed fields.
    fn from_ref<'a>(value: <Self::View<'a> as BlockView<'a>>::Ref) -> Self;
}

macro_rules! buffer_memory {
    ([$($generics:tt)*] $ty:ty; $rows:ident; $($field:ident),+) => {
        impl<$($generics)*> MemoryBuffer for $ty {
            fn allocated_bytes(&self) -> usize { 0 $(+ self.$field.allocated_bytes())+ }
            fn row_capacity(&self) -> usize { self.$rows.capacity() }
            fn clear_retained(&mut self, retention: Retention) {
                $(self.$field.clear_retained(retention);)+
            }
            fn shrink_to_fit(&mut self) { $(self.$field.shrink_to_fit();)+ }
        }
        impl<$($generics)*> $ty {
            /// Exact aggregate Vec capacity in bytes, including nested values.
            pub fn allocated_bytes(&self) -> usize { MemoryBuffer::allocated_bytes(self) }
            /// Clear lengths while retaining capacity within the default budget.
            pub fn clear(&mut self) { MemoryBuffer::clear(self); }
            /// Clear lengths and trim excess capacity using an aggregate budget.
            pub fn clear_with_policy(&mut self, policy: BufferPolicy) { MemoryBuffer::clear_with_policy(self, policy); }
            /// Clear lengths while retaining all warmed capacity.
            pub fn clear_unbounded(&mut self) { MemoryBuffer::clear_unbounded(self); }
            /// Release unused capacity while preserving current values.
            pub fn shrink_to_fit(&mut self) { MemoryBuffer::shrink_to_fit(self); }
        }
    };
}
buffer_memory!([] FlatBytes; offsets; offsets, bytes);
buffer_memory!([T] FlatArray<T>; offsets; offsets, values);
buffer_memory!([T: Field] NullableBuffer<T>; null_map; null_map, values);
buffer_memory!([K: Field, V: Field] MapBuffer<K,V>; offsets; offsets, keys, values);

#[cfg(feature = "tcp")]
pub(crate) fn to_native_block(block: &Block) -> Result<NativeBlock> {
    let mut native = NativeBlock::new();
    for index in 0..block.column_count() {
        let column = block.column(index)?;
        let wire = wire_bridge::to_wire(column)?;
        native.push_wire_column(&column.name, &wire)?;
    }
    Ok(native)
}

#[cfg(feature = "tcp")]
pub(crate) fn from_native_block(native: &NativeBlock) -> Result<Block> {
    let mut block = Block::with_capacity(native.columns(), native.rows());
    for index in 0..native.columns() {
        let name = native.column_name(index);
        let handle = native.wire_column(index);
        let handle = handle
            .as_ref()
            .ok_or_else(|| crate::Error::block(format!("native column `{name}` handle is null")))?;
        let column = wire_bridge::from_wire(handle, &name)?;
        block.append_column(column)?;
    }
    Ok(block)
}
