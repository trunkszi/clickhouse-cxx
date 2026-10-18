//! Shared contracts used by the row derive macro.
mod client;
mod field;
mod memory;
#[cfg(feature = "tcp")]
pub(crate) mod native;
#[cfg(feature = "tcp")]
pub(crate) use native::to_native_block;
mod owned;
#[cfg(feature = "tcp")]
mod wire_bridge;
pub use client::RowClient;
pub use owned::{OwnedField, OwnedRow};
mod composite;
mod variable;
pub use composite::{MapBuffer, MapRef, MapView, NullableBuffer, NullableView};
pub use field::{Field, Fixed};
pub use variable::{ArrayView, BinaryView, Bytes, FlatArray, FlatBytes, StringView};

use crate::{Batch, Block, Result};
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
