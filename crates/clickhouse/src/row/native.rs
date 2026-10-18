//! Native TCP implementation of the typed row client boundary.
//!
//! The row layer owns pure-Rust columns; this module converts them to and
//! from native blocks at the transport edge. The wire bridge
//! supplies the fixed-width, String, Bool, Array, Nullable, and Map encodings.

use super::wire_bridge;
use crate::{Block, NativeBlock, Result, error::Error};

pub(crate) fn to_native_block(block: &Block) -> Result<NativeBlock> {
    let mut native = NativeBlock::new();
    for index in 0..block.column_count() {
        let column = block.column(index)?;
        let wire = wire_bridge::to_wire(column)?;
        native.push_wire_column(&column.name, &wire)?;
    }
    Ok(native)
}

pub(crate) fn from_native(native: &NativeBlock) -> Result<Block> {
    let mut block = Block::with_capacity(native.columns(), native.rows());
    for index in 0..native.columns() {
        let name = native.column_name(index);
        let handle = native.wire_column(index);
        let handle = handle
            .as_ref()
            .ok_or_else(|| Error::block(format!("native column `{name}` handle is null")))?;
        let column = wire_bridge::from_wire(handle, &name)?;
        block.append_column(column)?;
    }
    Ok(block)
}
