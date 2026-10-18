//! Native TCP implementation of the typed row client boundary.
//!
//! The row layer owns pure-Rust columns; this module converts them to and
//! from native blocks at the transport edge. The wire bridge
//! supplies the fixed-width, String, Bool, Array, Nullable, and Map encodings.

use super::wire_bridge;
use crate::{Block, Client, NativeBlock, Result, error::Error, row::RowClient};

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

impl RowClient for Client {
    fn insert_block(&mut self, table: &str, block: &Block) -> Result<()> {
        let mut native = to_native_block(block)?;
        Client::insert_block(self, table, &mut native)
    }

    fn query_blocks(&mut self, sql: &str) -> Result<Vec<Block>> {
        let native_blocks = self.query_native_blocks(sql)?;
        let mut blocks = Vec::with_capacity(native_blocks.len());
        for native in &native_blocks {
            if native.rows() == 0 {
                continue;
            }
            blocks.push(from_native(native)?);
        }
        Ok(blocks)
    }
}
