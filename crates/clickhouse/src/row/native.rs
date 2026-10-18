//! Native TCP implementation of the typed row client boundary.
//!
//! The row layer owns pure-Rust columns; this module converts them to and
//! from `clickhouse-cpp` blocks at the transport edge. The C++ matrix bridge
//! supplies the fixed-width, String, Bool, Array, Nullable, and Map encodings.

use super::cpp_bridge;
use crate::{Block, Client, NativeBlock, Result, error::Error, row::RowClient};

fn to_native(block: &Block) -> Result<NativeBlock> {
    let mut native = NativeBlock::new();
    for index in 0..block.column_count() {
        let column = block.column(index)?;
        let cpp = cpp_bridge::to_cpp(column)?;
        native.push_cpp_column(&column.name, &cpp)?;
    }
    Ok(native)
}

fn from_native(native: &NativeBlock) -> Result<Block> {
    let mut block = Block::with_capacity(native.columns(), native.rows());
    for index in 0..native.columns() {
        let name = native.column_name(index);
        let handle = native.cpp_column(index);
        let handle = handle
            .as_ref()
            .ok_or_else(|| Error::block(format!("native column `{name}` handle is null")))?;
        let column = cpp_bridge::from_cpp(handle, &name)?;
        block.append_column(column)?;
    }
    Ok(block)
}

impl RowClient for Client {
    fn insert_block(&mut self, table: &str, block: &Block) -> Result<()> {
        let mut native = to_native(block)?;
        Client::insert_block(self, table, &mut native)
    }

    fn query_blocks(&mut self, sql: &str) -> Result<Vec<Block>> {
        let native_blocks = Client::query_blocks(self, sql)?;
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
