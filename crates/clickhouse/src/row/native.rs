//! Native TCP implementation of the typed row client boundary.
//!
//! The row layer owns pure-Rust columns; this module converts them to and
//! from `clickhouse-cpp` blocks at the transport edge. Primitive fixed-width
//! and String columns (including binary `#[row(bytes)]` payloads) are
//! supported; composite columns remain available through the in-memory and
//! HTTP paths until the native bridge grows them.

use std::borrow::Cow;

use crate::{
    Block, Client, Column, NativeBlock, Result, column::ColumnData, error::Error, row::RowClient,
};

fn unsupported(column: &Column) -> Error {
    Error::SchemaMismatch {
        table: None,
        reason: Cow::Owned(format!(
            "native transport does not support column type `{}`",
            column.type_name
        )),
    }
}

fn to_native(block: &Block) -> Result<NativeBlock> {
    let mut native = NativeBlock::new();
    for index in 0..block.column_count() {
        let column = block.column(index)?;
        match (&column.data, column.type_name.as_ref()) {
            (ColumnData::Fixed(_), "UInt8") => {
                native.push_u8(&column.name, column.slice::<u8>()?)?;
            }
            (ColumnData::Fixed(_), "UInt16") => {
                native.push_u16(&column.name, column.slice::<u16>()?)?;
            }
            (ColumnData::Fixed(_), "UInt32") => {
                native.push_u32(&column.name, column.slice::<u32>()?)?;
            }
            (ColumnData::Fixed(_), "UInt64") => {
                native.push_u64(&column.name, column.slice::<u64>()?)?;
            }
            (ColumnData::Fixed(_), "Int8") => {
                native.push_i8(&column.name, column.slice::<i8>()?)?;
            }
            (ColumnData::Fixed(_), "Int16") => {
                native.push_i16(&column.name, column.slice::<i16>()?)?;
            }
            (ColumnData::Fixed(_), "Int32") => {
                native.push_i32(&column.name, column.slice::<i32>()?)?;
            }
            (ColumnData::Fixed(_), "Int64") => {
                native.push_i64(&column.name, column.slice::<i64>()?)?;
            }
            (ColumnData::Fixed(_), "Float32") => {
                native.push_f32(&column.name, column.slice::<f32>()?)?;
            }
            (ColumnData::Fixed(_), "Float64") => {
                native.push_f64(&column.name, column.slice::<f64>()?)?;
            }
            (ColumnData::Strings { offsets, bytes }, "String") => {
                native.push_string_flat(&column.name, offsets, bytes)?;
            }
            _ => return Err(unsupported(column)),
        }
    }
    Ok(native)
}

fn from_native(native: &NativeBlock) -> Result<Block> {
    let mut block = Block::with_capacity(native.columns(), native.rows());
    for index in 0..native.columns() {
        let name = native.column_name(index);
        match native.column_kind(index) {
            1 => {
                let values = native.column_slice::<u8>(&name)?;
                block.append_fixed_slice::<u8>(name, "UInt8", values)?;
            }
            2 => {
                let values = native.column_slice::<u16>(&name)?;
                block.append_fixed_slice::<u16>(name, "UInt16", values)?;
            }
            3 => {
                let values = native.column_slice::<u32>(&name)?;
                block.append_fixed_slice::<u32>(name, "UInt32", values)?;
            }
            4 => {
                let values = native.column_slice::<u64>(&name)?;
                block.append_fixed_slice::<u64>(name, "UInt64", values)?;
            }
            5 => {
                let values = native.column_slice::<i8>(&name)?;
                block.append_fixed_slice::<i8>(name, "Int8", values)?;
            }
            6 => {
                let values = native.column_slice::<i16>(&name)?;
                block.append_fixed_slice::<i16>(name, "Int16", values)?;
            }
            7 => {
                let values = native.column_slice::<i32>(&name)?;
                block.append_fixed_slice::<i32>(name, "Int32", values)?;
            }
            8 => {
                let values = native.column_slice::<i64>(&name)?;
                block.append_fixed_slice::<i64>(name, "Int64", values)?;
            }
            9 => {
                let values = native.column_slice::<f32>(&name)?;
                block.append_fixed_slice::<f32>(name, "Float32", values)?;
            }
            10 => {
                let values = native.column_slice::<f64>(&name)?;
                block.append_fixed_slice::<f64>(name, "Float64", values)?;
            }
            11 => {
                let offsets = native.column_string_offsets(index);
                let bytes = native.column_string_flat(index);
                block.append_strings_flat(name, &offsets, &bytes)?;
            }
            kind => {
                return Err(Error::SchemaMismatch {
                    table: None,
                    reason: Cow::Owned(format!(
                        "native column kind {kind} is not supported by the row bridge"
                    )),
                });
            }
        }
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
