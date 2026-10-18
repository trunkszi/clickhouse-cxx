//! Checked HTTP Native and RowBinaryWithNamesAndTypes codecs for shared columns.
use crate::{
    Block, Column, Error, Result,
    column::{AlignedBytes, ColumnData, validate_offsets},
    type_layout::{Layout, layout},
    varint::{read_varu64_slice, write_string, write_varu64},
};

/// Native format revision used by the HTTP interface.
pub const HTTP_NATIVE_REVISION: u64 = 0;
const MAX_ROWS: usize = 16 * 1024 * 1024;
fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    let bytes = input
        .get(..len)
        .ok_or_else(|| Error::protocol("truncated column payload"))?;
    *input = &input[len..];
    Ok(bytes)
}
fn count(input: &mut &[u8]) -> Result<usize> {
    usize::try_from(read_varu64_slice(input)?).map_err(|_| Error::protocol("length exceeds usize"))
}
fn string(input: &mut &[u8]) -> Result<String> {
    let len = count(input)?;
    let bytes = take(input, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::protocol("invalid UTF-8 header"))
}
fn offsets(input: &mut &[u8], rows: usize) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    let bytes = take(
        input,
        rows.checked_mul(8)
            .ok_or_else(|| Error::protocol("offset size overflow"))?,
    )?;
    for b in bytes.chunks_exact(8) {
        out.push(u64::from_le_bytes(
            b.try_into()
                .map_err(|_| Error::protocol("invalid offset width"))?,
        ));
    }
    if out.windows(2).any(|w| w[0] > w[1]) {
        return Err(Error::protocol("nonmonotonic offsets"));
    }
    Ok(out)
}
/// Decode a column body directly into shared recursive storage.
/// # Errors
/// Unsupported types, invalid offsets, oversized rows, or truncated bytes fail closed.
pub(crate) fn decode_column(
    name: &str,
    ty: &str,
    rows: usize,
    input: &mut &[u8],
) -> Result<Column> {
    if rows > MAX_ROWS {
        return Err(Error::protocol("column exceeds row limit"));
    }
    let data = match layout(ty)? {
        Layout::Fixed(width) => ColumnData::Fixed(AlignedBytes::from_bytes(take(
            input,
            rows.checked_mul(width)
                .ok_or_else(|| Error::protocol("fixed length overflow"))?,
        )?)),
        Layout::String => {
            let mut offsets = Vec::with_capacity(rows);
            let mut bytes = Vec::new();
            for _ in 0..rows {
                let n = count(input)?;
                bytes.extend_from_slice(take(input, n)?);
                offsets.push(bytes.len() as u64);
            }
            ColumnData::Strings { offsets, bytes }
        }
        Layout::Nullable(inner) => {
            let null_map = take(input, rows)?.to_vec();
            if null_map.iter().any(|x| *x > 1) {
                return Err(Error::protocol("invalid null map"));
            }
            let values = Box::new(decode_column("values", inner, rows, input)?);
            ColumnData::Nullable { null_map, values }
        }
        Layout::Array(inner) => {
            let offsets = offsets(input, rows)?;
            let len = usize::try_from(offsets.last().copied().unwrap_or(0))
                .map_err(|_| Error::protocol("array length overflow"))?;
            let values = Box::new(decode_column("values", inner, len, input)?);
            ColumnData::Array { offsets, values }
        }
        Layout::Map(key, value) => {
            let offsets = offsets(input, rows)?;
            let len = usize::try_from(offsets.last().copied().unwrap_or(0))
                .map_err(|_| Error::protocol("map length overflow"))?;
            let keys = Box::new(decode_column("keys", key, len, input)?);
            let values = Box::new(decode_column("values", value, len, input)?);
            ColumnData::Map {
                offsets,
                keys,
                values,
            }
        }
    };
    Ok(Column {
        name: name.to_owned().into(),
        type_name: ty.to_owned().into(),
        rows,
        data,
    })
}
fn encode_column(column: &Column, out: &mut Vec<u8>) -> Result<()> {
    match (&column.data, layout(&column.type_name)?) {
        (ColumnData::Fixed(b), Layout::Fixed(w)) => {
            if b.as_bytes().len()
                != column
                    .len()
                    .checked_mul(w)
                    .ok_or_else(|| Error::protocol("column size overflow"))?
            {
                return Err(Error::protocol("invalid fixed column length"));
            }
            out.extend_from_slice(b.as_bytes());
        }
        (ColumnData::Strings { offsets, bytes }, Layout::String) => {
            validate_offsets(offsets, bytes.len())?;
            if offsets.len() != column.len() {
                return Err(Error::protocol("invalid string cardinality"));
            }
            let mut start = 0;
            for end in offsets {
                let end = *end as usize;
                write_varu64(out, (end - start) as u64);
                out.extend_from_slice(&bytes[start..end]);
                start = end;
            }
        }
        (ColumnData::Nullable { null_map, values }, Layout::Nullable(_)) => {
            if null_map.len() != column.len()
                || values.len() != column.len()
                || null_map.iter().any(|x| *x > 1)
            {
                return Err(Error::protocol("invalid nullable cardinality"));
            }
            out.extend_from_slice(null_map);
            encode_column(values, out)?;
        }
        (ColumnData::Array { offsets, values }, Layout::Array(_)) => {
            validate_offsets(offsets, values.len())?;
            if offsets.len() != column.len() {
                return Err(Error::protocol("invalid array cardinality"));
            }
            for offset in offsets {
                out.extend_from_slice(&offset.to_le_bytes());
            }
            encode_column(values, out)?;
        }
        (
            ColumnData::Map {
                offsets,
                keys,
                values,
            },
            Layout::Map(_, _),
        ) => {
            validate_offsets(offsets, keys.len())?;
            if keys.len() != values.len() || offsets.len() != column.len() {
                return Err(Error::protocol("invalid map cardinality"));
            }
            for offset in offsets {
                out.extend_from_slice(&offset.to_le_bytes());
            }
            encode_column(keys, out)?;
            encode_column(values, out)?;
        }
        _ => return Err(Error::unsupported("column storage and type mismatch")),
    }
    Ok(())
}
/// Encode a Native block with revision-appropriate header fields.
/// # Errors
/// Malformed column storage and unsupported column types reject encoding.
pub fn encode_block(block: &Block, revision: u64, out: &mut Vec<u8>) -> Result<()> {
    if revision >= 51903 {
        write_varu64(out, 1);
        out.push(0);
        write_varu64(out, 2);
        out.extend_from_slice(&(-1_i32).to_le_bytes());
        write_varu64(out, 0);
    }
    write_varu64(out, block.column_count() as u64);
    write_varu64(out, block.row_count() as u64);
    for i in 0..block.column_count() {
        let c = block.column(i)?;
        write_string(out, &c.name);
        write_string(out, &c.type_name);
        if revision >= 54454 {
            out.push(0);
        }
        if block.row_count() > 0 {
            encode_column(c, out)?;
        }
    }
    Ok(())
}
/// Decode consecutive Native blocks at the specified protocol revision.
/// # Errors
/// Invalid headers, unknown types, or truncated payloads reject the response.
pub fn decode_blocks(mut input: &[u8], revision: u64) -> Result<Vec<Block>> {
    let mut blocks = Vec::new();
    while !input.is_empty() {
        if revision >= 51903 {
            if read_varu64_slice(&mut input)? != 1
                || take(&mut input, 1)?[0] > 1
                || read_varu64_slice(&mut input)? != 2
            {
                return Err(Error::protocol("invalid Native block info"));
            }
            take(&mut input, 4)?;
            if read_varu64_slice(&mut input)? != 0 {
                return Err(Error::protocol("invalid Native block info terminator"));
            }
        }
        let cols = count(&mut input)?;
        let rows = count(&mut input)?;
        if cols > 65536 || rows > MAX_ROWS || (cols == 0 && rows != 0) {
            return Err(Error::protocol("block dimensions exceed limits"));
        }
        let mut block = Block::new();
        for _ in 0..cols {
            let name = string(&mut input)?;
            let ty = string(&mut input)?;
            if revision >= 54454 && take(&mut input, 1)?[0] != 0 {
                return Err(Error::unsupported("custom Native serialization"));
            }
            block.append_column(decode_column(&name, &ty, rows, &mut input)?)?;
        }
        blocks.push(block);
    }
    Ok(blocks)
}
/// Encode shared columns as RowBinaryWithNamesAndTypes.
/// # Errors
/// Unknown types, invalid storage, or row bounds reject encoding.
pub fn encode_rowbinary(block: &Block) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_varu64(&mut out, block.column_count() as u64);
    for i in 0..block.column_count() {
        write_string(&mut out, &block.column(i)?.name);
    }
    for i in 0..block.column_count() {
        write_string(&mut out, &block.column(i)?.type_name);
    }
    for row in 0..block.row_count() {
        for i in 0..block.column_count() {
            encode_value(block.column(i)?, row, &mut out)?;
        }
    }
    Ok(out)
}
fn encode_value(c: &Column, row: usize, out: &mut Vec<u8>) -> Result<()> {
    if row >= c.len() {
        return Err(Error::protocol("row out of bounds"));
    }
    match (&c.data, layout(&c.type_name)?) {
        (ColumnData::Fixed(b), Layout::Fixed(w)) => {
            let start = row
                .checked_mul(w)
                .ok_or_else(|| Error::protocol("row offset overflow"))?;
            out.extend_from_slice(
                b.as_bytes()
                    .get(start..start + w)
                    .ok_or_else(|| Error::protocol("truncated fixed data"))?,
            );
        }
        (ColumnData::Strings { offsets, bytes }, Layout::String) => {
            validate_offsets(offsets, bytes.len())?;
            let end = *offsets
                .get(row)
                .ok_or_else(|| Error::protocol("missing string offset"))?
                as usize;
            let start = if row == 0 {
                0
            } else {
                offsets[row - 1] as usize
            };
            write_varu64(out, (end - start) as u64);
            out.extend_from_slice(&bytes[start..end]);
        }
        (ColumnData::Nullable { null_map, values }, Layout::Nullable(_)) => {
            let flag = *null_map
                .get(row)
                .ok_or_else(|| Error::protocol("missing null flag"))?;
            if flag > 1 {
                return Err(Error::protocol("invalid null flag"));
            }
            out.push(flag);
            if flag == 0 {
                encode_value(values, row, out)?;
            }
        }
        (ColumnData::Array { offsets, values }, Layout::Array(_)) => {
            validate_offsets(offsets, values.len())?;
            let end = *offsets
                .get(row)
                .ok_or_else(|| Error::protocol("missing array offset"))?
                as usize;
            let start = if row == 0 {
                0
            } else {
                offsets[row - 1] as usize
            };
            write_varu64(out, (end - start) as u64);
            for i in start..end {
                encode_value(values, i, out)?;
            }
        }
        (
            ColumnData::Map {
                offsets,
                keys,
                values,
            },
            Layout::Map(_, _),
        ) => {
            validate_offsets(offsets, keys.len())?;
            if keys.len() != values.len() {
                return Err(Error::protocol("invalid map cardinality"));
            }
            let end = *offsets
                .get(row)
                .ok_or_else(|| Error::protocol("missing map offset"))?
                as usize;
            let start = if row == 0 {
                0
            } else {
                offsets[row - 1] as usize
            };
            write_varu64(out, (end - start) as u64);
            for i in start..end {
                encode_value(keys, i, out)?;
                encode_value(values, i, out)?;
            }
        }
        _ => return Err(Error::unsupported("unsupported RowBinary storage")),
    }
    Ok(())
}
/// Decode RowBinaryWithNamesAndTypes into shared recursive columns.
/// # Errors
/// Missing headers, malformed types, or truncated rows reject decoding.
pub fn decode_rowbinary(mut input: &[u8]) -> Result<Block> {
    let cols = count(&mut input)?;
    if cols == 0 || cols > 65536 {
        return Err(Error::protocol("invalid RowBinary column count"));
    }
    let mut names = Vec::with_capacity(cols);
    for _ in 0..cols {
        names.push(string(&mut input)?);
    }
    let mut columns = Vec::with_capacity(cols);
    for name in &names {
        let ty = string(&mut input)?;
        columns.push(crate::serde_bridge::column_buffer::ColumnBuffer::new(
            name.clone(),
            ty,
        )?);
    }
    while !input.is_empty() {
        let before = input.len();
        for c in &mut columns {
            c.push(&mut input)?;
        }
        if input.len() >= before {
            return Err(Error::protocol("RowBinary decoder made no progress"));
        }
    }
    let mut block = Block::new();
    for c in columns {
        block.append_column(c.finish())?;
    }
    Ok(block)
}
