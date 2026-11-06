//! Checked HTTP Native and RowBinaryWithNamesAndTypes codecs for shared columns.
use uarch::prelude::*;

use crate::{
    Block, Column, Error, Result,
    column::{
        AlignedBytes, ColumnData, monotonic_offsets, prefetch_offsets, valid_null_map,
        validate_child_type, validate_offsets,
    },
    type_layout::{Layout, layout},
    varint::{read_varu64_slice, write_string, write_varu64},
};

/// Native format revision used by the HTTP interface.
pub const HTTP_NATIVE_REVISION: u64 = 0;
const MAX_ROWS: usize = 16 * 1024 * 1024;
const MAX_DECODE_BYTES: usize = 256 * 1024 * 1024;

struct DecodeBudget(usize);
impl DecodeBudget {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.0 = self
            .0
            .checked_sub(bytes)
            .ok_or_else(|| Error::protocol("Native allocation budget exceeded"))?;
        Ok(())
    }
}
fn allocation_size(count: usize, width: usize) -> Result<usize> {
    count
        .checked_mul(width)
        .ok_or_else(|| Error::protocol("Native allocation size overflow"))
}
#[hot_inline]
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
/// One string row: payload length and the exact LEB128 width that produced it.
///
/// Width is the number of bytes consumed, not the canonical width of the value.
/// An overlong length would desynchronize the payload if it were recomputed.
#[hot_inline]
fn string_span(input: &mut &[u8]) -> Result<(usize, usize)> {
    let before = input.len();
    let data = *input;
    let Some((&first, rest)) = data.split_first() else {
        return Err(Error::protocol("truncated string lengths"));
    };
    let (len, width) = if first < 0x80 {
        *input = rest;
        (usize::from(first), 1)
    } else if let Some((&second, tail)) = rest.split_first()
        && second < 0x80
    {
        *input = tail;
        (usize::from(first & 0x7f) | (usize::from(second) << 7), 2)
    } else {
        let len = count(input)?;
        (len, before - input.len())
    };
    take(input, len)?;
    Ok((len, width))
}
fn decode_strings(rows: usize, input: &mut &[u8], budget: &mut DecodeBudget) -> Result<ColumnData> {
    if rows > input.len() {
        return Err(Error::protocol("truncated string lengths"));
    }
    // Retained cost is the offset vector plus payload bytes. Length prefixes
    // and the width scratch are not part of that column budget.
    budget.charge(allocation_size(rows, size_of::<u64>())?)?;
    let mut lengths = Vec::with_capacity(rows);
    let mut widths = Vec::with_capacity(rows);
    let mut scan = *input;
    let mut payload_len = 0usize;
    for _ in 0..rows {
        let (len, width) = string_span(&mut scan)?;
        payload_len = payload_len
            .checked_add(len)
            .ok_or_else(|| Error::protocol("string payload size overflow"))?;
        lengths.push(len as u64);
        widths.push(width as u8);
    }
    budget.charge(payload_len)?;
    let mut bytes = Vec::with_capacity(payload_len);
    let mut cursor = *input;
    let mut end = 0u64;
    for (slot, width) in lengths.iter_mut().zip(widths) {
        let len = *slot as usize;
        cursor = cursor
            .get(usize::from(width)..)
            .ok_or_else(|| Error::protocol("truncated string lengths"))?;
        let (payload, rest) = cursor
            .split_at_checked(len)
            .ok_or_else(|| Error::protocol("truncated column payload"))?;
        bytes.extend_from_slice(payload);
        cursor = rest;
        end = end
            .checked_add(*slot)
            .ok_or_else(|| Error::protocol("string payload size overflow"))?;
        *slot = end;
    }
    *input = cursor;
    Ok(ColumnData::Strings {
        offsets: lengths,
        bytes,
    })
}
fn string(input: &mut &[u8]) -> Result<String> {
    let len = count(input)?;
    let bytes = take(input, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| Error::protocol("invalid UTF-8 header"))
}
fn offsets(input: &mut &[u8], rows: usize, budget: &mut DecodeBudget) -> Result<Vec<u64>> {
    let bytes = take(
        input,
        rows.checked_mul(8)
            .ok_or_else(|| Error::protocol("offset size overflow"))?,
    )?;
    budget.charge(bytes.len())?;
    let mut out = Vec::with_capacity(rows);
    for b in bytes.as_chunks::<8>().0 {
        out.push(u64::from_le_bytes(*b));
    }
    if !monotonic_offsets(&out) {
        return Err(Error::protocol("nonmonotonic offsets"));
    }
    Ok(out)
}
/// Decode a column body directly into shared recursive storage.
/// # Errors
/// Unsupported types, invalid offsets, oversized rows, or truncated bytes fail closed.
fn decode_column(
    name: &str,
    ty: &str,
    rows: usize,
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> Result<Column> {
    if rows > MAX_ROWS {
        return Err(Error::protocol("column exceeds row limit"));
    }
    // Owning vectors may reserve up to four slots on their first insertion.
    budget.charge(allocation_size(4, size_of::<Column>())?)?;
    budget.charge(name.len())?;
    budget.charge(ty.len())?;
    let data = match layout(ty)? {
        Layout::Fixed(width) => {
            let bytes = take(input, allocation_size(rows, width)?)?;
            budget.charge(
                AlignedBytes::allocation_size(bytes.len())
                    .ok_or_else(|| Error::protocol("Native allocation size overflow"))?,
            )?;
            ColumnData::Fixed(AlignedBytes::from_bytes(bytes))
        }
        Layout::String => decode_strings(rows, input, budget)?,
        Layout::Nullable(inner) => {
            let flags = take(input, rows)?;
            budget.charge(rows)?;
            let null_map = flags.to_vec();
            if !valid_null_map(&null_map) {
                return Err(Error::protocol("invalid null map"));
            }
            let values = Box::new(decode_column("values", inner, rows, input, budget)?);
            ColumnData::Nullable { null_map, values }
        }
        Layout::Array(inner) => {
            let offsets = offsets(input, rows, budget)?;
            let len = usize::try_from(offsets.last().copied().unwrap_or(0))
                .map_err(|_| Error::protocol("array length overflow"))?;
            let values = Box::new(decode_column("values", inner, len, input, budget)?);
            ColumnData::Array { offsets, values }
        }
        Layout::Map(key, value) => {
            let offsets = offsets(input, rows, budget)?;
            let len = usize::try_from(offsets.last().copied().unwrap_or(0))
                .map_err(|_| Error::protocol("map length overflow"))?;
            let keys = Box::new(decode_column("keys", key, len, input, budget)?);
            let values = Box::new(decode_column("values", value, len, input, budget)?);
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
        (ColumnData::Nullable { null_map, values }, Layout::Nullable(inner)) => {
            if !ilp_all!(
                null_map.len() == column.len(),
                values.len() == column.len(),
                valid_null_map(null_map)
            ) {
                return Err(Error::protocol("invalid nullable cardinality"));
            }
            validate_child_type(inner, values)?;
            out.extend_from_slice(null_map);
            encode_column(values, out)?;
        }
        (ColumnData::Array { offsets, values }, Layout::Array(inner)) => {
            validate_offsets(offsets, values.len())?;
            if offsets.len() != column.len() {
                return Err(Error::protocol("invalid array cardinality"));
            }
            validate_child_type(inner, values)?;
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
            Layout::Map(key_type, value_type),
        ) => {
            validate_offsets(offsets, keys.len())?;
            if !ilp_all!(keys.len() == values.len(), offsets.len() == column.len()) {
                return Err(Error::protocol("invalid map cardinality"));
            }
            validate_child_type(key_type, keys)?;
            validate_child_type(value_type, values)?;
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
        encode_column(c, out)?;
    }
    Ok(())
}
/// Decode consecutive Native blocks at the specified protocol revision.
/// # Errors
/// Invalid headers, unknown types, truncated payloads, or decoded storage exceeding
/// the cumulative 256 MiB allocation budget reject the response.
pub fn decode_blocks(input: &[u8], revision: u64) -> Result<Vec<Block>> {
    decode_blocks_with_budget(input, revision, MAX_DECODE_BYTES)
}
fn decode_blocks_with_budget(mut input: &[u8], revision: u64, limit: usize) -> Result<Vec<Block>> {
    let mut budget = DecodeBudget(limit);
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
        if !ilp_all!(cols <= 65536, rows <= MAX_ROWS, cols != 0 || rows == 0) {
            return Err(Error::protocol("block dimensions exceed limits"));
        }
        budget.charge(allocation_size(4, size_of::<Block>())?)?;
        let mut block = Block::new();
        for _ in 0..cols {
            let name_len = count(&mut input)?;
            let name = std::str::from_utf8(take(&mut input, name_len)?)
                .map_err(|_| Error::protocol("invalid UTF-8 header"))?;
            let ty_len = count(&mut input)?;
            let ty = std::str::from_utf8(take(&mut input, ty_len)?)
                .map_err(|_| Error::protocol("invalid UTF-8 header"))?;
            if revision >= 54454 && take(&mut input, 1)?[0] != 0 {
                return Err(Error::unsupported("custom Native serialization"));
            }
            block.append_column(decode_column(name, ty, rows, &mut input, &mut budget)?)?;
        }
        blocks.push(block);
    }
    Ok(blocks)
}
/// Encode shared columns as RowBinaryWithNamesAndTypes.
/// # Errors
/// Unknown types, invalid storage, or row bounds reject encoding.
pub fn encode_rowbinary(block: &Block) -> Result<Vec<u8>> {
    if block.row_count() == 0 {
        for index in 0..block.column_count() {
            validate_schema(block.column(index)?)?;
        }
    }
    let mut out = Vec::new();
    write_varu64(&mut out, block.column_count() as u64);
    for i in 0..block.column_count() {
        write_string(&mut out, &block.column(i)?.name);
    }
    for i in 0..block.column_count() {
        write_string(&mut out, &block.column(i)?.type_name);
    }
    if block.row_count() > 0 {
        for index in 0..block.column_count() {
            let column = block.column(index)?;
            encode_value(column, 0, &mut out)?;
            validate_schema(column)?;
        }
    }
    for row in 1..block.row_count() {
        for i in 0..block.column_count() {
            encode_value(block.column(i)?, row, &mut out)?;
        }
    }
    Ok(out)
}

fn validate_schema(column: &Column) -> Result<()> {
    match (&column.data, layout(&column.type_name)?) {
        (ColumnData::Fixed(_), Layout::Fixed(_)) | (ColumnData::Strings { .. }, Layout::String) => {
            Ok(())
        }
        (ColumnData::Nullable { values, .. }, Layout::Nullable(inner))
        | (ColumnData::Array { values, .. }, Layout::Array(inner)) => {
            validate_child_type(inner, values)?;
            validate_schema(values)
        }
        (ColumnData::Map { keys, values, .. }, Layout::Map(key_type, value_type)) => {
            validate_child_type(key_type, keys)?;
            validate_child_type(value_type, values)?;
            validate_schema(keys)?;
            validate_schema(values)
        }
        _ => Err(Error::unsupported("unsupported RowBinary storage")),
    }
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
            let end = start
                .checked_add(w)
                .ok_or_else(|| Error::protocol("row end offset overflow"))?;
            out.extend_from_slice(
                b.as_bytes()
                    .get(start..end)
                    .ok_or_else(|| Error::protocol("truncated fixed data"))?,
            );
        }
        (ColumnData::Strings { offsets, bytes }, Layout::String) => {
            validate_offsets(offsets, bytes.len())?;
            prefetch_offsets(offsets, row);
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
            prefetch_offsets(offsets, row);
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
            prefetch_offsets(offsets, row);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn string_block(columns: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_varu64(&mut bytes, columns as u64);
        write_varu64(&mut bytes, 64);
        for index in 0..columns {
            write_string(&mut bytes, &index.to_string());
            write_string(&mut bytes, "String");
            bytes.extend_from_slice(&[0; 64]);
        }
        bytes
    }

    #[test]
    fn unit_scenario_native_budget_is_cumulative_across_columns_and_blocks() {
        let one = string_block(1);
        let limit = 4 * size_of::<Block>() + 4 * size_of::<Column>() + 7 + 64 * 8;
        assert!(decode_blocks_with_budget(&one, 0, limit).is_ok());
        let error = decode_blocks_with_budget(&string_block(2), 0, limit)
            .err()
            .unwrap();
        assert!(error.to_string().contains("allocation budget exceeded"));
        let mut two = one.clone();
        two.extend_from_slice(&one);
        let error = decode_blocks_with_budget(&two, 0, limit).err().unwrap();
        assert!(error.to_string().contains("allocation budget exceeded"));
    }

    #[test]
    fn unit_scenario_native_budget_includes_nested_storage() {
        let mut bytes = Vec::new();
        write_varu64(&mut bytes, 1);
        write_varu64(&mut bytes, 1);
        write_string(&mut bytes, "a");
        write_string(&mut bytes, "Array(String)");
        bytes.extend_from_slice(&64u64.to_le_bytes());
        bytes.extend_from_slice(&[0; 64]);
        let limit = 4 * size_of::<Block>() + 8 * size_of::<Column>() + 64;
        let error = decode_blocks_with_budget(&bytes, 0, limit).err().unwrap();
        assert!(error.to_string().contains("allocation budget exceeded"));
    }

    #[test]
    fn unit_scenario_rowbinary_fixed_end_overflow_is_rejected() {
        let column = Column {
            name: "c".into(),
            type_name: format!("FixedString({})", usize::MAX).into(),
            rows: 2,
            data: ColumnData::Fixed(AlignedBytes::from_bytes(&[])),
        };
        assert!(encode_value(&column, 1, &mut Vec::new()).is_err());
    }

    #[test]
    fn unit_scenario_native_budget_counts_payloads_and_fixed_alignment() {
        for (ty, payload, storage) in [
            ("UInt8", &[1u8][..], 64usize),
            ("FixedString(64)", &[0u8; 64][..], 64usize),
            ("FixedString(65)", &[0u8; 65][..], 128usize),
            ("String", &[4u8, b'd', b'a', b't', b'a'][..], 12usize),
        ] {
            let limit = 4 * size_of::<Column>() + 1 + ty.len() + storage;
            assert!(decode_column("c", ty, 1, &mut &payload[..], &mut DecodeBudget(limit)).is_ok());
            let error = decode_column("c", ty, 1, &mut &payload[..], &mut DecodeBudget(limit - 1))
                .err()
                .unwrap();
            assert!(error.to_string().contains("allocation budget exceeded"));
        }
    }

    #[test]
    fn unit_scenario_string_lengths_are_validated_before_offset_allocation() {
        let mut input = &[0u8][..];
        let error = decode_column("s", "String", MAX_ROWS, &mut input, &mut DecodeBudget(4096))
            .err()
            .unwrap();
        assert!(error.to_string().contains("truncated string lengths"));
        let mut input = &[3u8, 1][..];
        let error = decode_column("s", "String", 1, &mut input, &mut DecodeBudget(4096))
            .err()
            .unwrap();
        assert!(error.to_string().contains("truncated column payload"));
    }
}
