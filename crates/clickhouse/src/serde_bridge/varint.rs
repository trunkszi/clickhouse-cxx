//! Unsigned LEB128 lengths used by ClickHouse RowBinary.

use super::SerError;

/// Append an unsigned LEB128 integer.
pub(crate) fn write_varu64(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Read an unsigned LEB128 integer from a slice cursor.
pub(crate) fn read_varu64(input: &mut &[u8]) -> Result<u64, SerError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        if shift >= 64 {
            return Err(SerError::custom("length prefix overflows u64"));
        }
        let (byte, rest) = input
            .split_first()
            .ok_or_else(|| SerError::custom("truncated length prefix"))?;
        *input = rest;
        let piece = u64::from(byte & 0x7f);
        if shift == 63 && piece > 1 {
            return Err(SerError::custom("length prefix overflows u64"));
        }
        value |= piece << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

/// Read a length prefix and convert it to a platform length.
pub(crate) fn read_len(input: &mut &[u8]) -> Result<usize, SerError> {
    let len = read_varu64(input)?;
    usize::try_from(len).map_err(|_| SerError::custom("length overflows usize"))
}
