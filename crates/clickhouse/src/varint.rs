//! Unsigned LEB128 helpers shared by Native and RowBinary.

use crate::error::Error;

/// Appends an unsigned LEB128 integer.
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

/// Appends a length-prefixed byte string.
pub(crate) fn write_string(out: &mut Vec<u8>, value: &str) {
    write_varu64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

/// Reads an unsigned LEB128 integer from a slice cursor.
pub(crate) fn read_varu64_slice(input: &mut &[u8]) -> Result<u64, Error> {
    read_varu64(|| {
        let (byte, rest) = input
            .split_first()
            .ok_or_else(|| Error::protocol("truncated varuint"))?;
        *input = rest;
        Ok(*byte)
    })
}

fn read_varu64(mut next: impl FnMut() -> Result<u8, Error>) -> Result<u64, Error> {
    let mut value = 0_u64;
    let mut shift = 0;
    loop {
        if shift >= 64 {
            return Err(Error::protocol("varuint overflows u64"));
        }
        let byte = next()?;
        let piece = u64::from(byte & 0x7f);
        if shift == 63 && piece > 1 {
            return Err(Error::protocol("varuint overflows u64"));
        }
        value |= piece << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}
