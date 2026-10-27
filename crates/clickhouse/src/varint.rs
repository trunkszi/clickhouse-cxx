//! Unsigned LEB128 helpers shared by Native and RowBinary.

use crate::error::Error;

/// Appends an unsigned LEB128 integer.
///
/// Values below `0x80` are a single byte. Wider values use a stack buffer so
/// the destination grows at most once.
#[inline]
pub(crate) fn write_varu64(out: &mut Vec<u8>, value: u64) {
    if value < 0x80 {
        out.push(value as u8);
        return;
    }
    write_varu64_wide(out, value);
}

/// Two or more LEB128 bytes. A `u64` needs at most ten bytes, and the tenth
/// byte holds only the final payload bit, so it never continues.
#[inline]
fn write_varu64_wide(out: &mut Vec<u8>, mut value: u64) {
    let mut buf = [0_u8; 10];
    let mut len = 0_usize;
    macro_rules! step {
        () => {
            let byte = (value as u8) & 0x7f;
            value >>= 7;
            if value == 0 {
                buf[len] = byte;
                len += 1;
                out.extend_from_slice(&buf[..len]);
                return;
            }
            buf[len] = byte | 0x80;
            len += 1;
        };
    }
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    buf[len] = (value as u8) & 0x7f;
    len += 1;
    out.extend_from_slice(&buf[..len]);
}

/// Appends a length-prefixed byte string.
pub(crate) fn write_string(out: &mut Vec<u8>, value: &str) {
    write_varu64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

/// Reads an unsigned LEB128 integer from a slice cursor.
#[inline]
pub(crate) fn read_varu64_slice(input: &mut &[u8]) -> Result<u64, Error> {
    let data = *input;
    let Some((&first, rest)) = data.split_first() else {
        return Err(Error::protocol("truncated varuint"));
    };
    if first < 0x80 {
        *input = rest;
        return Ok(u64::from(first));
    }
    read_varu64_wide(input, first)
}

/// Continuation encodings. Bytes two through nine are ordinary 7-bit groups.
/// The tenth byte is the leftover bit of a `u64`: a payload above 1, or
/// another continuation, does not fit.
fn read_varu64_wide(input: &mut &[u8], first: u8) -> Result<u64, Error> {
    let data = *input;
    let mut rest = &data[1..];
    let mut value = u64::from(first & 0x7f);
    let mut shift = 7_u32;
    macro_rules! step {
        () => {
            let Some((&byte, tail)) = rest.split_first() else {
                *input = rest;
                return Err(Error::protocol("truncated varuint"));
            };
            rest = tail;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                *input = rest;
                return Ok(value);
            }
            shift += 7;
        };
    }
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    step!();
    let Some((&byte, tail)) = rest.split_first() else {
        *input = rest;
        return Err(Error::protocol("truncated varuint"));
    };
    let piece = u64::from(byte & 0x7f);
    if piece > 1 || byte & 0x80 != 0 {
        *input = tail;
        return Err(Error::protocol("varuint overflows u64"));
    }
    value |= piece << shift;
    *input = tail;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{read_varu64_slice, write_varu64};
    use crate::error::Error;

    fn decode(bytes: &[u8]) -> Result<u64, Error> {
        read_varu64_slice(&mut &bytes[..])
    }

    #[test]
    fn writes_known_encodings() {
        let mut out = Vec::new();
        write_varu64(&mut out, 0);
        assert_eq!(out, [0]);

        out.clear();
        write_varu64(&mut out, 300);
        assert_eq!(out, [0xac, 0x02]);
    }

    #[test]
    fn round_trips_u64_max() {
        let mut out = Vec::new();
        write_varu64(&mut out, u64::MAX);
        assert_eq!(out.len(), 10);
        assert_eq!(decode(&out).expect("u64::MAX round-trip"), u64::MAX);
    }

    #[test]
    fn rejects_truncated_input() {
        assert!(decode(&[0x80]).is_err());
    }

    #[test]
    fn rejects_overlong_encoding() {
        let mut bytes = vec![0x80; 10];
        bytes.push(0x00);
        assert!(decode(&bytes).is_err());
    }
}
