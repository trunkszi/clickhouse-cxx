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
