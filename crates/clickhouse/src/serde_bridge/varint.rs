//! Unsigned LEB128 lengths used by ClickHouse RowBinary.

use super::SerError;

/// Append an unsigned LEB128 integer.
pub(crate) fn write_varu64(out: &mut Vec<u8>, value: u64) {
    crate::varint::write_varu64(out, value);
}

/// Read an unsigned LEB128 integer from a slice cursor.
pub(crate) fn read_varu64(input: &mut &[u8]) -> Result<u64, SerError> {
    crate::varint::read_varu64_slice(input).map_err(SerError::custom)
}

/// Read a length prefix and convert it to a platform length.
pub(crate) fn read_len(input: &mut &[u8]) -> Result<usize, SerError> {
    let len = read_varu64(input)?;
    usize::try_from(len).map_err(|_| SerError::custom("length overflows usize"))
}
