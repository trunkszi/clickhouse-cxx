//! FNV-1a 64-bit batch fingerprints.

use crate::{
    block::Block,
    error::Error,
    native_format::{HTTP_NATIVE_REVISION, encode_block},
};

const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64-bit hash.
#[must_use]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Fingerprint of the HTTP Native (revision 0) encoding.
///
/// # Errors
///
/// Returns an error when `block` cannot be encoded.
pub fn fingerprint_block(block: &Block) -> Result<u64, Error> {
    let mut encoded = Vec::new();
    encode_block(block, HTTP_NATIVE_REVISION, &mut encoded)?;
    Ok(fnv1a64(&encoded))
}

/// Lower-hex form of [`fingerprint_block`].
///
/// # Errors
///
/// Returns an error when `block` cannot be encoded.
pub fn deduplication_token(block: &Block) -> Result<String, Error> {
    Ok(format!("{:016x}", fingerprint_block(block)?))
}
