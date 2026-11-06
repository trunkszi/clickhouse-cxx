//! Byte writes keep allocation growth outside the scalar serialization leaves.

use uarch::prelude::*;

#[hot_inline]
pub(super) fn push<T>(out: &mut Vec<T>, value: T) {
    if out.len() == out.capacity() {
        grow(out, 1);
    }
    out.push(value);
}

#[hot_inline]
pub(super) fn extend(out: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.len() > out.capacity() - out.len() {
        grow(out, bytes.len());
    }
    out.extend_from_slice(bytes);
}

#[cold_sink]
fn grow<T>(out: &mut Vec<T>, additional: usize) {
    out.reserve(additional);
}
