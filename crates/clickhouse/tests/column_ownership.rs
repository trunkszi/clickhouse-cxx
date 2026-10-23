#![cfg(feature = "tcp")]
//! Borrowed Native projections remain valid across independent column mutations.

use clickhouse::{NativeBlock, WireColumn};

#[test]
fn integration_native_attachment_preserves_a_live_projection() {
    let mut source = WireColumn::fixed(&[7_u64]).unwrap();
    let mut block = NativeBlock::new();
    block.push_wire_column("id", &source).unwrap();
    let values = block.column_slice::<u64>("id").unwrap();
    let address = values.as_ptr();
    if let WireColumn::Fixed(fixed) = &mut source {
        fixed.append_slice(&vec![9_u64; 1_000_000]).unwrap();
    }
    assert_eq!(block.column_slice::<u64>("id").unwrap().as_ptr(), address);
    assert_eq!(block.rows(), 1);
    assert_eq!(values, &[7]);
}

#[test]
fn integration_shared_native_blocks_detach_before_clear() {
    let source = WireColumn::fixed(&[7_u64]).unwrap();
    let mut first = NativeBlock::new();
    let mut second = NativeBlock::new();
    first.push_wire_column("id", &source).unwrap();
    second.push_wire_column("id", &source).unwrap();
    let values = first.column_slice::<u64>("id").unwrap();
    let address = values.as_ptr();
    second.clear();
    assert_eq!(first.column_slice::<u64>("id").unwrap().as_ptr(), address);
    assert_eq!(values, &[7]);
    assert_eq!(second.rows(), 0);
}

#[test]
fn integration_native_attachment_and_reads_are_zero_copy_until_mutation() {
    let source = WireColumn::fixed(&[7_u64, 9]).unwrap();
    let mut block = NativeBlock::new();
    block.push_wire_column("id", &source).unwrap();
    if let WireColumn::Fixed(fixed) = &source {
        assert_eq!(
            fixed.as_slice::<u64>().unwrap().as_ptr(),
            block.column_slice::<u64>("id").unwrap().as_ptr()
        );
    }
    let first = block.column_slice::<u64>("id").unwrap();
    let second = block.column_slice::<u64>("id").unwrap();
    assert_eq!(first.as_ptr(), second.as_ptr());
    assert_eq!(first, &[7, 9]);
}
