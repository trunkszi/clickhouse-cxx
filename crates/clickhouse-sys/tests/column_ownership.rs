//! Integration ownership scenarios for Native columns and borrowed storage.
use clickhouse_sys::{ColumnHandle, ffi, new_block, new_column};
use cxx::UniquePtr;

fn fixed() -> UniquePtr<ColumnHandle> {
    let mut column = new_column("UInt64").unwrap();
    column.pin_mut().append_fixed(&7_u64.to_ne_bytes()).unwrap();
    column
}

#[test]
fn integration_extracted_matrix_column_survives_both_owners_mutating() {
    let mut source = fixed();
    let mut block = new_block();
    block.pin_mut().append_column("id", &source).unwrap();
    let extracted = block.column(0).unwrap();
    let alias = extracted.clone();
    let bytes = extracted.fixed_bytes();
    let address = bytes.as_ptr();
    source.pin_mut().reserve(1_000_000);
    assert_eq!(extracted.fixed_bytes().as_ptr(), address);
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    block.pin_mut().reserve(1_000_000);
    block.pin_mut().clear();
    assert_eq!(alias.fixed_bytes().as_ptr(), address);
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    assert_eq!(block.row_count(), 0);
    assert_eq!(source.fixed_bytes(), 7_u64.to_ne_bytes());
}

#[test]
fn integration_extracted_native_column_survives_native_append_and_clear() {
    let source = fixed();
    let mut block = ffi::block_new();
    ffi::block_append_column_handle(block.pin_mut(), "id", &source).unwrap();
    let extracted = ffi::block_column_handle(&block, 0);
    let bytes = extracted.fixed_bytes();
    let address = bytes.as_ptr();
    ffi::block_append_u64(block.pin_mut(), "id", &[9]).unwrap();
    assert_eq!(extracted.fixed_bytes().as_ptr(), address);
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    assert_eq!(ffi::block_column_bytes(&block, 0).len(), 16);
    ffi::block_clear(block.pin_mut());
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    assert_eq!(source.fixed_bytes(), 7_u64.to_ne_bytes());
}

#[test]
fn integration_nullable_child_survives_parent_default_and_clear() {
    let mut parent = new_column("Nullable(UInt64)").unwrap();
    parent
        .pin_mut()
        .nullable_append_fixed(&7_u64.to_ne_bytes())
        .unwrap();
    parent.pin_mut().append_nullable_flag(false).unwrap();
    let child = parent.nested();
    let bytes = child.fixed_bytes();
    let address = bytes.as_ptr();
    parent.pin_mut().append_default().unwrap();
    assert_eq!(child.fixed_bytes().as_ptr(), address);
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    assert_eq!(parent.nulls(), &[0, 1]);
    assert_eq!(parent.nested().size(), 2);
    parent.pin_mut().clear();
    assert_eq!(bytes, 7_u64.to_ne_bytes());
}

#[test]
fn integration_recursive_leaf_survives_parent_reserve_after_intermediate_drop() {
    let mut row = new_column("Tuple(UInt64, String)").unwrap();
    row.pin_mut()
        .tuple_append_fixed(0, &7_u64.to_ne_bytes())
        .unwrap();
    row.pin_mut().tuple_append_string(1, b"value").unwrap();
    let mut array = new_column("Array(Tuple(UInt64, String))").unwrap();
    array.pin_mut().array_append_row(&row).unwrap();
    let tuple = array.nested();
    let leaf = tuple.tuple_at(0);
    drop(tuple);
    let bytes = leaf.fixed_bytes();
    let address = bytes.as_ptr();
    array.pin_mut().reserve(1_000_000);
    assert_eq!(leaf.fixed_bytes().as_ptr(), address);
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    array.pin_mut().clear();
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    assert_eq!(array.size(), 0);
}

#[test]
fn integration_cached_projection_stays_stable_across_reads_and_alias_mutation() {
    for (name, value) in [
        ("Bool", vec![1]),
        ("Enum8('x'=1)", vec![1]),
        ("Decimal32(2)", 7_i32.to_ne_bytes().to_vec()),
        ("DateTime64(3)", 7_i64.to_ne_bytes().to_vec()),
        ("FixedString(8)", b"abcdefgh".to_vec()),
        ("UUID", vec![7; 16]),
        ("IPv6", vec![7; 16]),
    ] {
        let mut source = new_column(name).unwrap();
        source.pin_mut().append_fixed(&value).unwrap();
        let mut block = new_block();
        block.pin_mut().append_column("value", &source).unwrap();
        let extracted = block.column(0).unwrap();
        let bytes = extracted.fixed_bytes();
        let expected = bytes.to_vec();
        let address = bytes.as_ptr();
        assert_eq!(extracted.item_bytes(0), expected);
        assert_eq!(extracted.fixed_bytes().as_ptr(), address);
        source.pin_mut().append_fixed(&value).unwrap();
        block.pin_mut().clear();
        assert_eq!(extracted.fixed_bytes().as_ptr(), address);
        assert_eq!(bytes, expected);
    }
}

#[test]
fn integration_fixed_append_accepts_unaligned_bytes() {
    let mut column = new_column("UInt64").unwrap();
    let mut input = [0_u8; 24];
    let offset = (0..8)
        .find(|offset| !(input.as_ptr() as usize + offset).is_multiple_of(8))
        .unwrap();
    input[offset..offset + 8].copy_from_slice(&7_u64.to_ne_bytes());
    column
        .pin_mut()
        .append_fixed(&input[offset..offset + 8])
        .unwrap();
    assert_eq!(column.fixed_bytes(), 7_u64.to_ne_bytes());
}

#[test]
fn integration_partially_built_nullable_retains_child_values_when_detached() {
    let mut parent = new_column("Nullable(UInt64)").unwrap();
    parent
        .pin_mut()
        .nullable_append_fixed(&7_u64.to_ne_bytes())
        .unwrap();
    let child = parent.nested();
    let bytes = child.fixed_bytes();
    parent.pin_mut().append_nullable_flag(false).unwrap();
    assert_eq!(parent.nulls(), &[0]);
    assert_eq!(parent.nested().fixed_bytes(), 7_u64.to_ne_bytes());
    assert_eq!(bytes, 7_u64.to_ne_bytes());
}

#[test]
fn integration_invalid_tuple_children_are_rejected_without_invalidating_a_borrow() {
    let mut parent = new_column("Tuple(UInt64)").unwrap();
    parent
        .pin_mut()
        .tuple_append_fixed(0, &7_u64.to_ne_bytes())
        .unwrap();
    let child = parent.tuple_at(0);
    let bytes = child.fixed_bytes();
    assert!(parent.tuple_at(usize::MAX).is_null());
    assert!(
        parent
            .pin_mut()
            .tuple_append_fixed(usize::MAX, &9_u64.to_ne_bytes())
            .is_err()
    );
    assert_eq!(bytes, 7_u64.to_ne_bytes());
    assert_eq!(parent.tuple_at(0).fixed_bytes(), 7_u64.to_ne_bytes());
}

#[test]
fn integration_partially_built_tuple_retains_other_children_when_detached() {
    let mut parent = new_column("Tuple(UInt64, String)").unwrap();
    parent
        .pin_mut()
        .tuple_append_string(1, b"retained")
        .unwrap();
    let child = parent.tuple_at(1);
    let bytes = child.string_at(0);
    parent
        .pin_mut()
        .tuple_append_fixed(0, &7_u64.to_ne_bytes())
        .unwrap();
    assert_eq!(parent.size(), 1);
    assert_eq!(parent.tuple_at(1).string_at(0), b"retained");
    assert_eq!(bytes, b"retained");
}

#[test]
fn integration_nullable_dictionary_values_survive_detachment() {
    let mut value = new_column("Nullable(Int64)").unwrap();
    value
        .pin_mut()
        .nullable_append_fixed(&0_i64.to_ne_bytes())
        .unwrap();
    value.pin_mut().append_nullable_flag(false).unwrap();
    let mut source = new_column("LowCardinality(Nullable(Int64))").unwrap();
    source.pin_mut().append_default().unwrap();
    source.pin_mut().append_column(&value).unwrap();
    assert_eq!(source.item_bytes(1), 0_i64.to_ne_bytes());
    let mut block = new_block();
    block.pin_mut().append_column("value", &source).unwrap();
    let frozen = block.column(0).unwrap();
    let bytes = frozen.item_bytes(1);
    source.pin_mut().append_default().unwrap();
    assert!(source.item_bytes(0).is_empty());
    assert_eq!(source.item_bytes(1), 0_i64.to_ne_bytes());
    assert!(source.item_bytes(2).is_empty());
    assert_eq!(bytes, 0_i64.to_ne_bytes());
    assert_eq!(frozen.size(), 2);
}
