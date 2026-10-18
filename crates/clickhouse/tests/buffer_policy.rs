//! Integration and scenario coverage for aggregate recursive retention budgets.
use std::collections::BTreeMap;

use clickhouse::{
    BufferPolicy, MemoryBuffer, Row,
    row::{Bytes, Field},
};

#[derive(Row)]
struct Nested {
    id: u64,
    text: String,
    values: Vec<u32>,
    optional: Option<String>,
    map: BTreeMap<String, Vec<u64>>,
}
fn filled(rows: usize) -> NestedBatch {
    let row = Nested {
        id: 7,
        text: "a long string value".into(),
        values: vec![1, 2, 3],
        optional: Some("present".into()),
        map: BTreeMap::from([("key".into(), vec![8, 9])]),
    };
    let mut batch = NestedBatch::default();
    for _ in 0..rows {
        batch.push(&row);
    }
    batch
}
/// All nested field Vecs share the same byte budget rather than one per field.
#[test]
fn integration_nested_byte_budget() {
    let mut batch = filled(8192);
    let expected = batch.id.capacity() * 8
        + batch.text.allocated_bytes()
        + batch.values.allocated_bytes()
        + batch.optional.allocated_bytes()
        + batch.map.allocated_bytes();
    assert_eq!(batch.allocated_bytes(), expected);
    let peak = batch.allocated_bytes();
    let policy = BufferPolicy {
        max_retained_rows: usize::MAX,
        max_retained_bytes: 16 * 1024,
    };
    batch.clear_with_policy(policy);
    assert!(batch.allocated_bytes() <= policy.max_retained_bytes);
    assert!(batch.allocated_bytes() > 0);
    assert!(batch.allocated_bytes() < peak);
    assert_eq!(batch.len(), 0);
    let bytes = batch.allocated_bytes();
    batch.clear_with_policy(policy);
    assert_eq!(batch.allocated_bytes(), bytes);
    batch.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
}
/// Blocks account for the column Vec as well as every nested value buffer.
#[test]
fn integration_block_budget_and_refill() {
    let batch = filled(8192);
    let source = batch.to_block().unwrap();
    let mut block = batch.to_block().unwrap();
    let peak = block.allocated_bytes();
    let policy = BufferPolicy {
        max_retained_rows: 512,
        max_retained_bytes: 32 * 1024,
    };
    block.clear_with_policy(policy);
    assert!(block.allocated_bytes() <= policy.max_retained_bytes);
    assert!(block.allocated_bytes() > 0);
    assert!(block.allocated_bytes() < peak);
    assert_eq!(block.row_count(), 0);
    block.refill_from(&source).unwrap();
    let view = NestedBlockView::try_from_block(&block).unwrap();
    let last = view.get(8191).unwrap();
    assert_eq!(last.map.iter().next().unwrap(), ("key", &[8, 9][..]));
    assert_eq!(last.optional, Some("present"));
    block.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(block.allocated_bytes(), 0);
}
/// Unbounded clearing preserves peaks; shrinking live buffers preserves values.
#[test]
fn scenario_unbounded_and_shrink() {
    let mut batch = filled(17);
    let peak = batch.allocated_bytes();
    batch.shrink_to_fit();
    assert!(batch.allocated_bytes() <= peak);
    assert_eq!(batch.len(), 17);
    let block = batch.to_block().unwrap();
    assert_eq!(
        NestedBlockView::try_from_block(&block)
            .unwrap()
            .get(16)
            .unwrap()
            .id,
        7
    );
    let before = batch.allocated_bytes();
    batch.clear_unbounded();
    assert_eq!(batch.allocated_bytes(), before);
    batch.clear_with_policy(BufferPolicy {
        max_retained_rows: 4,
        max_retained_bytes: usize::MAX,
    });
    assert!(batch.id.capacity() <= 4);
    assert!(batch.text.offsets.capacity() <= 4);
}
/// Binary and fixed-width field buffers expose the same retention policy.
#[test]
fn integration_binary_and_fixed_buffers() {
    let mut binary = <Bytes as Field>::Buffer::default();
    for _ in 0..4096 {
        Bytes::push(&mut binary, &[0, 255, 10]);
    }
    binary.clear_with_policy(BufferPolicy {
        max_retained_rows: 128,
        max_retained_bytes: 1024,
    });
    assert!(binary.allocated_bytes() <= 1024);
    binary.clear_unbounded();
    binary.shrink_to_fit();
    assert_eq!(binary.allocated_bytes(), 0);
    let mut fixed = vec![9u64; 131_072];
    <u64 as Field>::clear(&mut fixed);
    assert!(fixed.capacity() <= 65_536);
    assert!(fixed.allocated_bytes() > 0);
}
/// Schema mismatch rejects a refill before existing rows can be changed.
#[test]
fn scenario_refill_schema_rejection() {
    let mut block = filled(2).to_block().unwrap();
    let mut other = clickhouse::Block::new();
    other
        .append_fixed_slice("different", "UInt64", &[5u64])
        .unwrap();
    assert!(block.refill_from(&other).is_err());
    assert_eq!(block.row_count(), 2);
    assert_eq!(block.column_slice::<u64>(0).unwrap(), &[7, 7]);
}
