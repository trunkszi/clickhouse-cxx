//! Rust recursive columns and supported numeric/String native conversions.
use std::{collections::BTreeMap, hint::black_box};

use clickhouse::{BufferPolicy, Row};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
#[derive(Row)]
struct Composite {
    id: u64,
    text: String,
    optional: Option<f64>,
    values: Vec<u32>,
    map: BTreeMap<String, u64>,
}
#[derive(Row)]
struct Primitive {
    id: u64,
    text: String,
}
fn columns(c: &mut Criterion) {
    let source = Composite {
        id: 9,
        text: "sample".into(),
        optional: Some(1.5),
        values: vec![1, 2, 3],
        map: BTreeMap::from([("key".into(), 7)]),
    };
    let mut batch = CompositeBatch::with_capacity(4096);
    for _ in 0..4096 {
        batch.push(&source);
    }
    let mut primitive = PrimitiveBatch::with_capacity(4096);
    for id in 0..4096 {
        primitive.push_ref(PrimitiveRef { id, text: "sample" });
    }
    let block = primitive.to_block().unwrap();
    let native = block.to_native().unwrap();
    let restored = clickhouse::Block::from_native(&native).unwrap();
    assert_eq!(
        restored.column_slice::<u64>(0).unwrap(),
        block.column_slice::<u64>(0).unwrap()
    );
    let mut group = c.benchmark_group("columns");
    group.throughput(Throughput::Elements(4096));
    group.bench_function("recursive_append_reuse", |b| {
        b.iter(|| {
            batch.clear();
            for _ in 0..4096 {
                batch.push(black_box(&source));
            }
            black_box(batch.allocated_bytes());
        })
    });
    group.bench_function("recursive_block_append", |b| {
        b.iter(|| black_box(batch.to_block().unwrap()))
    });
    let incoming = batch.to_block().unwrap();
    let mut reusable = batch.to_block().unwrap();
    group.bench_function("recursive_clear_refill", |b| {
        b.iter(|| {
            reusable.clear_with_policy(BufferPolicy::default());
            reusable.refill_from(black_box(&incoming)).unwrap();
            black_box(reusable.allocated_bytes());
        })
    });
    group.bench_function("native_roundtrip", |b| {
        b.iter(|| {
            let native = black_box(&block).to_native().unwrap();
            black_box(clickhouse::Block::from_native(&native).unwrap());
        })
    });
    group.finish();
}
criterion_group!(benches, columns);
criterion_main!(benches);
