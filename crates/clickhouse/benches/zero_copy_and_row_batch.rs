//! SoA append, checked projections, borrowed rows, and owned row materialization.
use std::hint::black_box;

use clickhouse::{Row, row::OwnedRow};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
#[derive(Row)]
struct Event {
    id: u64,
    text: String,
    values: Vec<u32>,
}
fn rows(c: &mut Criterion) {
    let mut batch = EventBatch::with_capacity(4096);
    for id in 0..4096 {
        batch.push_ref(EventRef {
            id,
            text: "payload",
            values: &[1, 2, 3],
        });
    }
    let block = batch.to_block().unwrap();
    let view = EventBlockView::try_from_block(&block).unwrap();
    assert_eq!(
        view.column_id().as_ptr(),
        block.column_slice::<u64>(0).unwrap().as_ptr()
    );
    let mut group = c.benchmark_group("rows");
    group.throughput(Throughput::Elements(4096));
    group.bench_function("soa_append", |b| {
        b.iter(|| {
            batch.clear();
            for row in view.iter() {
                batch.push_ref(black_box(row));
            }
            black_box(batch.len());
        })
    });
    group.bench_function("zero_copy_slice", |b| {
        b.iter(|| black_box(block.column_slice::<u64>(0).unwrap()))
    });
    group.bench_function("checked_view", |b| {
        b.iter(|| black_box(EventBlockView::try_from_block(&block).unwrap()))
    });
    group.bench_function("borrowed_rows", |b| {
        b.iter(|| {
            for row in view.iter() {
                black_box(row);
            }
        })
    });
    group.bench_function("owned_rows", |b| {
        b.iter(|| {
            let rows: Vec<_> = view.iter().map(Event::from_ref).collect();
            black_box(rows);
        })
    });
    group.finish();
}
criterion_group!(benches, rows);
criterion_main!(benches);
