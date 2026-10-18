//! Mixed steady-state and burst workloads with bounded and unbounded reuse.
use std::hint::black_box;

use clickhouse::{BufferPolicy, Row};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[derive(Row)]
struct Event {
    id: u64,
    payload: String,
}
const STEADY: usize = 4096;
const BURST: usize = 262_144;
fn refill(batch: &mut EventBatch, rows: usize) {
    for id in 0..rows {
        batch.push_ref(EventRef {
            id: id as u64,
            payload: black_box("sixteen-byte-msg"),
        });
    }
}
fn memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory");
    // One burst followed by eight normal batches is one measured cycle.
    group.throughput(Throughput::Elements((BURST + 8 * STEADY) as u64));
    group.bench_function("cold", |b| {
        b.iter(|| {
            for rows in std::iter::once(BURST).chain(std::iter::repeat_n(STEADY, 8)) {
                let mut batch = EventBatch::default();
                refill(&mut batch, rows);
                black_box(batch);
            }
        })
    });
    for (name, policy) in [
        ("bounded", BufferPolicy::default()),
        ("unbounded", BufferPolicy::unbounded()),
    ] {
        let mut batch = EventBatch::default();
        group.bench_function(name, |b| {
            b.iter(|| {
                for rows in std::iter::once(BURST).chain(std::iter::repeat_n(STEADY, 8)) {
                    refill(&mut batch, rows);
                    batch.clear_with_policy(policy);
                    if name == "bounded" {
                        assert!(batch.allocated_bytes() <= policy.max_retained_bytes);
                        assert!(batch.id.capacity() >= STEADY);
                    }
                    black_box(batch.allocated_bytes());
                }
            })
        });
    }
    group.finish();
}
criterion_group!(benches, memory);
criterion_main!(benches);
