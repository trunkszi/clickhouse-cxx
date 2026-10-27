//! Mixed steady-state and burst workloads with bounded and unbounded reuse.
use std::hint::black_box;

use clickhouse::{BufferPolicy, Row};
use criterion::{Criterion, criterion_group, criterion_main};

mod support;
use support::assert_zero;

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

fn preflight() {
    let mut batch = EventBatch::with_capacity(32);
    for id in 0..32u64 {
        batch.push_ref(EventRef {
            id,
            payload: "sixteen-byte-msg",
        });
    }
    batch.clear_unbounded();
    assert_zero("reused steady batch", || {
        for id in 0..32u64 {
            batch.push_ref(EventRef {
                id,
                payload: "sixteen-byte-msg",
            });
        }
        batch.clear_unbounded();
    });
}

fn memory(c: &mut Criterion) {
    preflight();
    c.bench_function("memory/cold", |b| {
        b.iter(|| {
            for rows in std::iter::once(BURST).chain(std::iter::repeat_n(STEADY, 8)) {
                let mut batch = EventBatch::default();
                refill(&mut batch, rows);
                black_box(batch);
            }
        });
    });
    c.bench_function("memory/bounded", |b| {
        let mut batch = EventBatch::default();
        let policy = BufferPolicy::default();
        b.iter(|| {
            for rows in std::iter::once(BURST).chain(std::iter::repeat_n(STEADY, 8)) {
                refill(&mut batch, rows);
                batch.clear_with_policy(policy);
                assert!(batch.allocated_bytes() <= policy.max_retained_bytes);
                assert!(batch.id.capacity() >= STEADY);
                black_box(batch.allocated_bytes());
            }
        });
    });
    c.bench_function("memory/unbounded", |b| {
        let mut batch = EventBatch::default();
        b.iter(|| {
            for rows in std::iter::once(BURST).chain(std::iter::repeat_n(STEADY, 8)) {
                refill(&mut batch, rows);
                batch.clear_with_policy(BufferPolicy::unbounded());
                black_box(batch.allocated_bytes());
            }
        });
    });
}

criterion_group!(memory_group, memory);
criterion_main!(memory_group);
