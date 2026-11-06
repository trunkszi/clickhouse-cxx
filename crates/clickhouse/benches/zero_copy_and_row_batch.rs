//! SoA append, checked projections, borrowed rows, and failover selection.
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use clickhouse::{EndpointSelection, FailoverPolicy, Row, failover::EndpointHealth, row::OwnedRow};
use criterion::{Criterion, criterion_group, criterion_main};

mod support;
use support::assert_zero;

#[derive(Row)]
struct Event {
    id: u64,
    text: String,
    values: Vec<u32>,
}

const ROWS: usize = 4096;

fn fill(batch: &mut EventBatch) {
    for id in 0..ROWS {
        batch.push_ref(EventRef {
            id: id as u64,
            text: "payload",
            values: &[1, 2, 3],
        });
    }
}

fn preflight() {
    let mut batch = EventBatch::with_capacity(64);
    for id in 0..64u64 {
        batch.push_ref(EventRef {
            id,
            text: "payload",
            values: &[1, 2, 3],
        });
    }
    batch.clear_unbounded();
    assert_zero("reused row batch", || {
        for id in 0..64u64 {
            batch.push_ref(EventRef {
                id,
                text: "payload",
                values: &[1, 2, 3],
            });
        }
        batch.clear_unbounded();
    });
    fill(&mut batch);
    let block = batch.to_block().expect("row batch becomes a block");
    let view = EventBlockView::try_from_block(&block).expect("checked row view");
    assert_zero("zero-copy column slice", || {
        black_box(block.column_slice::<u64>(0).expect("id column"));
    });
    assert_zero("borrowed row view", || {
        for row in view.iter() {
            black_box(row);
        }
    });
}

fn rows(c: &mut Criterion) {
    preflight();
    let mut batch = EventBatch::with_capacity(ROWS);
    fill(&mut batch);
    let block = batch.to_block().expect("row batch becomes a block");
    let view = EventBlockView::try_from_block(&block).expect("checked row view");
    c.bench_function("rows/soa_append", |b| {
        b.iter(|| {
            batch.clear();
            for row in view.iter() {
                batch.push_ref(black_box(row));
            }
            black_box(batch.len());
        });
    });
    c.bench_function("rows/zero_copy_slice", |b| {
        b.iter(|| black_box(block.column_slice::<u64>(0).expect("id column")));
    });
    c.bench_function("rows/checked_view", |b| {
        b.iter(|| black_box(EventBlockView::try_from_block(&block).expect("checked row view")));
    });
    c.bench_function("rows/borrowed_rows", |b| {
        b.iter(|| {
            for row in view.iter() {
                black_box(row);
            }
        });
    });
    c.bench_function("rows/owned_rows", |b| {
        b.iter(|| {
            let rows: Vec<_> = view.iter().map(Event::from_ref).collect();
            black_box(rows);
        });
    });
}

const STRATEGIES: [(EndpointSelection, bool); 4] = [
    (EndpointSelection::Sticky, false),
    (EndpointSelection::RoundRobin, true),
    (EndpointSelection::Random, false),
    (EndpointSelection::LatencyAware, false),
];

fn sample_health() -> Vec<EndpointHealth> {
    let later = Instant::now() + Duration::from_secs(30);
    let spec = [
        (true, Some(Duration::from_micros(40)), false, None),
        (true, Some(Duration::from_micros(10)), false, None),
        (false, None, true, Some(later)),
        (true, Some(Duration::from_micros(25)), false, None),
    ];
    spec.into_iter()
        .enumerate()
        .map(
            |(index, (healthy, latency, circuit_open, retry_at))| EndpointHealth {
                index,
                healthy,
                failures: u32::from(circuit_open),
                latency,
                circuit_open,
                retry_at,
                last_error: None,
            },
        )
        .collect()
}

fn selection_preflight() {
    let health = sample_health();
    let now = Instant::now();
    assert_zero("endpoint phase", || {
        for endpoint in &health {
            black_box(endpoint.phase(now));
        }
    });
    for (selection, rotate) in STRATEGIES {
        let policy = FailoverPolicy::new().with_selection(selection);
        assert_zero("failover select_index", || {
            let mut state = 11u64;
            black_box(policy.select_index(&health, 0, now, rotate, &mut state));
        });
    }
}

fn failover(c: &mut Criterion) {
    selection_preflight();
    let health = sample_health();
    let now = Instant::now();
    c.bench_function("failover/endpoint_phase", |b| {
        b.iter(|| {
            for endpoint in &health {
                black_box(endpoint.phase(now));
            }
        });
    });
    c.bench_function("failover/select_index", |b| {
        b.iter(|| {
            let mut state = 11u64;
            for (selection, rotate) in STRATEGIES {
                let policy = FailoverPolicy::new().with_selection(selection);
                black_box(policy.select_index(&health, 0, now, rotate, &mut state));
            }
        });
    });
}

fn short_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(1))
}

criterion_group!(row_group, rows);
criterion_group! {
    name = failover_group;
    config = short_criterion();
    targets = failover
}
criterion_main!(row_group, failover_group);
