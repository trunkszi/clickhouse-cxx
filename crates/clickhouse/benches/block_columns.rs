//! Column reuse, Native header codec, fixed slices, and circuit phases.
use std::{
    collections::BTreeMap,
    hint::black_box,
    time::{Duration, Instant},
};

use clickhouse::{
    BufferPolicy, HTTP_NATIVE_REVISION, Row, decode_blocks, encode_block,
    failover::{CircuitPhase, CircuitSuperstate, EndpointHealth},
};
use criterion::{Criterion, criterion_group, criterion_main};

mod support;
use support::assert_zero;

#[derive(Row)]
struct Composite {
    id: u64,
    text: String,
    optional: Option<f64>,
    values: Vec<u32>,
    map: BTreeMap<String, u64>,
}

#[derive(Row)]
struct HeaderTick {
    id: u64,
}

#[derive(Row)]
struct ShortLabel {
    text: String,
}

#[cfg(feature = "tcp")]
#[derive(Row)]
struct Primitive {
    id: u64,
    text: String,
}

fn header_block() -> clickhouse::Block {
    let mut batch = HeaderTickBatch::with_capacity(300);
    for id in 0..300u64 {
        batch.push_ref(HeaderTickRef { id });
    }
    batch.to_block().expect("header block")
}

fn label_block() -> clickhouse::Block {
    let mut batch = ShortLabelBatch::with_capacity(2048);
    for _ in 0..2048 {
        batch.push_ref(ShortLabelRef { text: "row" });
    }
    batch
        .to_block()
        .expect("short labels become a native block")
}

fn preflight() {
    let now = Instant::now();
    let mut closed = EndpointHealth {
        index: 0,
        healthy: true,
        failures: 0,
        latency: Some(Duration::from_micros(5)),
        circuit_open: false,
        retry_at: Some(now + Duration::from_secs(5)),
        last_error: None,
    };
    assert_eq!(closed.phase(now), CircuitPhase::ClosedHealthy);
    assert_eq!(closed.phase(now).superstate(), CircuitSuperstate::Eligible);
    closed.healthy = false;
    closed.failures = 2;
    assert_eq!(closed.phase(now), CircuitPhase::ClosedDegraded);
    closed.circuit_open = true;
    closed.retry_at = Some(now + Duration::from_secs(5));
    assert_eq!(closed.phase(now), CircuitPhase::OpenCooldown);
    assert!(!closed.phase(now).is_eligible());
    closed.retry_at = Some(now);
    assert_eq!(closed.phase(now), CircuitPhase::HalfOpenProbe);

    let block = header_block();
    let mut encoded = Vec::new();
    encode_block(&block, HTTP_NATIVE_REVISION, &mut encoded).expect("encode header");
    encoded.clear();
    encoded.reserve(4096);
    assert_zero("native header encode", || {
        encoded.clear();
        encode_block(&block, HTTP_NATIVE_REVISION, &mut encoded).expect("encode header");
        black_box(encoded.len());
    });

    let labels = label_block();
    let mut label_bytes = Vec::new();
    encode_block(&labels, HTTP_NATIVE_REVISION, &mut label_bytes).expect("encode labels");
    label_bytes.clear();
    label_bytes.reserve(16_384);
    assert_zero("string label encode", || {
        label_bytes.clear();
        encode_block(&labels, HTTP_NATIVE_REVISION, &mut label_bytes).expect("encode labels");
        black_box(label_bytes.len());
    });
}

fn columns(c: &mut Criterion) {
    preflight();
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
    c.bench_function("columns/recursive_append_reuse", |b| {
        b.iter(|| {
            batch.clear();
            for _ in 0..4096 {
                batch.push(black_box(&source));
            }
            black_box(batch.allocated_bytes());
        });
    });
    c.bench_function("columns/recursive_block_append", |b| {
        b.iter(|| black_box(batch.to_block().expect("composite block")));
    });
    let incoming = batch.to_block().expect("composite block");
    let mut reusable = batch.to_block().expect("composite block");
    c.bench_function("columns/recursive_clear_refill", |b| {
        b.iter(|| {
            reusable.clear_with_policy(BufferPolicy::default());
            reusable
                .refill_from(black_box(&incoming))
                .expect("refill composite block");
            black_box(reusable.allocated_bytes());
        });
    });

    let block = header_block();
    let mut encoded = Vec::with_capacity(8192);
    encode_block(&block, HTTP_NATIVE_REVISION, &mut encoded).expect("encode header");
    let frame = encoded.clone();
    c.bench_function("varint_leb128_header_encode", |b| {
        b.iter(|| {
            encoded.clear();
            encode_block(&block, HTTP_NATIVE_REVISION, &mut encoded).expect("encode header");
            black_box(encoded.len());
        });
    });
    c.bench_function("varint_leb128_header_decode", |b| {
        b.iter(|| {
            black_box(
                decode_blocks(black_box(&frame), HTTP_NATIVE_REVISION).expect("decode header"),
            )
        });
    });

    let labels = label_block();
    let mut label_bytes = Vec::with_capacity(16_384);
    encode_block(&labels, HTTP_NATIVE_REVISION, &mut label_bytes).expect("encode labels");
    let label_frame = label_bytes.clone();
    c.bench_function("varint_leb128_string_encode", |b| {
        b.iter(|| {
            label_bytes.clear();
            encode_block(&labels, HTTP_NATIVE_REVISION, &mut label_bytes).expect("encode labels");
            black_box(label_bytes.len());
        });
    });
    c.bench_function("varint_leb128_string_decode", |b| {
        b.iter(|| {
            black_box(
                decode_blocks(black_box(&label_frame), HTTP_NATIVE_REVISION)
                    .expect("decode labels"),
            )
        });
    });

    #[cfg(feature = "tcp")]
    {
        let mut primitive = PrimitiveBatch::with_capacity(4096);
        for id in 0..4096u64 {
            primitive.push_ref(PrimitiveRef { id, text: "sample" });
        }
        let block = primitive.to_block().expect("primitive block");
        let native = block.to_native().expect("native block");
        let restored = clickhouse::Block::from_native(&native).expect("restored block");
        assert_eq!(
            restored.column_slice::<u64>(0).expect("restored ids"),
            block.column_slice::<u64>(0).expect("source ids")
        );
        c.bench_function("columns/native_roundtrip", |b| {
            b.iter(|| {
                let native = black_box(&block).to_native().expect("native block");
                black_box(clickhouse::Block::from_native(&native).expect("restored block"));
            });
        });
        let values: Vec<u64> = (0..4096).collect();
        let column =
            clickhouse::FixedWireColumn::from_slice(values.as_slice()).expect("fixed column");
        assert_zero("fixed column slice", || {
            black_box(column.as_slice::<u64>().expect("fixed slice"));
        });
        c.bench_function("fixed_column_slice", |b| {
            b.iter(|| black_box(column.as_slice::<u64>().expect("fixed slice")));
        });
        c.bench_function("fixed_column_to_vec", |b| {
            b.iter(|| black_box(column.to_vec::<u64>().expect("fixed copy")));
        });
    }
}

criterion_group!(column_group, columns);
criterion_main!(column_group);
