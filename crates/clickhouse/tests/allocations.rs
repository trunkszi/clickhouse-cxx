//! Integration coverage for allocation-free projections and warmed batch appends.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use clickhouse::{Batch, Block, Row, SerdeBridge, row::RowClient};

#[derive(Debug, Default, Clone, Copy, PartialEq, Row, serde::Serialize, serde::Deserialize)]
struct FixedRecord {
    id: u64,
    sequence: u32,
    price: f64,
}

/// A transport whose stored blocks are moved out, so `query_blocks` itself
/// does not allocate during a measured call.
#[derive(Default)]
struct SingleBlockClient {
    blocks: Vec<Block>,
}

impl RowClient for SingleBlockClient {
    fn insert_block(&mut self, _table: &str, _block: &Block) -> clickhouse::Result<()> {
        Ok(())
    }

    fn query_blocks(&mut self, _sql: &str) -> clickhouse::Result<Vec<Block>> {
        Ok(std::mem::take(&mut self.blocks))
    }
}
struct Counter;
thread_local! {
    // Count allocations only on the test thread that enabled measurement.
    static ENABLED: Cell<bool> = const { Cell::new(false) };
}
fn enabled() -> bool {
    ENABLED.try_with(Cell::get).unwrap_or(false)
}
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
// Allocation-sensitive tests share one counting switch, so they must not run concurrently.
static ALLOCATION_TEST_LOCK: Mutex<()> = Mutex::new(());
#[global_allocator]
static ALLOCATOR: Counter = Counter;
// SAFETY: Every allocation and deallocation is delegated unchanged to System.
unsafe impl GlobalAlloc for Counter {
    /// # Safety
    /// The caller supplies a valid allocation layout.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if enabled() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: Forward the caller's unchanged valid layout.
        unsafe { System.alloc(layout) }
    }
    /// # Safety
    /// The pointer and layout identify an allocation returned by this allocator.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Forward the same allocation identity to its owning allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
    /// # Safety
    /// The pointer/layout identify a live allocation and the size is valid.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if enabled() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: The caller's live allocation and new size pass through unchanged.
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[derive(Row)]
pub struct Record {
    id: u64,
    text: String,
    values: Vec<u32>,
    optional: Option<String>,
}
/// Projection, row iteration, and refilling warmed buffers allocate nothing.
#[test]
fn warmed_borrowed_paths_do_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut batch = RecordBatch::with_capacity(4);
    batch.push_ref(RecordRef {
        id: 7,
        text: "sample",
        values: &[1, 2, 3],
        optional: Some("optional"),
    });
    let block = batch.to_block().unwrap();
    batch.clear();
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let view = RecordBlockView::try_from_block(&block).unwrap();
    let mut sum = 0;
    for row in view.iter() {
        sum += row.id;
        batch.push_ref(row);
    }
    ENABLED.with(|flag| flag.set(false));
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(sum, 7);
    assert_eq!(batch.len(), 1);
    assert_eq!(
        view.column_text().bytes().as_ptr(),
        view.get(0).unwrap().text.as_ptr()
    );
}

/// Burst capacity is trimmed once; later steady-state refills allocate nothing.
#[test]
fn integration_burst_trim_and_warm_refill() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut batch = RecordBatch::default();
    let value = RecordRef {
        id: 1,
        text: "payload",
        values: &[1, 2, 3],
        optional: Some("optional"),
    };
    for _ in 0..262_144 {
        batch.push_ref(value);
    }
    let peak = batch.allocated_bytes();
    batch.clear();
    let warm = batch.allocated_bytes();
    assert!(warm < peak);
    assert!(warm <= clickhouse::BufferPolicy::default().max_retained_bytes);
    assert!(batch.id.capacity() >= 4096);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    batch.clear();
    for _ in 0..4096 {
        batch.push_ref(value);
    }
    ENABLED.with(|flag| flag.set(false));
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(batch.allocated_bytes(), warm);
    assert_eq!(batch.len(), 4096);
    batch.shrink_to_fit();
    assert_eq!(batch.len(), 4096);
    assert!(batch.allocated_bytes() < warm);
    batch.clear_with_policy(clickhouse::BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
}

/// Recursive blocks reuse each nested Vec after a bounded clear.
#[test]
fn integration_block_refill_reuses_capacity() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut batch = RecordBatch::default();
    for _ in 0..4096 {
        batch.push_ref(RecordRef {
            id: 8,
            text: "text",
            values: &[4, 5],
            optional: Some("yes"),
        });
    }
    let source = batch.to_block().unwrap();
    let mut target = batch.to_block().unwrap();
    let bytes = target.allocated_bytes();
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    target.clear();
    target.refill_from(&source).unwrap();
    ENABLED.with(|flag| flag.set(false));
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(target.allocated_bytes(), bytes);
    assert_eq!(
        RecordBlockView::try_from_block(&target)
            .unwrap()
            .get(4095)
            .unwrap()
            .id,
        8
    );
}

fn fixed_records(count: usize) -> Vec<FixedRecord> {
    (0..count)
        .map(|index| FixedRecord {
            id: index as u64,
            sequence: index as u32,
            price: index as f64 * 0.5,
        })
        .collect()
}

/// The RowBinary decode/write path allocates nothing once the encoded bytes
/// and the destination slice already exist. Encoding the rows and building the
/// destination happen before measurement, so this covers only the conversion
/// and overwrite of caller memory.
#[test]
fn serde_rowbinary_into_slice_does_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let records = fixed_records(64);
    let (_, bytes) = SerdeBridge::to_rowbinary(&records).unwrap();
    let mut dst = vec![FixedRecord::default(); records.len()];
    // Warm the decode path before measuring.
    assert_eq!(
        SerdeBridge::from_rowbinary_into_slice(&bytes, &mut dst).unwrap(),
        records.len()
    );

    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let count = SerdeBridge::from_rowbinary_into_slice(&bytes, &mut dst).unwrap();
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(count, records.len());
    assert_eq!(dst, records);
}

/// Block row decoding into an existing Vec capacity and into a preallocated
/// slice allocates nothing for fixed-width rows. Block construction happens
/// before measurement.
#[test]
fn serde_block_rows_into_callers_allocate_nothing() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let records = fixed_records(64);
    let block = SerdeBridge::rows_to_block(&records).unwrap();
    let mut slice = vec![FixedRecord::default(); records.len()];
    let mut appended: Vec<FixedRecord> = Vec::with_capacity(records.len());
    // Warm both paths before measuring.
    SerdeBridge::block_to_rows_into_slice(&block, &mut slice).unwrap();
    SerdeBridge::block_to_rows_into(&block, &mut appended).unwrap();
    appended.clear();

    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let slice_count = SerdeBridge::block_to_rows_into_slice(&block, &mut slice).unwrap();
    let append_count = SerdeBridge::block_to_rows_into(&block, &mut appended).unwrap();
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(slice_count, records.len());
    assert_eq!(append_count, records.len());
    assert_eq!(slice, records);
    assert_eq!(appended, records);
}

fn fixed_block(records: &[FixedRecord]) -> Block {
    let mut batch = FixedRecordBatch::with_capacity(records.len());
    batch.extend_from_slice(records);
    batch.to_block().unwrap()
}

/// `query_rows_into_slice` reads the transport blocks and overwrites a
/// preallocated slice; block construction and the first (warming) call happen
/// before measurement.
#[test]
fn row_client_query_rows_into_slice_does_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let records = fixed_records(64);
    let mut client = SingleBlockClient::default();
    let mut dst = vec![FixedRecord::default(); records.len()];

    client.blocks = vec![fixed_block(&records)];
    assert_eq!(
        client.query_rows_into_slice("SELECT", &mut dst).unwrap(),
        records.len()
    );

    client.blocks = vec![fixed_block(&records)];
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let count = client.query_rows_into_slice("SELECT", &mut dst).unwrap();
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(count, records.len());
    assert_eq!(dst, records);
}

/// `query_batch_into` clears and refills a warmed batch. The measured call
/// reuses the batch capacity for fixed-width rows.
#[test]
fn row_client_query_batch_into_does_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let records = fixed_records(64);
    let mut client = SingleBlockClient::default();
    let mut batch = FixedRecordBatch::with_capacity(records.len());

    client.blocks = vec![fixed_block(&records)];
    assert_eq!(
        client
            .query_batch_into::<FixedRecord>("SELECT", &mut batch)
            .unwrap(),
        records.len()
    );

    client.blocks = vec![fixed_block(&records)];
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let count = client
        .query_batch_into::<FixedRecord>("SELECT", &mut batch)
        .unwrap();
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(count, records.len());
    assert_eq!(batch.len(), records.len());
}

/// `query_block_into` moves a validated block into an empty destination.
#[test]
fn row_client_query_block_into_does_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let records = fixed_records(64);
    let mut client = SingleBlockClient::default();
    let mut dst = Block::new();

    client.blocks = vec![fixed_block(&records)];
    assert_eq!(
        client
            .query_block_into::<FixedRecord>("SELECT", &mut dst)
            .unwrap(),
        records.len()
    );

    dst = Block::new();
    client.blocks = vec![fixed_block(&records)];
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let count = client
        .query_block_into::<FixedRecord>("SELECT", &mut dst)
        .unwrap();
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(count, records.len());
    assert_eq!(dst.row_count(), records.len());
}

/// Copying a prebuilt fixed-width column into a preallocated slice allocates
/// nothing; the block and the destination are built before measurement.
#[test]
fn column_read_into_slice_does_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let records = fixed_records(64);
    let block = fixed_block(&records);
    let column = block.column(0).unwrap();
    let mut dst = vec![0_u64; records.len()];

    assert_eq!(column.read_into_slice(&mut dst).unwrap(), records.len());

    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let count = column.read_into_slice(&mut dst).unwrap();
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(count, records.len());
    assert_eq!(dst, records.iter().map(|row| row.id).collect::<Vec<_>>());
}

/// Native fixed-width reads reuse the caller's slice without allocating a name.
#[cfg(feature = "tcp")]
#[test]
fn native_slice_read_does_not_allocate() {
    let _guard = ALLOCATION_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let values = [7_u64, 11, 19];
    let mut block = clickhouse::NativeBlock::new();
    block.push_u64("id", &values).unwrap();
    let mut dst = [0_u64; 3];

    ALLOCATIONS.store(0, Ordering::Relaxed);
    ENABLED.with(|flag| flag.set(true));
    let result = block.read_into_slice(0, &mut dst);
    ENABLED.with(|flag| flag.set(false));

    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(result.unwrap(), values.len());
    assert_eq!(dst, values);
}
