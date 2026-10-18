//! Integration coverage for allocation-free projections and warmed batch appends.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use clickhouse::Row;
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
        view.text.bytes().as_ptr(),
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
