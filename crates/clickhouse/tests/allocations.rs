//! Integration coverage for allocation-free projections and warmed batch appends.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use clickhouse::Row;
struct Counter;
static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
#[global_allocator]
static ALLOCATOR: Counter = Counter;
// SAFETY: Every allocation and deallocation is delegated unchanged to System.
unsafe impl GlobalAlloc for Counter {
    /// # Safety
    /// The caller supplies a valid allocation layout.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
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
        if ENABLED.load(Ordering::Relaxed) {
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
    ENABLED.store(true, Ordering::Relaxed);
    let view = RecordBlockView::try_from_block(&block).unwrap();
    let mut sum = 0;
    for row in view.iter() {
        sum += row.id;
        batch.push_ref(row);
    }
    ENABLED.store(false, Ordering::Relaxed);
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(sum, 7);
    assert_eq!(batch.len(), 1);
    assert_eq!(
        view.text.bytes().as_ptr(),
        view.get(0).unwrap().text.as_ptr()
    );
}
