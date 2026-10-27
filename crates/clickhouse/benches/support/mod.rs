//! Allocation counter for hot-path preflight checks.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

thread_local! {
    static COUNT_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOC_COUNT: Cell<u64> = const { Cell::new(0) };
}

/// Counts heap allocations while a preflight section is active.
pub struct CountingAllocator;

// SAFETY: every allocation is forwarded to the system allocator. The counter only
// updates thread-local cells and does not create or free pointers itself.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_alloc();
        // SAFETY: `layout` is a valid non-zero layout supplied by the language allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator with the same `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_alloc();
        // SAFETY: `layout` is a valid non-zero layout supplied by the language allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_alloc();
        // SAFETY: `ptr` came from this allocator with `layout`, and `new_size` is non-zero.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn note_alloc() {
    let Ok(active) = COUNT_ACTIVE.try_with(Cell::get) else {
        return;
    };
    if !active {
        return;
    }
    let Ok(()) = ALLOC_COUNT.try_with(|count| count.set(count.get().saturating_add(1))) else {
        return;
    };
}

fn prime_counter() {
    COUNT_ACTIVE.with(|_| {});
    ALLOC_COUNT.with(|_| {});
}

/// Number of allocations performed by `body` on this thread.
pub fn allocations_during(body: impl FnOnce()) -> u64 {
    prime_counter();
    ALLOC_COUNT.with(|count| count.set(0));
    COUNT_ACTIVE.with(|active| active.set(true));
    body();
    COUNT_ACTIVE.with(|active| active.set(false));
    ALLOC_COUNT.with(Cell::get)
}

/// Fail the bench when a reusable hot path allocates.
pub fn assert_zero(label: &str, body: impl FnOnce()) {
    let count = allocations_during(body);
    assert!(count == 0, "{label} allocated {count} times");
}
