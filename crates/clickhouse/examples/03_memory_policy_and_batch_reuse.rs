//! Buffer reuse: keep steady-state ingestion allocation-free with `.clear()`,
//! then trim retained capacity when a burst is unlikely to repeat.
//!
//! Run with:
//!
//! ```text
//! cargo run -p clickhouse --example 03_memory_policy_and_batch_reuse
//! ```
//!
//! A derived `<Name>Batch` is a struct of arrays. `.clear()` resets every
//! column length while preserving each `Vec` allocation. `to_block()` copies
//! the current buffers into an independently owned block; the batch keeps its
//! capacity for the next cycle.

use clickhouse::Row;

#[derive(Row)]
struct Sample {
    ts: u64,
    value: f64,
}

/// A teaching policy for what to do with capacity after a buffer is cleared.
#[derive(Clone, Copy, Debug)]
enum BufferPolicy {
    /// Keep the allocation. This is the steady-state zero-allocation path.
    ReuseCapacity,
    /// Release the allocation. Use this after a one-off burst.
    TrimToFit,
}

/// Clear a buffer and apply the chosen capacity policy.
fn recycle(buffer: &mut Vec<u64>, policy: BufferPolicy) -> usize {
    buffer.clear();
    if matches!(policy, BufferPolicy::TrimToFit) {
        buffer.shrink_to_fit();
    }
    buffer.capacity()
}

fn main() {
    // ---------------------------------------------------------------------
    // 1. Warm the batch once with a known working set.
    //
    // `with_capacity` reserves each generated column buffer before the first
    // push, so the fills below do not grow the heap.
    // ---------------------------------------------------------------------
    let mut batch = SampleBatch::with_capacity(1_024);
    for index in 0..1_024_u64 {
        batch.push_ref(SampleRef {
            ts: index,
            value: index as f64,
        });
    }
    let warm_capacity = (batch.ts.capacity(), batch.value.capacity());
    println!("after warm-up");
    println!(
        "  rows={} ts_capacity={} value_capacity={}",
        batch.len(),
        warm_capacity.0,
        warm_capacity.1
    );

    // ---------------------------------------------------------------------
    // 2. Clear and refill a smaller batch.
    //
    // `.clear()` drops the row count to zero but deliberately keeps both
    // allocations. The refill fits in the existing capacity, so no allocation
    // occurs and the addresses do not change.
    // ---------------------------------------------------------------------
    let addresses_before = (batch.ts.as_ptr(), batch.value.as_ptr());
    batch.clear();
    assert_eq!(batch.len(), 0);
    for index in 0..64_u64 {
        batch.push_ref(SampleRef {
            ts: index,
            value: index as f64,
        });
    }
    let steady_capacity = (batch.ts.capacity(), batch.value.capacity());
    let addresses_after = (batch.ts.as_ptr(), batch.value.as_ptr());
    assert_eq!(steady_capacity, warm_capacity);
    assert_eq!(addresses_after, addresses_before);
    println!("\nafter clear + steady refill");
    println!(
        "  rows={} ts_capacity={} value_capacity={}",
        batch.len(),
        steady_capacity.0,
        steady_capacity.1
    );
    println!("  allocations were retained; buffer addresses are unchanged");

    // ---------------------------------------------------------------------
    // 3. A burst grows the buffers; a one-off burst should not pin that memory forever. After
    //    clearing, `shrink_to_fit` releases the excess.
    // ---------------------------------------------------------------------
    batch.clear();
    for index in 0..65_536_u64 {
        batch.push_ref(SampleRef {
            ts: index,
            value: index as f64,
        });
    }
    let burst_capacity = (batch.ts.capacity(), batch.value.capacity());
    batch.clear();
    let retained_capacity = (batch.ts.capacity(), batch.value.capacity());
    batch.ts.shrink_to_fit();
    batch.value.shrink_to_fit();
    let trimmed_capacity = (batch.ts.capacity(), batch.value.capacity());
    assert_eq!(retained_capacity, burst_capacity);
    assert_eq!(trimmed_capacity, (0, 0));
    println!("\nburst then trim");
    println!("  burst_capacity     = {:?}", burst_capacity);
    println!("  retained_after_clear = {:?}", retained_capacity);
    println!("  trimmed_capacity   = {:?}", trimmed_capacity);

    // ---------------------------------------------------------------------
    // 4. The same policy applies to any reusable buffer. Pick one policy per workload: reuse for a
    //    predictable rate, trim after an exceptional spike that should not define the long-lived
    //    footprint.
    // ---------------------------------------------------------------------
    let mut scratch = Vec::with_capacity(4_096);
    scratch.extend(0..1_024_u64);
    let reuse = recycle(&mut scratch, BufferPolicy::ReuseCapacity);
    let trim = recycle(&mut scratch, BufferPolicy::TrimToFit);
    println!("\npolicy comparison");
    println!("  ReuseCapacity -> capacity={reuse}");
    println!("  TrimToFit     -> capacity={trim}");
}
