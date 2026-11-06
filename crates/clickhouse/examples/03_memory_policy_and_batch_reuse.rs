//! Buffer policy: keep steady-state ingestion allocation-free while bounding
//! the retained capacity after a one-off burst.
//!
//! Run with:
//!
//! ```text
//! cargo run -p clickhouse --example 03_memory_policy_and_batch_reuse
//! ```
//!
//! A derived `<Name>Batch` is a struct of arrays. `clickhouse::BufferPolicy`
//! gives every nested `Vec` one aggregate row/byte budget, so a burst cannot
//! silently pin an unbounded amount of memory:
//!
//! - `BufferPolicy::default()` retains up to 65,536 rows and 8 MiB.
//! - `BufferPolicy::unbounded()` retains every warmed allocation.
//! - `BufferPolicy::minimal()` releases all value-buffer capacity.
//!
//! `allocated_bytes()` counts the exact `Vec` capacity, `clear()` applies the
//! default policy, `clear_with_policy()` applies a custom budget, and
//! `shrink_to_fit()` releases unused capacity without dropping current rows.

use clickhouse::{BufferPolicy, Row};

#[derive(Row)]
struct Sample {
    ts: u64,
    value: f64,
    label: String,
    tags: Vec<u32>,
}

/// Fill a batch with rows that exercise fixed, string, and array buffers.
fn fill(batch: &mut SampleBatch, rows: usize) {
    for index in 0..rows {
        batch.push_ref(SampleRef {
            ts: index as u64,
            value: index as f64,
            label: "warm-buffer-policy",
            tags: &[1, 2, 3],
        });
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let default_policy = BufferPolicy::default();
    println!(
        "default BufferPolicy: max_retained_rows={} max_retained_bytes={}",
        default_policy.max_retained_rows, default_policy.max_retained_bytes
    );

    // ---------------------------------------------------------------------
    // 1. Warm the batch once with a known working set.
    // ---------------------------------------------------------------------
    let mut batch = SampleBatch::with_capacity(1_024);
    fill(&mut batch, 1_024);
    let warm_bytes = batch.allocated_bytes();
    let warm_capacity = (
        batch.ts.capacity(),
        batch.label.offsets.capacity(),
        batch.tags.values.capacity(),
    );
    println!("\nafter warm-up");
    println!("  rows={} allocated_bytes={warm_bytes}", batch.len());
    println!(
        "  ts_capacity={} label_offset_capacity={} tag_value_capacity={}",
        warm_capacity.0, warm_capacity.1, warm_capacity.2
    );

    // ---------------------------------------------------------------------
    // 2. `clear()` applies the default policy. This working set is below the budget, so every
    //    allocation is retained for the next cycle.
    // ---------------------------------------------------------------------
    batch.clear();
    assert_eq!(batch.len(), 0);
    assert_eq!(batch.allocated_bytes(), warm_bytes);
    fill(&mut batch, 64);
    assert_eq!(
        (
            batch.ts.capacity(),
            batch.label.offsets.capacity(),
            batch.tags.values.capacity(),
        ),
        warm_capacity
    );
    println!("\nafter default clear + steady refill");
    println!(
        "  rows={} allocated_bytes={}",
        batch.len(),
        batch.allocated_bytes()
    );
    println!("  allocations and addresses were retained");

    // ---------------------------------------------------------------------
    // 3. A burst exceeds the default row budget. `clear()` keeps the budgeted share instead of
    //    pinning the entire peak.
    // ---------------------------------------------------------------------
    batch.clear();
    fill(&mut batch, 262_144);
    let peak_bytes = batch.allocated_bytes();
    batch.clear();
    let default_cleared_bytes = batch.allocated_bytes();
    assert!(default_cleared_bytes <= default_policy.max_retained_bytes);
    assert!(default_cleared_bytes < peak_bytes);
    assert_eq!(batch.len(), 0);
    println!("\nburst then default clear");
    println!("  peak_allocated_bytes         = {peak_bytes}");
    println!("  after_default_clear_bytes    = {default_cleared_bytes}");
    println!(
        "  retained within row budget   = {}",
        default_cleared_bytes <= default_policy.max_retained_bytes
    );

    // ---------------------------------------------------------------------
    // 4. An explicit policy can be tighter than the default. The aggregate budget is shared across
    //    every field, including nested string bytes, array offsets, and array values.
    // ---------------------------------------------------------------------
    fill(&mut batch, 4_096);
    let bounded_before = batch.allocated_bytes();
    let bounded_policy = BufferPolicy {
        max_retained_rows: 1_024,
        max_retained_bytes: 128 * 1_024,
    };
    batch.clear_with_policy(bounded_policy);
    let bounded_after = batch.allocated_bytes();
    assert!(bounded_after <= bounded_policy.max_retained_bytes);
    assert!(bounded_after < bounded_before);
    assert_eq!(batch.len(), 0);
    println!("\ncustom policy");
    println!(
        "  before={bounded_before} max_retained_rows={} max_retained_bytes={} \
         after={bounded_after}",
        bounded_policy.max_retained_rows, bounded_policy.max_retained_bytes
    );

    // ---------------------------------------------------------------------
    // 5. `clear_unbounded()` is the explicit "keep everything" policy. It is useful when the next
    //    cycle is known to have the same shape.
    // ---------------------------------------------------------------------
    fill(&mut batch, 1_024);
    let before_unbounded = batch.allocated_bytes();
    batch.clear_unbounded();
    assert_eq!(batch.allocated_bytes(), before_unbounded);
    assert_eq!(batch.len(), 0);
    let unbounded = BufferPolicy::unbounded();
    println!("\nunbounded clear");
    println!(
        "  max_retained_rows={} max_retained_bytes={} retained_bytes={}",
        unbounded.max_retained_rows,
        unbounded.max_retained_bytes,
        batch.allocated_bytes()
    );

    // ---------------------------------------------------------------------
    // 6. `minimal()` releases all value-buffer capacity.
    // ---------------------------------------------------------------------
    fill(&mut batch, 16);
    batch.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
    assert_eq!(batch.len(), 0);
    println!("\nminimal policy");
    println!("  allocated_bytes={}", batch.allocated_bytes());

    // ---------------------------------------------------------------------
    // 7. `shrink_to_fit()` is different from clearing: it keeps the current rows and releases only
    //    the unused capacity behind them.
    // ---------------------------------------------------------------------
    fill(&mut batch, 1_024);
    let before_shrink = batch.allocated_bytes();
    let rows_before_shrink = batch.len();
    batch.shrink_to_fit();
    let after_shrink = batch.allocated_bytes();
    assert!(after_shrink <= before_shrink);
    assert_eq!(batch.len(), rows_before_shrink);
    let mut block = batch.to_block()?;
    let view = SampleBlockView::try_from_block(&block)?;
    assert_eq!(view.len(), 1_024);
    let first = view.get(0).expect("first row");
    let last = view.get(1_023).expect("last row");
    assert_eq!(first.ts, 0);
    assert_eq!(last.ts, 1_023);
    println!("\nshrink_to_fit with live rows");
    println!(
        "  rows={} before_bytes={before_shrink} after_bytes={after_shrink}",
        rows_before_shrink
    );

    // ---------------------------------------------------------------------
    // 8. `Block` and `Column` expose the same memory policy. A minimal clear on a block releases
    //    both the column values and the column schema.
    // ---------------------------------------------------------------------
    let block_bytes = block.allocated_bytes();
    block.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.allocated_bytes(), 0);
    println!("\nBlock policy");
    println!(
        "  before_bytes={block_bytes} after_minimal_clear_bytes={}",
        block.allocated_bytes()
    );

    Ok(())
}
