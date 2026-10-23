//! SoA append, checked projections, borrowed rows, and owned row materialization.
use std::hint::black_box;

use clickhouse::{Row, row::OwnedRow};
mod support;
use support::measure;
#[derive(Row)]
struct Event {
    id: u64,
    text: String,
    values: Vec<u32>,
}
fn main() {
    let mut batch = EventBatch::with_capacity(4096);
    for id in 0..4096 {
        batch.push_ref(EventRef {
            id,
            text: "payload",
            values: &[1, 2, 3],
        });
    }
    let block = batch.to_block().unwrap();
    let view = EventBlockView::try_from_block(&block).unwrap();
    assert_eq!(
        view.column_id().as_ptr(),
        block.column_slice::<u64>(0).unwrap().as_ptr()
    );
    measure("rows/soa_append", 4096, || {
        batch.clear();
        for row in view.iter() {
            batch.push_ref(black_box(row));
        }
        black_box(batch.len());
    });
    measure("rows/zero_copy_slice", 4096, || {
        black_box(block.column_slice::<u64>(0).unwrap())
    });
    measure("rows/checked_view", 4096, || {
        black_box(EventBlockView::try_from_block(&block).unwrap())
    });
    measure("rows/borrowed_rows", 4096, || {
        for row in view.iter() {
            black_box(row);
        }
    });
    measure("rows/owned_rows", 4096, || {
        let rows: Vec<_> = view.iter().map(Event::from_ref).collect();
        black_box(rows);
    });
}
