//! `#[derive(Row)]`: build a reusable columnar batch, materialize a block,
//! then read it back through a checked zero-copy view.
//!
//! Run with:
//!
//! ```text
//! cargo run -p clickhouse --example 02_derive_row_and_zero_copy_views
//! ```
//!
//! The example needs no server. It covers the row attributes, the generated
//! `Batch`, `BlockView`, and `Ref` types, and the borrowed field shapes used by
//! the Native TCP transport.

// The `eventId` field intentionally exercises `rename_all = "snake_case"`.
// The generated `LessonEventRef` keeps the Rust field spelling, so the allow
// covers both the source struct and the generated companion types.
#![allow(non_snake_case)]

use std::{borrow::Cow, collections::BTreeMap};

use clickhouse::{Row, row::MapRef};

#[derive(Row)]
#[row(table = "lesson_events", rename_all = "snake_case")]
struct LessonEvent<'input> {
    /// The attribute converts this Rust field name to the `event_id` column.
    eventId: u64,
    /// A per-field override; the column is `kind_text`, not `kind`.
    #[row(rename = "kind_text")]
    kind: Cow<'input, str>,
    /// `bytes` selects a ClickHouse String with binary payloads. Without it,
    /// `Vec<u8>`/`Cow<[u8]>`-shaped fields are treated as arrays.
    #[row(bytes)]
    payload: Cow<'input, [u8]>,
    user_id: Option<Cow<'input, str>>,
    scores: Vec<f64>,
    attributes: BTreeMap<String, i64>,
}

static PAYLOAD: &[u8] = &[0, 255, 7];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("table metadata");
    println!("  table   = {:?}", LessonEvent::TABLE_NAME);
    println!("  columns = {:?}", LessonEvent::COLUMN_NAMES);

    // ---------------------------------------------------------------------
    // 1. Fill the generated struct-of-arrays batch.
    //
    // `push` borrows an owned source row. `push_ref` accepts the generated
    // `LessonEventRef` directly, which avoids constructing owned strings or
    // arrays for values that are already borrowed.
    // ---------------------------------------------------------------------
    let owned_row = LessonEvent {
        eventId: 42,
        kind: Cow::Borrowed("trade"),
        payload: Cow::Borrowed(PAYLOAD),
        user_id: Some(Cow::Borrowed("u-1")),
        scores: vec![1.0, -0.0],
        attributes: BTreeMap::from([(String::from("venue"), 7_i64)]),
    };

    let borrowed_attributes = BTreeMap::from([(String::from("venue"), 9_i64)]);
    let mut batch = LessonEventBatch::with_capacity(2);
    batch.push(&owned_row);
    batch.push_ref(LessonEventRef {
        eventId: 43,
        kind: "quote",
        payload: &[],
        user_id: None,
        scores: &[3.5],
        attributes: MapRef::BTree(&borrowed_attributes),
    });
    println!(
        "\nbatch rows={} column_names={:?}",
        batch.len(),
        LessonEvent::COLUMN_NAMES
    );

    // ---------------------------------------------------------------------
    // 2. Materialize an independently owned block.
    //
    // `to_block` copies the flat column buffers. The block is then the owner
    // of every byte that the view below borrows.
    // ---------------------------------------------------------------------
    let block = batch.to_block()?;
    let view = LessonEventBlockView::try_from_block(&block)?;

    println!("\nzero-copy projections");
    println!("  view rows        = {}", view.len());
    println!("  event_id slice   = {:?}", view.eventId);
    println!("  scores values    = {:?}", view.scores.values);
    println!("  scores offsets   = {:?}", view.scores.offsets);

    // Fixed-width columns are slices into the block itself, not copies.
    let projected = block.column_slice_by_name::<u64>("event_id")?;
    assert_eq!(view.eventId.as_ptr(), projected.as_ptr());
    println!("  event_id address = {:p}", view.eventId.as_ptr());

    // ---------------------------------------------------------------------
    // 3. Iterate borrowed rows without allocating.
    //
    // String, binary, array, optional, and map fields borrow their backing
    // storage. The map iterator yields borrowed key/value projections.
    // ---------------------------------------------------------------------
    println!("\nborrowed rows");
    for (index, row) in view.iter().enumerate() {
        let attributes: Vec<(&str, i64)> = row.attributes.iter().collect();
        println!(
            "  row {index}: event_id={} kind={:?} payload={:?} user_id={:?} scores={:?} \
             attributes={attributes:?}",
            row.eventId, row.kind, row.payload, row.user_id, row.scores
        );
    }

    // `from_block` rebuilds a batch from the same projection and keeps the
    // reusable buffer shape for a subsequent ingestion cycle.
    let restored = LessonEventBatch::from_block(&block)?;
    println!(
        "\nrestored batch rows={} retained_event_id_capacity={}",
        restored.len(),
        restored.eventId.capacity()
    );

    Ok(())
}
