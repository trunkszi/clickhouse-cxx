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
//! `Batch`, `BlockView`, and `Ref` types, the borrowed field shapes used by
//! the Native TCP transport, and `RowClient` callback and caller-buffer reads
//! over an in-memory transport.

// The `eventId` field intentionally exercises `rename_all = "snake_case"`.
// The generated `LessonEventRef` keeps the Rust field spelling, so the allow
// covers both the source struct and the generated companion types.
#![allow(non_snake_case)]

use std::{borrow::Cow, collections::BTreeMap};

use clickhouse::{
    Batch, Block, Row,
    row::{MapRef, RowClient},
};

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

/// An owned row type used by the `RowClient` buffer demonstration.
#[derive(Debug, Clone, PartialEq, Row)]
struct FillEvent {
    id: u64,
    kind: String,
    score: f64,
}

/// A minimal synchronous transport: `insert_block` and `query_blocks` are the
/// only required `RowClient` primitives.
#[derive(Default)]
struct FillMemoryClient {
    rows: Vec<FillEvent>,
}

impl RowClient for FillMemoryClient {
    fn insert_block(&mut self, _table: &str, _block: &Block) -> clickhouse::Result<()> {
        Ok(())
    }

    fn query_blocks(&mut self, _sql: &str) -> clickhouse::Result<Vec<Block>> {
        let mut batch = FillEventBatch::with_capacity(self.rows.len());
        batch.extend_from_slice(&self.rows);
        Ok(vec![batch.to_block()?])
    }
}

/// Callback reads and caller-owned buffer reads over the same query result.
fn demonstrate_row_client() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FillMemoryClient {
        rows: vec![
            FillEvent {
                id: 1,
                kind: String::from("alpha"),
                score: 0.5,
            },
            FillEvent {
                id: 2,
                kind: String::from("beta"),
                score: 1.5,
            },
            FillEvent {
                id: 3,
                kind: String::from("gamma"),
                score: 2.5,
            },
        ],
    };
    let sql = "SELECT id, kind, score FROM fill_events";

    // Borrowed references die with each backing block. The callback must copy
    // anything it keeps.
    let mut kinds = Vec::new();
    let refs = client.query_row_refs::<FillEvent, _>(sql, |row| {
        kinds.push(row.kind.to_owned());
        Ok(())
    })?;

    // Owned rows are untouched by the transport's next block.
    let mut streamed = Vec::new();
    let owned = client.query_for_each::<FillEvent, _>(sql, |row| {
        streamed.push(row);
        Ok(())
    })?;

    // The caller owns these buffers; the client only overwrites their slots.
    let placeholder = FillEvent {
        id: 0,
        kind: String::new(),
        score: 0.0,
    };
    let mut slice = vec![placeholder; client.rows.len()];
    let filled = client.query_rows_into_slice(sql, &mut slice)?;

    let mut batch = FillEventBatch::with_capacity(client.rows.len());
    let batched = client.query_batch_into::<FillEvent>(sql, &mut batch)?;

    let mut block = Block::new();
    let blocked = client.query_block_into::<FillEvent>(sql, &mut block)?;
    let view = FillEventBlockView::try_from_block(&block)?;

    assert_eq!(&streamed, &client.rows);
    assert_eq!(slice, client.rows);
    assert_eq!(batch.len(), client.rows.len());
    assert_eq!(view.len(), client.rows.len());

    println!("  query_row_refs       calls={refs} kinds={kinds:?}");
    println!(
        "  query_for_each       calls={owned} rows={}",
        streamed.len()
    );
    println!(
        "  query_rows_into_slice count={filled} slots={}",
        slice.len()
    );
    println!(
        "  query_batch_into     count={batched} rows={}",
        batch.len()
    );
    println!(
        "  query_block_into     count={blocked} rows={} first={:?}",
        block.row_count(),
        view.get(0).map(|row| row.kind)
    );
    Ok(())
}

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

    // ---------------------------------------------------------------------
    // 4. Callback reads and caller-owned buffers through `RowClient`.
    //
    // `query_row_refs` lends each borrowed row for the duration of the
    // callback; `query_for_each` yields owned rows. The `_into` methods write
    // into storage the caller already owns, so steady-state reads can reuse
    // one slice, batch, or block.
    // ---------------------------------------------------------------------
    println!("\nRowClient callback and caller-buffer reads");
    demonstrate_row_client()?;

    Ok(())
}
