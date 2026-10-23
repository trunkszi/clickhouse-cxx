//! One typed `RowClient` surface over two transports: an in-memory teaching
//! transport and the synchronous Native TCP `Client`.
//!
//! Run with:
//!
//! ```text
//! cargo run -p clickhouse --example 05_dual_transport_and_row_client
//! ```
//!
//! Set `CLICKHOUSE_URL=tcp://127.0.0.1:9000` to run the Native TCP part.
//! The in-memory part always runs and shows the exact same
//! `insert_batch`, `insert_rows`, `query_rows`, and `query_views` calls.
//!
//! The row includes Bool, Array, Nullable, and Map fields. On Native TCP those
//! columns are converted through the C++ column matrix before they reach the
//! server and when result blocks are projected back into typed rows.

use std::{collections::BTreeMap, env};

use clickhouse::{
    Batch, Block, Client, Endpoint, Row,
    row::{MapRef, OwnedRow, RowClient},
};

#[derive(Row)]
#[row(table = "lesson_composite")]
struct CompositeEvent {
    id: u64,
    active: bool,
    tags: Vec<u32>,
    note: Option<String>,
    scores: BTreeMap<String, u64>,
}

/// A minimal `RowClient` implementation backed by owned rows.
///
/// A transport only has to provide `insert_block` and `query_blocks`.
/// Everything else, including owned-row materialization and borrowed view
/// callbacks, is implemented by `RowClient` in terms of those two primitives.
#[derive(Default)]
struct MemoryTransport {
    rows: Vec<CompositeEvent>,
}

impl RowClient for MemoryTransport {
    fn insert_block(&mut self, _table: &str, block: &Block) -> clickhouse::Result<()> {
        let view = CompositeEventBlockView::try_from_block(block)?;
        for row in view.iter() {
            self.rows.push(CompositeEvent::from_ref(row));
        }
        Ok(())
    }

    fn query_blocks(&mut self, _sql: &str) -> clickhouse::Result<Vec<Block>> {
        let mut batch = CompositeEventBatch::with_capacity(self.rows.len());
        batch.extend_from_slice(&self.rows);
        Ok(vec![batch.to_block()?])
    }
}

fn print_event(prefix: &str, row: &CompositeEvent) {
    println!(
        "  {prefix} id={} active={} tags={:?} note={:?} scores={:?}",
        row.id, row.active, row.tags, row.note, row.scores
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---------------------------------------------------------------------
    // 1. Prepare one borrowed batch and one owned row.
    // ---------------------------------------------------------------------
    let mut first_scores = BTreeMap::new();
    first_scores.insert(String::from("a"), 1_u64);
    first_scores.insert(String::from("b"), 2_u64);

    let mut batch = CompositeEventBatch::with_capacity(1);
    batch.push_ref(CompositeEventRef {
        id: 1,
        active: true,
        tags: &[1, 2],
        note: Some("hello"),
        scores: MapRef::BTree(&first_scores),
    });

    let second = CompositeEvent {
        id: 2,
        active: false,
        tags: Vec::new(),
        note: None,
        scores: BTreeMap::new(),
    };

    // ---------------------------------------------------------------------
    // 2. In-memory transport.
    //
    // `insert_batch` sends the generated batch; `insert_rows` builds a
    // temporary batch from owned rows. Both paths use the same row codec.
    // ---------------------------------------------------------------------
    let mut memory = MemoryTransport::default();
    memory.insert_batch(&batch)?;
    memory.insert_rows(std::slice::from_ref(&second))?;

    let memory_rows: Vec<CompositeEvent> =
        memory.query_rows("SELECT id, active, tags, note, scores")?;
    println!("in-memory query_rows rows={}", memory_rows.len());
    for row in &memory_rows {
        print_event("row", row);
    }

    println!("in-memory query_views");
    memory.query_views::<CompositeEvent, _>("SELECT id, active, tags, note, scores", |view| {
        for row in view.iter() {
            let scores: Vec<(&str, u64)> = row.scores.iter().collect();
            println!(
                "  view id={} active={} tags={:?} note={:?} scores={scores:?}",
                row.id, row.active, row.tags, row.note
            );
        }
        Ok(())
    })?;

    // ---------------------------------------------------------------------
    // 3. Native TCP transport.
    //
    // `Client` implements the same trait. The DDL mirrors the derived row
    // schema, including the Map key/value spelling without extra spaces.
    // ---------------------------------------------------------------------
    let Ok(url) = env::var("CLICKHOUSE_URL") else {
        println!("\nSet CLICKHOUSE_URL=tcp://127.0.0.1:9000 to run the Native TCP part.");
        return Ok(());
    };

    let endpoint = Endpoint::parse(url)?;
    let mut client = Client::connect(endpoint, "default", "", "default")?;
    client.ping()?;
    client.execute(
        "CREATE OR REPLACE TABLE lesson_composite (id UInt64, active Bool, tags Array(UInt32), \
         note Nullable(String), scores Map(String, UInt64)) ENGINE = Memory",
    )?;

    client.insert_batch(&batch)?;
    client.insert_rows(std::slice::from_ref(&second))?;

    let live_rows: Vec<CompositeEvent> = client
        .query_rows("SELECT id, active, tags, note, scores FROM lesson_composite ORDER BY id")?;
    println!("\nNative TCP query_rows rows={}", live_rows.len());
    for row in &live_rows {
        print_event("row", row);
    }

    println!("Native TCP query_views");
    client.query_views::<CompositeEvent, _>(
        "SELECT id, active, tags, note, scores FROM lesson_composite ORDER BY id",
        |view| {
            for row in view.iter() {
                let scores: Vec<(&str, u64)> = row.scores.iter().collect();
                println!(
                    "  view id={} active={} tags={:?} note={:?} scores={scores:?}",
                    row.id, row.active, row.tags, row.note
                );
            }
            Ok(())
        },
    )?;

    client.execute("DROP TABLE lesson_composite")?;
    Ok(())
}
