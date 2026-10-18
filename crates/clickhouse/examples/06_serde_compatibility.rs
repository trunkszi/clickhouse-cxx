//! Use `serde`-only structs through `SerdeBridge`, and derive `Row` alongside
//! `serde` on the same struct.
//!
//! Run the offline part with:
//!
//! ```text
//! cargo run -p clickhouse --example 06_serde_compatibility
//! ```
//!
//! Set `CLICKHOUSE_URL` to exercise the live Native TCP part:
//!
//! ```text
//! CLICKHOUSE_URL=tcp://127.0.0.1:9000 \
//!   cargo run -p clickhouse --example 06_serde_compatibility
//! ```
//!
//! `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE` are
//! optional and default to the standard `default` / empty / `default` login.
//!
//! The first struct derives only `serde`. The second derives `Row` and
//! `serde`; its serde naming attributes also define the generated column
//! schema.

use std::env;

use clickhouse::{Batch, Block, Client, Endpoint, Row, SerdeBridge, row::RowClient};
use serde::{Deserialize, Serialize};

/// A pure serde struct: no `#[derive(Row)]`, no generated batch or view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct QuoteTick {
    instrument: String,
    sequence: u64,
    bid: f64,
    ask: f64,
    venue: Option<String>,
    tags: Vec<u32>,
}

/// A row type that also derives serde.
///
/// The serde naming attributes are part of the row schema:
/// `rename_all` produces `accountId` and `displayName`, the field-level
/// `rename` produces `balance_micros`, and `serde_bytes` stores `payload` as
/// a binary `String` instead of `Array(UInt8)`.
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[row(table = "lesson_serde_account")]
struct AccountSnapshot {
    account_id: u64,
    display_name: String,
    #[serde(rename = "balance_micros")]
    balance: i64,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

/// A minimal `RowClient` that keeps owned serde rows.
///
/// Implementing `insert_block` and `query_blocks` is enough: `insert_serde`
/// and `query_serde` are provided by the trait and delegate to
/// `SerdeBridge` and these two primitives.
#[derive(Default)]
struct SerdeMemoryTransport {
    quotes: Vec<QuoteTick>,
}

impl RowClient for SerdeMemoryTransport {
    fn insert_block(&mut self, _table: &str, block: &Block) -> clickhouse::Result<()> {
        self.quotes
            .extend(SerdeBridge::block_to_rows::<QuoteTick>(block)?);
        Ok(())
    }

    fn query_blocks(&mut self, _sql: &str) -> clickhouse::Result<Vec<Block>> {
        Ok(vec![SerdeBridge::rows_to_block(&self.quotes)?])
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut text, "{byte:02x}").expect("writing to a String cannot fail");
    }
    text
}

fn print_columns(prefix: &str, block: &Block) -> clickhouse::Result<()> {
    println!(
        "{prefix} rows={} columns={}",
        block.row_count(),
        block.column_count()
    );
    for index in 0..block.column_count() {
        let column = block.column(index)?;
        println!("  {}: {}", column.name, column.type_name);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let quotes = vec![
        QuoteTick {
            instrument: String::from("BTC-USD"),
            sequence: 1,
            bid: 100.5,
            ask: 100.75,
            venue: Some(String::from("primary")),
            tags: vec![7, 11],
        },
        QuoteTick {
            instrument: String::from("ETH-USD"),
            sequence: 2,
            bid: 2_500.25,
            ask: 2_501.5,
            venue: None,
            tags: vec![13],
        },
    ];

    // ---------------------------------------------------------------------
    // 1. RowBinary for a serde-only struct.
    //
    // `to_rowbinary` walks the serde representation of the first row to learn
    // the column names and field order, then appends every row.
    // `from_rowbinary` consumes concatenated rows until the buffer ends.
    // ---------------------------------------------------------------------
    let (names, encoded) = SerdeBridge::to_rowbinary(&quotes)?;
    println!(
        "RowBinary columns={names:?} rows={} bytes={}",
        quotes.len(),
        encoded.len()
    );
    let decoded: Vec<QuoteTick> = SerdeBridge::from_rowbinary(&encoded)?;
    assert_eq!(decoded, quotes);
    println!("RowBinary round-trip rows={}", decoded.len());

    // ---------------------------------------------------------------------
    // 2. Block conversion for the same serde-only struct.
    //
    // `rows_to_block` dispatches each serde field into a typed column:
    // `Option` becomes `Nullable`, `Vec` becomes `Array`, and a
    // `serde_bytes` payload becomes a binary `String`.
    // ---------------------------------------------------------------------
    let quote_block = SerdeBridge::rows_to_block(&quotes)?;
    print_columns("serde Block", &quote_block)?;
    assert_eq!(
        quote_block.column_by_name("venue")?.type_name,
        "Nullable(String)"
    );
    assert_eq!(
        quote_block.column_by_name("tags")?.type_name,
        "Array(UInt32)"
    );
    let from_block: Vec<QuoteTick> = SerdeBridge::block_to_rows(&quote_block)?;
    assert_eq!(from_block, quotes);
    println!("Block round-trip rows={}", from_block.len());

    // ---------------------------------------------------------------------
    // 3. The shared `RowClient` surface over an in-memory transport.
    //
    // `insert_serde` and `query_serde` are trait methods, so the same calls
    // work for the Native TCP `Client` below.
    // ---------------------------------------------------------------------
    let mut memory = SerdeMemoryTransport::default();
    memory.insert_serde("lesson_serde_quote", &quotes)?;
    let memory_rows: Vec<QuoteTick> = memory.query_serde(
        "SELECT instrument, sequence, bid, ask, venue, tags FROM lesson_serde_quote ORDER BY \
         sequence",
    )?;
    assert_eq!(memory_rows, quotes);
    println!("\nin-memory query_serde rows={}", memory_rows.len());

    // ---------------------------------------------------------------------
    // 4. `#[derive(Row)]` and serde on the same struct.
    //
    // The generated row schema reads the serde naming attributes, so the
    // generated block and the serde bridge agree on column names and types.
    // ---------------------------------------------------------------------
    let snapshots = vec![
        AccountSnapshot {
            account_id: 7,
            display_name: String::from("alice"),
            balance: -1_250_000,
            payload: vec![0x00, 0xff, 0x80, 0x13],
        },
        AccountSnapshot {
            account_id: 9,
            display_name: String::from("bob"),
            balance: 4_000_000,
            payload: vec![0xde, 0xad, 0xbe, 0xef],
        },
    ];

    println!("\nderived row + serde");
    println!("  COLUMN_NAMES = {:?}", AccountSnapshot::COLUMN_NAMES);
    assert_eq!(
        AccountSnapshot::COLUMN_NAMES,
        &["accountId", "displayName", "balance_micros", "payload"][..]
    );

    let mut batch = AccountSnapshotBatch::with_capacity(snapshots.len());
    batch.extend_from_slice(&snapshots);
    let generated = batch.to_block()?;
    print_columns("derived Block", &generated)?;
    assert_eq!(generated.column_by_name("payload")?.type_name, "String");

    let view = AccountSnapshotBlockView::try_from_block(&generated)?;
    for (index, row) in view.iter().enumerate() {
        println!(
            "  view row {index}: accountId={} displayName={} balance_micros={} payload={}",
            row.account_id,
            row.display_name,
            row.balance,
            hex(row.payload)
        );
        assert_eq!(row.payload, snapshots[index].payload.as_slice());
    }

    // The serde bridge builds the same schema for the same rows.
    let bridged = SerdeBridge::rows_to_block(&snapshots)?;
    print_columns("serde Block", &bridged)?;
    assert_eq!(generated.column_count(), bridged.column_count());
    for index in 0..generated.column_count() {
        let generated_column = generated.column(index)?;
        let bridged_column = bridged.column(index)?;
        assert_eq!(generated_column.name, bridged_column.name);
        assert_eq!(generated_column.type_name, bridged_column.type_name);
    }

    let bridged_rows: Vec<AccountSnapshot> = SerdeBridge::block_to_rows(&bridged)?;
    assert_eq!(bridged_rows, snapshots);
    println!("derived/serde Block agreement rows={}", bridged_rows.len());

    let (account_names, account_bytes) = SerdeBridge::to_rowbinary(&snapshots)?;
    let account_rows: Vec<AccountSnapshot> = SerdeBridge::from_rowbinary(&account_bytes)?;
    assert_eq!(account_rows, snapshots);
    assert_eq!(
        account_names.as_slice(),
        &["accountId", "displayName", "balance_micros", "payload"][..]
    );
    println!(
        "derived RowBinary columns={account_names:?} bytes={} round-trip rows={}",
        account_bytes.len(),
        account_rows.len()
    );

    // ---------------------------------------------------------------------
    // 5. Optional live Native TCP round-trip.
    //
    // The pure serde struct and the combined derive both travel through
    // `insert_serde` / `query_serde`. The Memory tables mirror the struct
    // schemas, including the serde column names and the binary payload.
    // ---------------------------------------------------------------------
    let Ok(url) = env::var("CLICKHOUSE_URL") else {
        println!("\nSet CLICKHOUSE_URL=tcp://127.0.0.1:9000 to run the Native TCP part.");
        return Ok(());
    };

    let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| String::from("default"));
    let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| String::from("default"));
    let endpoint = Endpoint::parse(url)?;
    let mut client = Client::connect(endpoint, &user, &password, &database)?;
    client.ping()?;

    client.execute(
        "CREATE OR REPLACE TABLE lesson_serde_quote (instrument String, sequence UInt64, bid \
         Float64, ask Float64, venue Nullable(String), tags Array(UInt32)) ENGINE = Memory",
    )?;
    client.insert_serde("lesson_serde_quote", &quotes)?;
    let live_quotes: Vec<QuoteTick> = client.query_serde(
        "SELECT instrument, sequence, bid, ask, venue, tags FROM lesson_serde_quote ORDER BY \
         sequence",
    )?;
    assert_eq!(live_quotes, quotes);
    println!("\nNative TCP query_serde rows={}", live_quotes.len());
    client.execute("DROP TABLE lesson_serde_quote")?;

    client.execute(
        "CREATE OR REPLACE TABLE lesson_serde_account (accountId UInt64, displayName String, \
         balance_micros Int64, payload String) ENGINE = Memory",
    )?;
    client.insert_serde("lesson_serde_account", &snapshots)?;
    let live_snapshots: Vec<AccountSnapshot> = client.query_serde(
        "SELECT accountId, displayName, balance_micros, payload FROM lesson_serde_account ORDER \
         BY accountId",
    )?;
    assert_eq!(live_snapshots, snapshots);
    println!("Native TCP combined derive rows={}", live_snapshots.len());
    client.execute("DROP TABLE lesson_serde_account")?;

    Ok(())
}
