//! Quickstart: build a block, read fixed-width columns without copying, and
//! optionally round-trip the same data through a live Native TCP server.
//!
//! Run the offline part with:
//!
//! ```text
//! cargo run -p clickhouse --example 01_quickstart_native_tcp
//! ```
//!
//! Set `CLICKHOUSE_URL` to exercise the live part:
//!
//! ```text
//! CLICKHOUSE_URL=tcp://127.0.0.1:9000 \
//!   cargo run -p clickhouse --example 01_quickstart_native_tcp
//! ```
//!
//! `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE` are
//! optional and default to the standard `default` / empty / `default` login.

use std::env;

use clickhouse::{Block, Client, Endpoint, NativeBlock, WireBlock, WireColumn};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ids = [101_u64, 102, 103];
    let values = [1.5_f64, 2.5, 3.5];

    // ---------------------------------------------------------------------
    // 1. The owned row-layer Block.
    //
    // `Block` stores aligned bytes and is the representation produced by
    // `#[derive(Row)]` batches. Fixed-width columns can be projected back as
    // ordinary Rust slices with no element-by-element conversion.
    // ---------------------------------------------------------------------
    let mut block = Block::new();
    block.append_fixed_slice("id", "UInt64", &ids)?;
    block.append_fixed_slice("value", "Float64", &values)?;

    let block_ids: &[u64] = block.column_slice_by_name("id")?;
    let block_values: &[f64] = block.column_slice_by_name("value")?;
    println!("row-layer Block");
    println!(
        "  rows={} columns={}",
        block.row_count(),
        block.column_count()
    );
    println!("  id    = {block_ids:?}");
    println!("  value = {block_values:?}");

    // ---------------------------------------------------------------------
    // 2. The native WireBlock.
    //
    // WireBlock owns C++ `clickhouse::Block` storage. `column_slice` validates
    // the ClickHouse type before projecting the native buffer as `&[T]`.
    // ---------------------------------------------------------------------
    let mut wire_block = WireBlock::new()?;
    wire_block.append("id", WireColumn::fixed(&ids)?)?;
    wire_block.append("value", WireColumn::fixed(&values)?)?;

    let wire_ids: &[u64] = wire_block.column_slice("id")?;
    let wire_values: &[f64] = wire_block.column_slice("value")?;
    println!("\nC++ matrix WireBlock");
    println!(
        "  rows={} columns={}",
        wire_block.row_count(),
        wire_block.column_count()
    );
    for name in wire_block.column_names() {
        println!(
            "  {name}: {} -> {:?}",
            wire_block.column_type_name(name)?,
            wire_block.column_type_code(name)?
        );
    }
    println!("  id    = {wire_ids:?}");
    println!("  value = {wire_values:?}");

    // The slices above borrow block storage. No vector of values is created
    // between the column bytes and the printed slice.
    println!(
        "  first id address inside block: {:p}",
        wire_ids.first().expect("one row")
    );

    // ---------------------------------------------------------------------
    // 3. Optional live server round-trip.
    //
    // The transport `Client` implements the same `insert_block` /
    // `query_blocks` primitives used by the typed row layer. `NativeBlock`
    // is the transport-side builder for primitive columns.
    // ---------------------------------------------------------------------
    let Ok(url) = env::var("CLICKHOUSE_URL") else {
        println!("\nSet CLICKHOUSE_URL=tcp://127.0.0.1:9000 to run the live Native TCP part.");
        return Ok(());
    };

    let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| String::from("default"));
    let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| String::from("default"));
    let endpoint = Endpoint::parse(url)?;
    let mut client = Client::connect(endpoint, &user, &password, &database)?;
    client.ping()?;

    client.execute(
        "CREATE OR REPLACE TABLE lesson_quickstart (id UInt64, value Float64) ENGINE = Memory",
    )?;

    let mut native = NativeBlock::new();
    native.push_u64("id", &ids)?;
    native.push_f64("value", &values)?;
    client.insert_block("lesson_quickstart", &mut native)?;

    println!("\nlive Native TCP rows");
    for block in client.query_blocks("SELECT id, value FROM lesson_quickstart ORDER BY id")? {
        // The Native protocol may deliver a schema-only header block before
        // the data blocks. It has no rows and no usable column values.
        if block.rows() == 0 {
            continue;
        }
        let live_ids: &[u64] = block.column_slice("id")?;
        let live_values: &[f64] = block.column_slice("value")?;
        println!("  id={live_ids:?} value={live_values:?}");
    }

    client.execute("DROP TABLE lesson_quickstart")?;
    Ok(())
}
