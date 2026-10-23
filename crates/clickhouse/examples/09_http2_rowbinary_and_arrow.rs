//! HTTP protocol selection, RowBinary rows, Arrow projections, and caller-owned sinks.
//!
//! Run offline with `cargo run -p clickhouse --features http --example
//! 09_http2_rowbinary_and_arrow`. Set CLICKHOUSE_URL to an HTTP/HTTPS endpoint to exercise live
//! operations.
use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Float64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use clickhouse::{
    ArrowBlockView, Client, Endpoint, Query, Result, Row, SerdeBridge, row::RowClient,
};
use serde::{Deserialize, Serialize};

#[derive(Row, Serialize, Deserialize)]
struct Event {
    id: u64,
    name: String,
    value: f64,
}

/// Validate offline encodings, then optionally use the configured HTTP endpoint.
fn main() -> Result<()> {
    let rows = [
        Event {
            id: 1,
            name: String::from("negative-zero"),
            value: -0.0,
        },
        Event {
            id: 2,
            name: String::from("nan-payload"),
            value: f64::from_bits(0x7ff8_0000_0000_0001),
        },
    ];
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows)?;
    let decoded: Vec<Event> = SerdeBridge::from_rowbinary(&bytes)?;
    assert_bits(&decoded, &rows);
    offline_arrow(&rows)?;
    match env::var("CLICKHOUSE_URL") {
        Ok(url) if !url.is_empty() => live(&url, &rows)?,
        Ok(_) | Err(env::VarError::NotPresent) => {
            println!("Offline checks passed; set CLICKHOUSE_URL for live HTTP checks.")
        }
        Err(env::VarError::NotUnicode(_)) => {
            return Err(clickhouse::Error::Config {
                reason: "CLICKHOUSE_URL is not valid Unicode".into(),
            });
        }
    }
    Ok(())
}

/// Arrow IPC parsing owns decoded arrays; fixed-width projections borrow those arrays.
fn offline_arrow(rows: &[Event]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let array = Arc::new(Float64Array::from(
        rows.iter().map(|row| row.value).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).map_err(arrow_error)?;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).map_err(arrow_error)?;
        writer.write(&batch).map_err(arrow_error)?;
        writer.finish().map_err(arrow_error)?;
    }
    let views = ArrowBlockView::decode_stream(&bytes)?;
    let values = views[0].column_slice::<arrow_array::types::Float64Type>("value")?;
    assert_eq!(values[0].to_bits(), rows[0].value.to_bits());
    assert_eq!(values[1].to_bits(), rows[1].value.to_bits());
    Ok(())
}

/// Run real writes and reads on the client thread, and remove only the created table.
fn live(url: &str, rows: &[Event]) -> Result<()> {
    let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| String::from("default"));
    let password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let database = env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| String::from("default"));
    let mut client = Client::connect(
        Endpoint::parse(String::from(url))?,
        &user,
        &password,
        &database,
    )?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| clickhouse::Error::Config {
            reason: format!("system clock: {error}").into(),
        })?
        .as_nanos();
    let table = format!("http_example_{}_{suffix}", std::process::id());
    client.execute(format!(
        "CREATE TABLE {table}(id UInt64,name String,value Float64) ENGINE=Memory"
    ))?;
    let result: Result<()> = (|| {
        let block = SerdeBridge::rows_to_block(rows)?;
        client.set_insert_deduplication_token(SerdeBridge::deduplication_token(&block)?)?;
        client.serde_insert_rows(&table, rows)?;
        let query = Query::new(format!("SELECT value,name,id FROM {table} ORDER BY id"));
        let back: Vec<Event> = client.serde_query_rows(&query)?;
        assert_bits(&back, rows);
        let mut batch = EventBatch::with_capacity(rows.len());
        let written = RowClient::query_batch_into::<Event>(
            &mut client,
            &format!("SELECT id,name,value FROM {table} ORDER BY id"),
            &mut batch,
        )?;
        assert_eq!(written, rows.len());
        let arrow = client.fetch_bytes(&Query::new(format!(
            "SELECT value FROM {table} ORDER BY id FORMAT ArrowStream"
        )))?;
        let views = ArrowBlockView::decode_stream(&arrow)?;
        assert_eq!(
            views[0].column_slice::<arrow_array::types::Float64Type>("value")?[0].to_bits(),
            rows[0].value.to_bits()
        );
        let mut owned = Vec::with_capacity(128);
        let raw = Query::new(format!(
            "SELECT id FROM {table} ORDER BY id FORMAT TabSeparated"
        ));
        let written = client.query_raw_into(&raw, &mut owned)?;
        assert_eq!(written, owned.len());
        assert_eq!(owned, b"1\n2\n");
        println!(
            "Observed HTTP protocol: {:?}",
            client.negotiated_http_version()
        );
        Ok(())
    })();
    let cleanup = client.execute(format!("DROP TABLE {table}"));
    result?;
    cleanup
}

/// Floating-point verification compares payload bits, including signed zero and NaN.
fn assert_bits(actual: &[Event], expected: &[Event]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.value.to_bits(), expected.value.to_bits());
    }
}

/// Attach Arrow's diagnostic to the crate's structured protocol error.
fn arrow_error(error: arrow_schema::ArrowError) -> clickhouse::Error {
    clickhouse::Error::Protocol(format!("Arrow example: {error}").into())
}
