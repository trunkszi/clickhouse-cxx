#![cfg(feature = "http")]
//! E2E lifecycle and scenario coverage over an isolated ClickHouse 26.8.2.7 server.
//! Native TCP and production HTTP carry real rows; nginx is a controlled HTTP/2 edge.
#[path = "support/docker.rs"]
mod docker;
#[path = "support/http2.rs"]
mod http2;

use clickhouse::{
    ArrowBlockView, Client, Compression, Endpoint, HttpVersionChoice, Query, Row, row::RowClient,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Row, Serialize, Deserialize)]
#[row(table = "typed_rows")]
struct Event {
    id: u64,
    name: String,
    score: Option<i64>,
    values: Vec<f64>,
    z: f64,
}

/// Real transports preserve nullable, arrays, borrowed views, and floating-point bits.
#[test]
fn live_http_and_optional_native_roundtrip() {
    let server = docker::Server::start();
    #[cfg(feature = "tcp")]
    let mut native = connect("tcp", &server.tcp_address);
    let mut http = connect("http", &server.address);
    http.execute(
        "CREATE TABLE typed_rows (id UInt64, name String, score Nullable(Int64), values \
         Array(Float64), z Float64) ENGINE=Memory",
    )
    .unwrap();
    #[cfg(feature = "tcp")]
    typed_rows(&mut native, 1);
    assert_eq!(http.query_u64("SELECT toUInt64(7)").unwrap(), 7);
    assert!(http.query_u64("SELECT toFloat64(7)").is_err());
    assert!(http.query_u64("SELECT toInt64(7)").is_err());
    typed_rows(&mut http, 2);
    #[cfg(feature = "tcp")]
    serde_rows(&mut native, 3);
    serde_rows(&mut http, 4);
    staged_insert_lifecycle(&mut http, "staged_http");
    buffered_stream_drop(&mut http);
    compression_and_dedup(&mut http);
    arrow_roundtrip(&mut http);
    let proxy = http2::Http2Proxy::start(&server.address);
    let mut h2 = connect("http", &proxy.address);
    h2.set_http_version(HttpVersionChoice::Http2PriorKnowledge)
        .unwrap();
    typed_rows(&mut h2, 5);
    serde_rows(&mut h2, 6);
    incremental_h2_read(&mut h2);
    assert_eq!(
        h2.negotiated_http_version(),
        Some(HttpVersionChoice::Http2PriorKnowledge)
    );
    let expected_count = if cfg!(feature = "tcp") {
        b"6\n"
    } else {
        b"4\n"
    };
    assert_eq!(
        h2.fetch_bytes(&Query::new(
            "SELECT count() FROM typed_rows FORMAT TabSeparated"
        ))
        .unwrap(),
        expected_count
    );
    drop(h2);
    drop(proxy);
    drop(http);
    #[cfg(feature = "tcp")]
    drop(native);
    let id = String::from(server.id());
    drop(server);
    assert!(
        !std::process::Command::new("docker")
            .args(["inspect", &id])
            .output()
            .unwrap()
            .status
            .success()
    );
}

/// Open the public synchronous entry point without substituting the client transport.
fn connect(scheme: &str, address: &str) -> Client {
    let endpoint = Endpoint::parse(format!("{scheme}://{address}")).unwrap();
    Client::connect(endpoint, "default", "", "default").unwrap()
}

/// Construct bit-sensitive data including a noncanonical quiet NaN payload.
fn event(id: u64) -> Event {
    Event {
        id,
        name: String::from("BTC/USDT"),
        score: if id.is_multiple_of(2) { None } else { Some(-4) },
        values: vec![1.25, -0.0],
        z: f64::from_bits(if id.is_multiple_of(2) {
            0x7ff8_0000_0000_0001
        } else {
            0x8000_0000_0000_0000
        }),
    }
}

/// Derived batches and borrowed projections share the production RowClient boundary.
fn typed_rows(client: &mut Client, id: u64) {
    let row = event(id);
    let mut batch = EventBatch::with_capacity(4);
    batch.push(&row);
    let source = batch.to_block().unwrap();
    let source_view = EventBlockView::try_from_block(&source).unwrap();
    assert_eq!(
        source_view.id.as_ptr(),
        source.column_slice::<u64>(0).unwrap().as_ptr()
    );
    RowClient::insert_batch(client, &batch).unwrap_or_else(|error| {
        let endpoint = client.endpoint();
        panic!(
            "typed batch id={id} endpoint={:?}://{}:{}: {error}",
            endpoint.scheme(),
            endpoint.host(),
            endpoint.port()
        );
    });
    batch.clear();
    assert!(batch.is_empty());
    let sql = format!("SELECT id,name,score,values,z FROM typed_rows WHERE id={id}");
    let mut visited = 0;
    RowClient::query_views::<Event, _>(client, &sql, |view| {
        for value in view.iter() {
            assert_eq!(value.id, row.id);
            assert_eq!(value.name, row.name);
            assert_eq!(value.score, row.score);
            assert_eq!(value.z.to_bits(), row.z.to_bits());
            assert_eq!(value.values[1].to_bits(), (-0.0_f64).to_bits());
            visited += 1;
        }
        assert!(view.get(view.len()).is_none());
        Ok(())
    })
    .unwrap();
    assert_eq!(visited, 1);
    let owned = RowClient::query_rows::<Event>(client, &sql).unwrap();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0].z.to_bits(), row.z.to_bits());
}

/// Serde-only calls work without relying on derived row metadata or field order.
fn serde_rows(client: &mut Client, id: u64) {
    let row = event(id);
    client
        .serde_insert_rows("typed_rows", std::slice::from_ref(&row))
        .unwrap();
    let rows: Vec<Event> = client
        .serde_query_rows(&Query::new(format!(
            "SELECT z,name,score,values,id FROM typed_rows WHERE id={id}"
        )))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].name, row.name);
    assert_eq!(rows[0].score, row.score);
    assert_eq!(rows[0].z.to_bits(), row.z.to_bits());
    assert_eq!(rows[0].values[1].to_bits(), (-0.0_f64).to_bits());
}

/// Compression roundtrips and query-log settings verify server-observed options.
fn compression_and_dedup(client: &mut Client) {
    client
        .execute("CREATE TABLE compressed (id UInt64) ENGINE=Memory")
        .unwrap();
    for (id, compression) in [(1_u64, Compression::Lz4), (2, Compression::Zstd)] {
        client.set_compression(compression).unwrap();
        let mut block = clickhouse::Block::new();
        block.append_fixed_slice("id", "UInt64", &[id]).unwrap();
        RowClient::insert_block(client, "compressed", &block).unwrap();
        let blocks =
            RowClient::query_blocks(client, &format!("SELECT id FROM compressed WHERE id={id}"))
                .unwrap();
        assert_eq!(blocks[0].column_slice::<u64>(0).unwrap(), &[id]);
    }
    client.set_compression(Compression::None).unwrap();
    client
        .set_insert_deduplication_token("live-content-token")
        .unwrap();
    client
        .execute("CREATE TABLE token_rows (id UInt64) ENGINE=MergeTree ORDER BY id")
        .unwrap();
    let mut block = clickhouse::Block::new();
    block.append_fixed_slice("id", "UInt64", &[7_u64]).unwrap();
    RowClient::insert_block(client, "token_rows", &block).unwrap();
    client.execute("SYSTEM FLUSH LOGS").unwrap();
    let bytes = client
        .fetch_bytes(&Query::new(
            "SELECT Settings['insert_deduplication_token'] FROM system.query_log WHERE query LIKE \
             'INSERT INTO%token_rows%' AND type='QueryFinish' FORMAT TabSeparated",
        ))
        .unwrap();
    assert!(
        String::from_utf8_lossy(&bytes)
            .lines()
            .any(|line| line == "live-content-token")
    );
}

/// Arrow IPC parsing allocates; primitive projections borrow the decoded Arrow buffers.
fn arrow_roundtrip(client: &mut Client) {
    client
        .execute("CREATE TABLE arrow_rows (z Float64,n Float64) ENGINE=Memory")
        .unwrap();
    client
        .execute("CREATE TABLE arrow_copy (z Float64,n Float64) ENGINE=Memory")
        .unwrap();
    let negative_zero = (-0.0_f64).to_bits();
    let nan = 0x7ff8_0000_0000_0001;
    let mut block = clickhouse::Block::new();
    block
        .append_fixed_slice("z", "Float64", &[f64::from_bits(negative_zero)])
        .unwrap();
    block
        .append_fixed_slice("n", "Float64", &[f64::from_bits(nan)])
        .unwrap();
    RowClient::insert_block(client, "arrow_rows", &block).unwrap();
    let query = Query::new("SELECT z,n FROM arrow_rows FORMAT ArrowStream")
        .setting("output_format_arrow_compression_method", "none");
    let bytes = client.fetch_bytes(&query).unwrap();
    let views = ArrowBlockView::decode_stream(&bytes).unwrap();
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].row_count(), 1);
    let z = views[0]
        .column_slice::<arrow_array::types::Float64Type>("z")
        .unwrap();
    let n = views[0]
        .column_slice::<arrow_array::types::Float64Type>("n")
        .unwrap();
    assert_eq!(z[0].to_bits(), negative_zero);
    assert_eq!(n[0].to_bits(), nan);
    let array = views[0]
        .column("z")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert_eq!(z.as_ptr(), array.values().as_ptr());
    assert!(ArrowBlockView::decode_stream(&bytes[..bytes.len() / 2]).is_err());
    client
        .insert_formatted(
            &Query::new("INSERT INTO arrow_copy FORMAT ArrowStream"),
            &bytes,
        )
        .unwrap();
    let blocks = RowClient::query_blocks(client, "SELECT z,n FROM arrow_copy").unwrap();
    assert_eq!(
        blocks[0].column_slice::<f64>(0).unwrap()[0].to_bits(),
        negative_zero
    );
    assert_eq!(blocks[0].column_slice::<f64>(1).unwrap()[0].to_bits(), nan);
}

/// Unfinalized batches never reach the table; finalized staging preserves every block.
fn staged_insert_lifecycle(client: &mut Client, table: &str) {
    client
        .execute(format!("CREATE TABLE {table}(id UInt64) ENGINE=Memory"))
        .unwrap();
    let mut block = clickhouse::Block::new();
    block.append_fixed_slice("id", "UInt64", &[3_u64]).unwrap();
    {
        let mut inserter = client.http_inserter(table).unwrap();
        inserter.write(&block).unwrap();
    }
    let rows =
        RowClient::query_blocks(client, &format!("SELECT count() AS n FROM {table}")).unwrap();
    assert_eq!(rows[0].column_slice::<u64>(0).unwrap(), &[0]);
    {
        let mut inserter = client.http_inserter(table).unwrap();
        inserter.write(&block).unwrap();
        let mut incompatible = clickhouse::Block::new();
        incompatible
            .append_fixed_slice("id", "Int64", &[9_i64])
            .unwrap();
        assert!(inserter.write(&incompatible).is_err());
        inserter.write(&block).unwrap();
        inserter.end().unwrap();
    }
    let rows =
        RowClient::query_blocks(client, &format!("SELECT id FROM {table} ORDER BY id")).unwrap();
    let ids: Vec<u64> = rows
        .iter()
        .flat_map(|block| block.column_slice::<u64>(0).unwrap().iter().copied())
        .collect();
    assert_eq!(ids, [3, 3]);
}

/// Early drop releases the borrowed client after complete response buffering.
fn buffered_stream_drop(client: &mut Client) {
    {
        let mut stream = client
            .select_http_stream(&Query::new("SELECT number FROM numbers(10000)"))
            .unwrap();
        let first = stream.next().unwrap().unwrap();
        assert!(first.row_count() > 0);
        assert_eq!(first.column_slice::<u64>(0).unwrap()[0], 0);
    }
    client.ping().unwrap();
    let mut stream = client
        .select_http_stream(&Query::new("SELECT number FROM numbers(7)"))
        .unwrap();
    let mut values = Vec::new();
    for block in stream.by_ref() {
        values.extend_from_slice(block.unwrap().column_slice::<u64>(0).unwrap());
    }
    assert_eq!(values, [0, 1, 2, 3, 4, 5, 6]);
    assert!(stream.next().is_none());
}

/// Production HTTP/2 delivers raw chunks and writes exact bounded caller buffers.
fn incremental_h2_read(client: &mut Client) {
    let query = Query::new("SELECT number FROM numbers(10000) FORMAT TabSeparated");
    let mut chunks = Vec::with_capacity(65536);
    let mut callbacks = 0;
    let count = client
        .query_raw_with(&query, |chunk| {
            callbacks += 1;
            chunks.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap();
    assert!(callbacks > 0);
    assert_eq!(count, chunks.len());
    assert_eq!(chunks, client.fetch_bytes(&query).unwrap());
    assert_eq!(
        client.negotiated_http_version(),
        Some(HttpVersionChoice::Http2PriorKnowledge)
    );
    let mut exact = [0_u8; 2];
    assert_eq!(
        client
            .query_raw_into_slice(&Query::new("SELECT 1 FORMAT TabSeparated"), &mut exact)
            .unwrap(),
        2
    );
    assert_eq!(&exact, b"1\n");
}
