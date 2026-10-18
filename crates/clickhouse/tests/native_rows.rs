//! Native TCP row APIs exercised against a live ClickHouse container.

#[path = "support/docker.rs"]
mod docker;

use clickhouse::{BufferPolicy, Client, Endpoint, Row, row::RowClient};

#[derive(Row)]
#[row(table = "native_events")]
struct NativeEvent {
    id: u64,
    delta: i64,
    price: f64,
    symbol: String,
    #[row(bytes)]
    payload: Vec<u8>,
}

fn event(id: u64, delta: i64, price: f64, symbol: &str, payload: &[u8]) -> NativeEvent {
    NativeEvent {
        id,
        delta,
        price,
        symbol: String::from(symbol),
        payload: payload.to_vec(),
    }
}

#[test]
fn native_tcp_row_roundtrip() {
    let server = docker::Server::start();
    let endpoint = Endpoint::parse(format!("tcp://{}", server.tcp_address)).expect("endpoint");
    let mut client = Client::connect(endpoint, "default", "", "default").expect("native connect");
    client.ping().expect("ping");
    client
        .execute(
            "CREATE TABLE native_events (id UInt64, delta Int64, price Float64, symbol String, \
             payload String) ENGINE = Memory",
        )
        .expect("create table");

    let mut batch = NativeEventBatch::with_capacity(2);
    // A burst is submitted before trimming; the next batch reuses warm capacity.
    for id in 0..131_072 {
        batch.push_ref(NativeEventRef {
            id,
            delta: -1,
            price: 10.5,
            symbol: "BTC",
            payload: &[0, 255, 7],
        });
    }
    let peak = batch.allocated_bytes();
    client.insert_batch(&batch).expect("insert burst");
    let mut count = 0;
    client
        .query_views::<NativeEvent, _>(
            "SELECT id, delta, price, symbol, payload FROM native_events",
            |view| {
                for row in view.iter() {
                    assert_eq!(row.delta, -1);
                    assert_eq!(row.payload, &[0, 255, 7]);
                    count += 1;
                }
                Ok(())
            },
        )
        .expect("read burst");
    assert_eq!(count, 131_072);
    batch.clear();
    assert!(batch.allocated_bytes() < peak);
    assert!(batch.allocated_bytes() <= BufferPolicy::default().max_retained_bytes);
    assert!(batch.id.capacity() >= 4096);
    client
        .execute("TRUNCATE TABLE native_events")
        .expect("truncate burst");
    batch.push_ref(NativeEventRef {
        id: 1,
        delta: -3,
        price: 10.5,
        symbol: "BTC",
        payload: &[0, 255, 7],
    });
    let capacity = (batch.id.capacity(), batch.symbol.bytes.capacity());
    batch.clear();
    assert_eq!(batch.id.capacity(), capacity.0);
    assert_eq!(batch.symbol.bytes.capacity(), capacity.1);
    batch.push_ref(NativeEventRef {
        id: 2,
        delta: 0,
        price: 11.25,
        symbol: "ETH",
        payload: &[],
    });
    client.insert_batch(&batch).expect("insert batch");
    client
        .insert_rows(&[
            event(3, 5, 12.75, "WLD", &[9]),
            event(4, -1, 13.0, "BTC", &[1, 2]),
        ])
        .expect("insert rows");

    let rows: Vec<NativeEvent> = client
        .query_rows("SELECT id, delta, price, symbol, payload FROM native_events ORDER BY id")
        .expect("query rows");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id, 2);
    assert_eq!(rows[0].delta, 0);
    assert_eq!(rows[0].price, 11.25);
    assert_eq!(rows[0].symbol, "ETH");
    assert!(rows[0].payload.is_empty());
    assert_eq!(rows[1].symbol, "WLD");
    assert_eq!(rows[1].payload, [9]);
    assert_eq!(rows[2].payload, [1, 2]);

    let mut visited = Vec::new();
    client
        .query_views::<NativeEvent, _>(
            "SELECT id, delta, price, symbol, payload FROM native_events ORDER BY id",
            |view| {
                for row in view.iter() {
                    visited.push((row.id, String::from(row.symbol), row.payload.to_vec()));
                }
                Ok(())
            },
        )
        .expect("query views");
    assert_eq!(visited.len(), 3);
    assert_eq!(
        visited,
        vec![
            (2, String::from("ETH"), vec![]),
            (3, String::from("WLD"), vec![9]),
            (4, String::from("BTC"), vec![1, 2]),
        ]
    );

    client
        .execute("DROP TABLE native_events")
        .expect("drop table");
}
