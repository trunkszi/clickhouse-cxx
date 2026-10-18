//! Native TCP row APIs with Bool, Array, Nullable, and Map fields.

#[path = "support/docker.rs"]
mod docker;

use std::collections::BTreeMap;

use clickhouse::{
    Client, Endpoint, Row,
    row::{MapRef, RowClient},
};

#[derive(Row)]
#[row(table = "native_composite_events")]
struct CompositeEvent {
    id: u64,
    active: bool,
    tags: Vec<u32>,
    note: Option<String>,
    scores: BTreeMap<String, u64>,
}

#[test]
fn native_tcp_composite_row_roundtrip() {
    let server = docker::Server::start();
    let endpoint = Endpoint::parse(format!("tcp://{}", server.tcp_address)).expect("endpoint");
    let mut client = Client::connect(endpoint, "default", "", "default").expect("native connect");
    client.ping().expect("ping");
    client
        .execute(
            "CREATE TABLE native_composite_events (id UInt64, active Bool, tags Array(UInt32), \
             note Nullable(String), scores Map(String, UInt64)) ENGINE = Memory",
        )
        .expect("create table");

    let mut first_scores = BTreeMap::new();
    first_scores.insert(String::from("a"), 1_u64);
    first_scores.insert(String::from("b"), 2_u64);
    let mut batch = CompositeEventBatch::default();
    batch.push_ref(CompositeEventRef {
        id: 1,
        active: true,
        tags: &[1, 2],
        note: Some("hello"),
        scores: MapRef::BTree(&first_scores),
    });
    let empty_scores = BTreeMap::new();
    batch.push_ref(CompositeEventRef {
        id: 2,
        active: false,
        tags: &[],
        note: None,
        scores: MapRef::BTree(&empty_scores),
    });
    client.insert_batch(&batch).expect("insert batch");

    let rows: Vec<CompositeEvent> = client
        .query_rows(
            "SELECT id, active, tags, note, scores FROM native_composite_events ORDER BY id",
        )
        .expect("query rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert!(rows[0].active);
    assert_eq!(rows[0].tags, [1, 2]);
    assert_eq!(rows[0].note.as_deref(), Some("hello"));
    assert_eq!(rows[0].scores, first_scores);
    assert_eq!(rows[1].id, 2);
    assert!(!rows[1].active);
    assert!(rows[1].tags.is_empty());
    assert!(rows[1].note.is_none());
    assert!(rows[1].scores.is_empty());

    client
        .execute("DROP TABLE native_composite_events")
        .expect("drop table");
}
