//! E2E scenarios distinguish server rejections from broken Native transport sessions.
#![cfg(feature = "tcp")]

use std::{cell::Cell, time::Duration};

use clickhouse::{
    Client, ClientConfig, Compression, Error, FailoverPolicy, IMPORTANT, NativeBlock, Query,
    TracingContext,
};
#[path = "support/disconnecting_proxy.rs"]
mod disconnecting_proxy;
#[path = "support/docker.rs"]
mod docker;

/// Server INSERT rejections retain structured details and never open transport circuits.
#[test]
fn e2e_server_insert_rejection_keeps_health() {
    let server = docker::Server::start();
    let config = ClientConfig::parse(&format!("tcp://{}", server.tcp_address))
        .expect("valid Native endpoint")
        .with_policy(FailoverPolicy::new().with_circuit(1, Duration::from_secs(60)));
    let mut client = Client::with_config(config).expect("live Native handshake");
    borrowed_query_metadata(&mut client);
    serde_query_metadata(&mut client);
    for query in [
        Query::new("SELECT 1").compression(Compression::None),
        Query::new("SELECT 1").deduplication_token("native-reject"),
    ] {
        assert!(matches!(
            client.execute_query(&query),
            Err(Error::Unsupported(_))
        ));
    }
    for query in [
        Query::new("INSERT INTO nonexistent VALUES").compression(Compression::None),
        Query::new("INSERT INTO nonexistent VALUES").deduplication_token("native-reject"),
    ] {
        assert!(matches!(client.inserter(query), Err(Error::Unsupported(_))));
    }
    assert_eq!(client.endpoint_health()[0].failures, 0);

    let error = match client.inserter("INSERT INTO missing_insert_table VALUES") {
        Ok(_) => panic!("missing table unexpectedly accepted INSERT"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Server(_)), "{error}");
    assert_eq!(client.endpoint_health()[0].failures, 0);
    assert!(!client.endpoint_health()[0].circuit_open);
    assert_eq!(client.query_u64("SELECT toUInt64(7)").unwrap(), 7);
    client
        .execute(
            "CREATE TABLE constrained_insert (id UInt64, CONSTRAINT positive CHECK id > 0) \
             ENGINE=Memory",
        )
        .unwrap();
    let mut insert = client
        .inserter("INSERT INTO constrained_insert VALUES")
        .unwrap();
    let mut block = NativeBlock::new();
    block.append_u64("id", &[0]).unwrap();
    insert.send_block(&block).unwrap();
    let error = insert.end().expect_err("constraint must reject the row");
    assert!(matches!(error, Error::Server(_)), "{error}");
    assert_eq!(client.endpoint_health()[0].failures, 0);
    assert!(!client.endpoint_health()[0].circuit_open);
    assert_eq!(client.query_u64("SELECT toUInt64(9)").unwrap(), 9);
    let block = clickhouse::Block::from_native(&block).unwrap();
    let error = client
        .insert_block("constrained_insert", &block)
        .unwrap_err();
    assert!(matches!(error, Error::Server(_)), "{error}");
    assert_eq!(client.endpoint_health()[0].failures, 0);
    assert!(!client.endpoint_health()[0].circuit_open);
    assert_eq!(client.query_u64("SELECT toUInt64(13)").unwrap(), 13);
    client.reset_connection_endpoint().unwrap();
    assert!(client.is_usable());
    assert!(client.take_cleanup_error().is_none());
    assert_eq!(client.query_u64("SELECT toUInt64(19)").unwrap(), 19);
}

/// A dropped socket opens its circuit before a healthy alternate handshake completes.
#[test]
fn e2e_stream_failure_opens_before_reconnect() {
    let server = docker::Server::start();
    let port = server
        .tcp_address
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let mut proxy = disconnecting_proxy::DisconnectingProxy::start(port).unwrap();
    let config = ClientConfig::parse(&format!(
        "tcp://127.0.0.1:{},tcp://{}",
        proxy.port, server.tcp_address
    ))
    .unwrap()
    .with_policy(FailoverPolicy::new().with_circuit(1, Duration::from_secs(60)));
    let mut client = Client::with_config(config).unwrap();
    proxy.disconnect().unwrap();
    assert!(client.execute("SELECT 1").is_err());
    let health = &client.endpoint_health()[0];
    assert_eq!(health.failures, 1);
    assert!(health.circuit_open);
    assert_eq!(client.query_u64("SELECT toUInt64(11)").unwrap(), 11);
    assert_eq!(client.endpoint_health()[0].failures, 1);
    assert!(client.endpoint_health()[0].circuit_open);
    assert_eq!(client.endpoint_health()[1].failures, 0);
}

/// Repeated borrowed execution preserves parameters, settings, identity, tracing, and callbacks.
fn borrowed_query_metadata(client: &mut Client) {
    let progress_rows = Cell::new(0);
    let query = Query::new(
        "SELECT throwIf(queryID() != 'borrowed-native-query' OR getSetting('max_threads') != 1), \
         number FROM numbers({n:UInt64})",
    )
    .with_id("borrowed-native-query")
    .with_param("n", "3")
    .with_setting("max_threads", "1", IMPORTANT)
    .with_tracing(TracingContext {
        trace_id: 0x00112233445566778899aabbccddeeff,
        span_id: 987654321,
        tracestate: "sample=borrowed".into(),
        trace_flags: 1,
    })
    .on_progress(|progress| progress_rows.set(progress_rows.get() + progress.rows));
    client.execute_query(&query).unwrap();
    let first_rows = progress_rows.get();
    assert!(first_rows >= 3, "first execution must invoke the callback");
    client.execute_query(&query).unwrap();
    assert!(
        progress_rows.get() >= first_rows + 3,
        "second execution must reuse the original callback"
    );
    client.execute("SYSTEM FLUSH LOGS").unwrap();
    assert_eq!(
        client
            .query_u64(
                "SELECT count() FROM system.opentelemetry_span_log WHERE \
                 trace_id=toUUID('00112233-4455-6677-8899-aabbccddeeff') AND \
                 parent_span_id=987654321",
            )
            .unwrap(),
        2,
        "each borrowed execution must preserve its tracing context"
    );
}

/// Serde reads preserve borrowed query metadata instead of forwarding only the SQL text.
fn serde_query_metadata(client: &mut Client) {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct MetadataRow {
        guard: u8,
        value: u64,
    }

    let query = Query::new(
        "SELECT throwIf(queryID() != 'borrowed-serde-query' OR getSetting('max_threads') != 1) AS \
         guard, toUInt64({value:UInt64}) AS value",
    )
    .with_id("borrowed-serde-query")
    .with_param("value", "37")
    .with_setting("max_threads", "1", IMPORTANT);
    let rows: Vec<MetadataRow> = client.serde_query_rows(&query).unwrap();
    assert_eq!(
        rows,
        vec![MetadataRow {
            guard: 0,
            value: 37
        }]
    );
}
