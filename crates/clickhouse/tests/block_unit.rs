//! In-memory endpoint, block, and projection checks.

use clickhouse::{Block, Endpoint, Error, Scheme};

#[test]
fn endpoint_parses_host_and_port() {
    let endpoint = Endpoint::parse("tcp://127.0.0.1:9123").expect("endpoint");
    assert_eq!(endpoint.scheme(), Scheme::Tcp);
    assert_eq!(endpoint.host(), "127.0.0.1");
    assert_eq!(endpoint.port(), 9123);
}

#[test]
fn endpoint_defaults_to_native_port() {
    let endpoint = Endpoint::parse("tcp://localhost").expect("endpoint");
    assert_eq!(endpoint.port(), 9000);
}

#[test]
fn endpoint_rejects_non_tcp_scheme() {
    let error = Endpoint::parse("http://localhost:8123").expect_err("scheme");
    assert!(matches!(error, Error::UnsupportedScheme { .. }));
}

#[test]
fn endpoint_rejects_bad_port_and_path() {
    assert!(matches!(
        Endpoint::parse("tcp://localhost:not-a-port"),
        Err(Error::Endpoint { .. })
    ));
    assert!(matches!(
        Endpoint::parse("tcp://localhost:9000/path"),
        Err(Error::Endpoint { .. })
    ));
}

#[test]
fn block_round_trips_primitive_and_string_columns() {
    let mut block = Block::new();
    block.push_u64("id", &[1, 2, 3]).expect("push column");
    block.push_i64("delta", &[-2, 0, 2]).expect("push column");
    block
        .push_f64("price", &[10.5, 11.25, 12.0])
        .expect("push column");
    let symbols = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    block.push_string("symbol", &symbols).expect("push column");

    assert_eq!(block.rows(), 3);
    assert_eq!(block.columns(), 4);
    assert_eq!(block.column_slice::<u64>("id").expect("id"), [1, 2, 3]);
    assert_eq!(
        block.column_slice::<i64>("delta").expect("delta"),
        [-2, 0, 2]
    );
    assert_eq!(
        block.column_slice::<f64>("price").expect("price"),
        [10.5, 11.25, 12.0]
    );
    assert_eq!(block.column_strings("symbol").expect("symbol"), symbols);
}

#[test]
fn block_clear_retains_capacity() {
    let mut block = Block::new();
    block.push_u64("id", &[0; 128]).expect("push column");
    let capacity = block.column_capacity("id").expect("capacity");
    assert!(capacity >= 128);

    block.clear();
    assert_eq!(block.rows(), 0);
    assert_eq!(block.column_capacity("id").expect("capacity"), capacity);

    block.push_u64("id", &[7, 8]).expect("push column");
    assert_eq!(block.column_slice::<u64>("id").expect("id"), [7, 8]);
}

#[test]
fn block_reports_missing_and_mismatched_columns() {
    let mut block = Block::new();
    block.push_u64("id", &[1]).expect("push column");
    assert!(matches!(
        block.column_slice::<u64>("missing"),
        Err(Error::Block { .. })
    ));
    assert!(matches!(
        block.column_slice::<i64>("id"),
        Err(Error::Block { .. })
    ));
}
