//! End-to-end SDK lifecycle and scenario tests against an isolated real server.
//! Public Client calls drive the Native protocol through to server-side results;
//! Docker supplies the real database dependency without replacing protocol behavior.

mod support;

use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
};

use clickhouse::{
    Block, CUSTOM, Client, CompressionMethod, Error, ExternalTable, IMPORTANT, NativeBlock,
    OBSOLETE, Query, Row, TracingContext,
};
use support::TestClickHouseContainer;

fn block(values: &[u64]) -> NativeBlock {
    let mut block = NativeBlock::new();
    block.append_u64("id", values).unwrap();
    block
}

fn native_values(blocks: &[NativeBlock], column: usize) -> Vec<u64> {
    blocks
        .iter()
        .flat_map(|block| {
            let name = block.column_name(column);
            block.column_slice::<u64>(&name).unwrap().iter().copied()
        })
        .collect()
}

/// Read the selected UInt64 column from shared facade result blocks.
fn values(blocks: &[Block], column: usize) -> Vec<u64> {
    blocks
        .iter()
        .flat_map(|block| block.column_slice::<u64>(column).unwrap().iter().copied())
        .collect()
}

#[test]
fn lifecycle_early_drop_drains_before_reuse() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);
    {
        let mut stream = client
            .select_stream("SELECT number FROM numbers(100000000) SETTINGS max_block_size=128")
            .unwrap();
        assert!(stream.is_selecting());
        let mut seen = 0;
        for block in stream.by_ref() {
            seen += block.unwrap().rows();
            if seen > 0 {
                break;
            }
        }
        assert!(seen > 0 && seen < 100000000);
    }
    assert!(client.take_cleanup_error().is_none());
    client.ping().unwrap();
    assert_eq!(
        values(&client.query_blocks("SELECT toUInt64(42)").unwrap(), 0),
        [42]
    );
    let mut stream = client
        .select_stream("SELECT number FROM numbers(1000000)")
        .unwrap();
    stream.cancel().unwrap();
    stream.cancel().unwrap();
    assert!(!stream.is_selecting());
    assert!(stream.next().is_none());
}

#[test]
fn scenario_cancelable_stops_on_first_data_block() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::Lz4);
    let mut rows = 0;
    client
        .select_cancelable(
            "SELECT number FROM numbers(100000000) SETTINGS max_block_size=64",
            |b| {
                rows += b.rows();
                b.rows() == 0
            },
        )
        .unwrap();
    assert!(rows > 0 && rows < 100000000);
    client.ping().unwrap();
}

#[derive(Row)]
#[row(table = "data")]
struct IdRow {
    id: u64,
}

#[test]
fn lifecycle_multi_block_and_batch_insert() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::Zstd);
    client
        .execute("CREATE TABLE data (id UInt64) ENGINE=Memory")
        .unwrap();
    let mut insert = client.inserter("INSERT INTO data (id) VALUES").unwrap();
    assert!(insert.is_inserting());
    assert_eq!(insert.schema_block().column_name(0), "id");
    insert.send_block(&block(&[1, 2])).unwrap();
    let mut batch = IdRowBatch::with_capacity(2);
    batch.push(&IdRow { id: 3 });
    batch.push(&IdRow { id: 4 });
    insert.send_batch(&batch).unwrap();
    let mut wrong = NativeBlock::new();
    wrong.append_f64("id", &[5.0]).unwrap();
    assert!(matches!(insert.send_block(&wrong), Err(Error::Invalid(_))));
    insert.send_block(&block(&[5])).unwrap();
    insert.end().unwrap();
    assert_eq!(
        values(
            &client
                .query_blocks("SELECT id FROM data ORDER BY id")
                .unwrap(),
            0
        ),
        [1, 2, 3, 4, 5]
    );
    {
        let mut abandoned = client.inserter("INSERT INTO data (id) VALUES").unwrap();
        abandoned.send_block(&block(&[7])).unwrap();
    }
    assert!(client.take_cleanup_error().is_none());
    client.ping().unwrap();
    let mut insert = client.inserter("INSERT INTO data (id) VALUES").unwrap();
    insert.send_block(&block(&[99])).unwrap();
    insert.end().unwrap();
    // Disconnecting an INSERT does not promise rollback of already sent blocks.
    assert_eq!(
        values(
            &client
                .query_blocks("SELECT countIf(id=99) FROM data")
                .unwrap(),
            0
        ),
        [1]
    );
}

#[test]
fn scenario_external_join_and_early_cancel() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::Lz4);
    client
        .execute("CREATE TABLE data (id UInt64) ENGINE=Memory")
        .unwrap();
    let mut insert = client.inserter("INSERT INTO data (id) VALUES").unwrap();
    insert.send_block(&block(&[1, 2, 3, 4])).unwrap();
    insert.end().unwrap();
    let external = block(&[2, 4]);
    let tables = [ExternalTable {
        name: Cow::Borrowed("chosen"),
        data: &external,
    }];
    let query = Query::new("SELECT d.id FROM data d INNER JOIN chosen c USING (id) ORDER BY id")
        .with_id("external-join")
        .with_setting("max_block_size", "2", IMPORTANT);
    let result = client.select_with_external_data(query, &tables).unwrap();
    assert_eq!(native_values(&result, 0), [2, 4]);
    let mut seen = 0;
    client
        .select_with_external_data_cancelable(
            "SELECT source.number FROM numbers(100000000) AS source CROSS JOIN chosen SETTINGS \
             max_block_size=64",
            &tables,
            |b| {
                seen += b.rows();
                b.rows() == 0
            },
        )
        .unwrap();
    assert!(seen > 0 && seen < 100000000);
    client.ping().unwrap();
    assert_eq!(
        values(
            &client
                .query_blocks("SELECT count() FROM system.tables WHERE name='chosen'")
                .unwrap(),
            0
        ),
        [0]
    );
}

#[test]
fn scenario_metadata_events_and_server_exception() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);
    assert_eq!(client.get_server_info().version_major, 26);
    assert!(Client::get_version().major > 0);
    let progress = Cell::new(0);
    let profiles = Cell::new(0);
    let profile_events = Cell::new(0);
    let logs = Cell::new(0);
    let query = Query::new("SELECT number FROM numbers(20000)")
        .with_id("telemetry-roundtrip")
        .with_setting("max_block_size", "256", IMPORTANT | OBSOLETE)
        .with_setting("SQL_streaming_custom", "UInt64_1", CUSTOM)
        .with_setting("send_profile_events", "1", IMPORTANT)
        .with_setting("send_logs_level", "trace", IMPORTANT)
        .on_progress(|p| progress.set(progress.get() + p.rows))
        .on_profile(|p| profiles.set(profiles.get() + p.rows))
        .on_profile_events(|b| profile_events.set(profile_events.get() + b.rows()))
        .on_server_log(|b| logs.set(logs.get() + b.rows()));
    assert_eq!(values(&client.query_blocks(query).unwrap(), 0).len(), 20000);
    assert!(progress.get() >= 20000);
    assert!(profiles.get() >= 20000);
    assert!(profile_events.get() > 0);
    assert!(logs.get() > 0);
    let exceptions = RefCell::new(Vec::new());
    let query = Query::new("SELECT missing_streaming_column")
        .on_exception(|e| exceptions.borrow_mut().push(e.code));
    assert!(matches!(client.execute(query), Err(Error::Server(_))));
    assert_eq!(exceptions.borrow().len(), 1);
    assert!(exceptions.borrow()[0] > 0);
    client.ping().unwrap();
    let insert = Query::new("INSERT INTO absent VALUES").on_progress(|_| {});
    assert!(matches!(
        client.inserter(insert),
        Err(Error::InsertEventCallbacks)
    ));
    client.ping().unwrap();
}

#[test]
fn scenario_parameters_query_id_and_tracing() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);
    let query = Query::new(
        "SELECT toUInt64({n:UInt64}), toUInt64(isNull({p:Nullable(String)})), \
         length({empty:String}), toUInt64(queryID()='traced-query')",
    )
    .with_id("traced-query")
    .with_param("n", "42")
    .with_null_param("p")
    .with_param("empty", "")
    .with_tracing(TracingContext {
        trace_id: 0x00112233445566778899aabbccddeeff,
        span_id: 123456789,
        tracestate: "sample=streaming".into(),
        trace_flags: 1,
    });
    let result = client.query_blocks(query).unwrap();
    assert_eq!(values(&result, 0), [42]);
    assert_eq!(values(&result, 1), [1]);
    assert_eq!(values(&result, 2), [0]);
    assert_eq!(values(&result, 3), [1]);
    client.execute("SYSTEM FLUSH LOGS").unwrap();
    assert_eq!(
        values(
            &client
                .query_blocks(
                    "SELECT count() FROM system.opentelemetry_span_log WHERE \
                     trace_id=toUUID('00112233-4455-6677-8899-aabbccddeeff') AND \
                     parent_span_id=123456789"
                )
                .unwrap(),
            0
        ),
        [1]
    );
}

#[test]
fn scenario_compression_reads_and_writes() {
    let server = TestClickHouseContainer::start();
    for codec in [
        CompressionMethod::None,
        CompressionMethod::Lz4,
        CompressionMethod::Zstd,
    ] {
        let mut client = server.client(codec);
        client.execute("DROP TABLE IF EXISTS compressed").unwrap();
        client
            .execute("CREATE TABLE compressed (id UInt64) ENGINE=Memory")
            .unwrap();
        let input: Vec<_> = (0..10000).collect();
        let mut insert = client.inserter("INSERT INTO compressed VALUES").unwrap();
        insert.send_block(&block(&input)).unwrap();
        insert.end().unwrap();
        assert_eq!(
            values(
                &client
                    .query_blocks("SELECT id FROM compressed ORDER BY id")
                    .unwrap(),
                0
            ),
            input
        );
    }
}

#[test]
fn scenario_callback_panic_does_not_cross_ffi() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);
    let query = Query::new("SELECT number FROM numbers(20000)")
        .on_progress(|_| panic!("controlled callback failure"));
    assert!(matches!(
        client.execute(query),
        Err(Error::CallbackPanicked)
    ));
    client.ping().unwrap();
    let mut calls = 0;
    let result =
        client.select_with_external_data_cancelable("SELECT number FROM numbers(100)", &[], |_| {
            calls += 1;
            panic!("controlled external callback failure");
        });
    assert!(matches!(result, Err(Error::CallbackPanicked)));
    assert!(calls > 0);
    client.ping().unwrap();
}

#[test]
fn lifecycle_failed_drop_is_observable_and_poisoned() {
    let mut server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);
    let mut stream = client
        .select_stream("SELECT number FROM numbers(100000000) SETTINGS max_block_size=64")
        .unwrap();
    while stream.next().unwrap().unwrap().rows() == 0 {}
    server.terminate();
    drop(stream);
    assert!(client.take_cleanup_error().is_some());
    assert!(!client.is_usable());
    assert!(matches!(client.ping(), Err(Error::ConnectionPoisoned)));
}

#[test]
fn read_paths_deliver_into_caller_storage() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);

    let mut destination = Vec::new();
    let appended = client
        .query_blocks_into("SELECT number FROM numbers(1000)", &mut destination)
        .unwrap();
    assert_eq!(appended, destination.len());
    let rows: usize = destination.iter().map(Block::row_count).sum();
    assert_eq!(rows, 1000, "caller storage receives every row");
    let first: Vec<u64> = destination
        .iter()
        .flat_map(|block| block.column_slice::<u64>(0).unwrap().iter().copied())
        .collect();
    assert_eq!(first.len(), 1000);

    let mut delivered = 0;
    let callbacks = client
        .query_blocks_for_each("SELECT number FROM numbers(1000)", |block| {
            delivered += block.row_count();
            Ok(())
        })
        .unwrap();
    assert!(callbacks > 0, "at least one block is delivered");
    assert_eq!(delivered, 1000, "callbacks receive every row");

    let error = client
        .query_blocks_for_each("SELECT number FROM numbers(100000000)", |_| {
            Err(Error::Invalid("stop after the first block".into()))
        })
        .unwrap_err();
    assert!(matches!(error, Error::Invalid(_)));
    client.ping().unwrap();
}

#[test]
fn copy_column_into_slice_uses_caller_memory() {
    let server = TestClickHouseContainer::start();
    let mut client = server.client(CompressionMethod::None);
    let blocks = client
        .select_stream("SELECT toUInt64(number) AS id FROM numbers(5)")
        .unwrap()
        .collect::<clickhouse::Result<Vec<_>>>()
        .unwrap();
    let block = blocks
        .iter()
        .find(|block| block.rows() == 5)
        .expect("one result block");
    assert_eq!(block.rows(), 5);

    let mut values = [99_u64; 8];
    let copied = block.copy_column_into_slice(0, &mut values).unwrap();
    assert_eq!(copied, 5);
    assert_eq!(&values[..5], &[0, 1, 2, 3, 4]);
    assert_eq!(&values[5..], &[99, 99, 99], "tail stays caller-owned");

    let mut small = [7_u64; 2];
    assert_eq!(block.copy_column_into_slice(0, &mut small).unwrap(), 2);
    assert_eq!(small, [0, 1]);

    let mut wrong = [0_u32; 8];
    assert!(block.copy_column_into_slice(0, &mut wrong).is_err());
    assert!(block.copy_column_into_slice(9, &mut values).is_err());
}
