//! Thread-local clients and a shared worker pool over Native TCP.
//!
//! The transport handles stay single-threaded. `ThreadLocalClient` keeps one
//! client per OS thread, while `WorkerClient` is a `Send + Sync` handle that
//! leases one worker id per thread and balances calls across local connection
//! slots inside that thread.
//!
//! Run with:
//!
//! ```text
//! CLICKHOUSE_URL=tcp://127.0.0.1:9000 \
//!   cargo run -p clickhouse --example 07_thread_local_and_workers
//! ```
//!
//! `CLICKHOUSE_URL` defaults to `tcp://127.0.0.1:9000`. When the server is
//! unreachable the example prints a notice and exits successfully.

use std::{env, thread};

use clickhouse::{Block, Client, ClientConfig, Endpoint, ThreadLocalClient, WorkerBalance};

const DEFAULT_ADDR: &str = "tcp://127.0.0.1:9000";

fn unavailable(address: &str, error: impl std::fmt::Display) {
    println!("ClickHouse server unavailable at {address}: {error}");
    println!("Start a server or set CLICKHOUSE_NATIVETCP_ADDR, then rerun.");
}

fn config(endpoint: &Endpoint) -> Result<ClientConfig<'static>, Box<dyn std::error::Error>> {
    Ok(ClientConfig::from_endpoints(vec![endpoint.clone()])?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = env::var("CLICKHOUSE_URL")
        .or_else(|_| env::var("CLICKHOUSE_NATIVETCP_ADDR"))
        .unwrap_or_else(|_| String::from(DEFAULT_ADDR));
    let endpoint = Endpoint::parse(address.clone())?;

    // ---------------------------------------------------------------------
    // 1. Thread-local clients.
    //
    // The handle itself is `Send + Sync`; every OS thread gets its own
    // single-threaded `Client`, created on first use and reused afterwards.
    // ---------------------------------------------------------------------
    let local: ThreadLocalClient = match Client::thread_local(config(&endpoint)?) {
        Ok(local) => local,
        Err(error) => {
            unavailable(&address, error);
            return Ok(());
        }
    };
    if let Err(error) = local.with(|client| client.execute("SELECT 1")) {
        unavailable(&address, error);
        return Ok(());
    }
    let endpoints = local.endpoints();
    println!(
        "thread-local endpoints={:?}",
        endpoints
            .iter()
            .map(|endpoint| format!("{}:{}", endpoint.host(), endpoint.port()))
            .collect::<Vec<_>>()
    );

    println!("thread-local clients (4 scoped threads)");
    thread::scope(|scope| {
        for index in 0..4 {
            let local = &local;
            scope.spawn(move || {
                for call in 0..2 {
                    local
                        .with(|client| {
                            client.execute("SELECT 1")?;
                            println!(
                                "  thread {index} call {call} client={:#x}",
                                client as *mut Client as usize
                            );
                            Ok(())
                        })
                        .expect("thread-local SELECT 1");
                }
            });
        }
    });

    // ---------------------------------------------------------------------
    // 2. Round-robin worker pool.
    //
    // Every thread leases one worker id. Calls rotate through that thread's
    // local slots; the worker id also offsets the initial endpoint order.
    // ---------------------------------------------------------------------
    let round_robin = Client::connect_workers(config(&endpoint)?.with_workers(4))?;
    println!("\nround-robin pool workers={}", round_robin.workers());
    for _ in 0..8 {
        round_robin.execute("SELECT 1")?;
    }
    let blocks = round_robin.query_blocks("SELECT number FROM numbers(16)")?;
    let rows: usize = blocks.iter().map(Block::row_count).sum();
    println!("  execute calls=8, query rows={rows}");
    println!("  dispatch_counts={:?}", round_robin.dispatch_counts());

    // ---------------------------------------------------------------------
    // 3. Least-in-flight worker pool.
    //
    // Ties rotate so every local slot receives work even though synchronous
    // calls never overlap on one thread.
    // ---------------------------------------------------------------------
    let least = Client::connect_workers(
        config(&endpoint)?
            .with_workers(4)
            .with_balance(WorkerBalance::LeastInFlight),
    )?;
    thread::scope(|scope| {
        for _ in 0..4 {
            let least = &least;
            scope.spawn(move || {
                for _ in 0..8 {
                    let blocks = least
                        .query_blocks("SELECT number FROM numbers(64)")
                        .expect("least-in-flight query");
                    let rows: usize = blocks.iter().map(Block::row_count).sum();
                    assert_eq!(rows, 64, "each query returns the full result");
                }
            });
        }
    });
    println!(
        "\nleast-in-flight dispatch_counts={:?}",
        least.dispatch_counts()
    );

    // ---------------------------------------------------------------------
    // 4. Key-hash worker pool.
    //
    // Every call carrying the same key reaches the same local slot, which
    // keeps per-key ordering for callers sharing the handle.
    // ---------------------------------------------------------------------
    let keyed = Client::connect_workers(
        config(&endpoint)?
            .with_workers(4)
            .with_balance(WorkerBalance::KeyHash),
    )?;
    for _ in 0..8 {
        keyed.execute_keyed(7, "SELECT 1")?;
    }
    println!("keyed dispatch_counts={:?}", keyed.dispatch_counts());

    // ---------------------------------------------------------------------
    // 5. Row-layer blocks through the pool.
    //
    // A row-layer `Block` is `Send`; the selected local client converts it to
    // the native transport representation on the calling thread.
    // ---------------------------------------------------------------------
    least.execute(
        "CREATE OR REPLACE TABLE example_threading_events (id UInt64, name String) ENGINE = Memory",
    )?;
    let mut block = Block::new();
    block.append_fixed_slice("id", "UInt64", &[1_u64, 2])?;
    block.append_strings_flat("name", &[3, 3], b"abc")?;
    least.insert_block("example_threading_events", block)?;
    let blocks = least.query_blocks("SELECT id, name FROM example_threading_events ORDER BY id")?;
    for result in &blocks {
        let ids: &[u64] = result.column_slice_by_name("id")?;
        println!("table block rows={} ids={ids:?}", result.row_count());
    }

    // ---------------------------------------------------------------------
    // 6. Caller-directed reads.
    //
    // `query_blocks_for_each` delivers blocks without retaining a Vec;
    // `query_blocks_into` appends to storage supplied by the caller;
    // Project a fixed-width column and copy its prefix into caller memory.
    // ---------------------------------------------------------------------
    let mut plain = Client::new(config(&endpoint)?)?;
    let mut destination: Vec<Block> = Vec::new();
    let appended = plain.query_blocks_into("SELECT number FROM numbers(16)", &mut destination)?;
    let mut delivered = 0;
    let callbacks = plain.query_blocks_for_each("SELECT number FROM numbers(16)", |block| {
        delivered += block.row_count();
        Ok(())
    })?;
    let mut values = [0_u64; 4];
    let data = destination
        .iter()
        .find(|block| block.row_count() > 0)
        .expect("one nonempty result block");
    let source: &[u64] = data.column_slice(0)?;
    let copied = source.len().min(values.len());
    values[..copied].copy_from_slice(&source[..copied]);
    println!(
        "\nread path appended_blocks={appended} callback_blocks={callbacks} \
         callback_rows={delivered} copied={copied} values={values:?}"
    );

    Ok(())
}
