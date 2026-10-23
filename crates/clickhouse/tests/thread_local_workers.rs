//! Thread-local clients and locally balanced worker slots against a live server.
//!
//! The transport handles stay single-threaded. These tests exercise the two
//! public escape hatches: one client per OS thread, and a `Send + Sync` handle
//! that leases one worker id per thread and balances calls across that thread's
//! own local connection slots.

#[path = "support/docker.rs"]
mod docker;

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use clickhouse::{
    Block, Client, ClientConfig, Endpoint, Error, ThreadLocalClient, WorkerBalance, WorkerClient,
};

/// Compile-time proof that the pool handles can cross thread boundaries.
fn assert_send_sync<T: Send + Sync>() {}

fn endpoint(server: &docker::Server) -> Endpoint {
    Endpoint::parse(format!("tcp://{}", server.tcp_address)).expect("valid native endpoint")
}

fn config(server: &docker::Server) -> ClientConfig<'static> {
    ClientConfig::from_endpoints(vec![endpoint(server)]).expect("endpoint list")
}

#[test]
fn pool_handles_are_send_and_sync() {
    assert_send_sync::<ThreadLocalClient>();
    assert_send_sync::<WorkerClient>();
    assert_send_sync::<Block>();
}

#[test]
fn thread_local_client_keeps_one_client_per_os_thread() {
    let server = docker::Server::start();
    let local = Client::thread_local(config(&server)).expect("thread-local client");
    let barrier = AtomicUsize::new(0);
    let pointers = thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let first = local
                        .with(|client| {
                            client.execute("SELECT 1")?;
                            Ok(client as *mut Client as usize)
                        })
                        .expect("first call");
                    rendezvous(&barrier, 4);
                    let second = local
                        .with(|client| {
                            client.execute("SELECT 1")?;
                            Ok(client as *mut Client as usize)
                        })
                        .expect("second call");
                    assert_eq!(first, second);
                    first
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("client thread"))
            .collect::<Vec<_>>()
    });
    let distinct: HashSet<_> = pointers.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        4,
        "each live thread must own a distinct client: {pointers:?}"
    );
}

#[test]
fn round_robin_spreads_execute_evenly() {
    let server = docker::Server::start();
    let client = Client::connect_workers(config(&server).with_workers(4)).expect("worker pool");
    assert_eq!(client.workers(), 4);

    // The lease bounds concurrent worker threads to `workers`.
    thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..8 {
                    client.execute("SELECT 1").expect("round-robin SELECT 1");
                }
            });
        }
    });

    let counts = client.dispatch_counts();
    assert_eq!(counts, vec![8; 4], "counts={counts:?}");
}

#[test]
fn least_in_flight_keeps_every_worker_busy() {
    let server = docker::Server::start();
    let client = Client::connect_workers(
        config(&server)
            .with_workers(4)
            .with_balance(WorkerBalance::LeastInFlight),
    )
    .expect("worker pool");
    assert_eq!(client.workers(), 4);

    thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..8 {
                    let blocks = client
                        .query_blocks("SELECT number FROM numbers(64)")
                        .expect("least-in-flight query");
                    let rows: usize = blocks.iter().map(Block::row_count).sum();
                    assert_eq!(rows, 64, "each query returns the full result");
                }
            });
        }
    });

    let counts = client.dispatch_counts();
    assert_eq!(counts, vec![8; 4], "counts={counts:?}");
}

#[test]
fn key_hash_routes_one_key_to_one_worker() {
    let server = docker::Server::start();
    let client = Client::connect_workers(
        config(&server)
            .with_workers(4)
            .with_balance(WorkerBalance::KeyHash),
    )
    .expect("worker pool");
    assert_eq!(client.workers(), 4);

    for _ in 0..8 {
        client.execute_keyed(7, "SELECT 1").expect("keyed SELECT 1");
    }

    let counts = client.dispatch_counts();
    assert_eq!(counts.len(), 4, "counts={counts:?}");
    assert_eq!(
        counts.iter().filter(|&&count| count == 8).count(),
        1,
        "all keyed calls must land on one worker: {counts:?}"
    );
    assert_eq!(
        counts.iter().filter(|&&count| count == 0).count(),
        3,
        "the remaining workers must stay idle: {counts:?}"
    );
}

#[test]
fn worker_slot_lease_is_bounded_and_released_on_thread_exit() {
    let server = docker::Server::start();
    let client =
        Arc::new(Client::connect_workers(config(&server).with_workers(1)).expect("worker pool"));
    let ready = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let ready_tx = Arc::clone(&ready);
    let done_rx = Arc::clone(&done);

    let holder = Arc::clone(&client);
    let thread = thread::spawn(move || {
        holder
            .with(|_| {
                ready_tx.store(true, Ordering::Release);
                wait_flag(&done_rx);
                Ok(())
            })
            .expect("holder call");
    });
    wait_flag(&ready);
    let error = client.with(|_| Ok(())).expect_err("slot must be exhausted");
    match error {
        Error::WorkerSlotsExhausted { workers } => assert_eq!(workers, 1),
        other => panic!("expected WorkerSlotsExhausted, got {other:?}"),
    }
    done.store(true, Ordering::Release);
    thread.join().expect("holder thread");

    client
        .with(|_| Ok(()))
        .expect("the exited thread must release its worker slot");
}

#[test]
fn worker_ids_stripe_initial_endpoints() {
    let first = docker::Server::start();
    let second = docker::Server::start();
    let client = Client::connect_workers(
        ClientConfig::from_endpoints(vec![endpoint(&first), endpoint(&second)])
            .expect("endpoint list")
            .with_workers(2),
    )
    .expect("worker pool");
    let barrier = AtomicUsize::new(0);
    let seen = thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    client
                        .with(|client| {
                            let endpoint = format!(
                                "{}:{}",
                                client.endpoint().host(),
                                client.endpoint().port()
                            );
                            rendezvous(&barrier, 2);
                            Ok(endpoint)
                        })
                        .expect("striped worker call")
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("worker thread"))
            .collect::<Vec<_>>()
    });
    assert_eq!(seen.len(), 2, "one endpoint per worker: {seen:?}");
    assert_ne!(
        seen[0], seen[1],
        "worker ids must stripe distinct initial endpoints: {seen:?}"
    );
}

#[test]
fn worker_client_round_trips_row_blocks() {
    let server = docker::Server::start();
    let client = Client::connect_workers(config(&server).with_workers(2)).expect("worker pool");
    client
        .execute("CREATE OR REPLACE TABLE worker_events (id UInt64, name String) ENGINE = Memory")
        .expect("create table");

    let mut block = Block::new();
    block
        .append_fixed_slice("id", "UInt64", &[1_u64, 2])
        .expect("id column");
    block
        .append_strings_flat("name", &[3, 6], b"abcabc")
        .expect("name column");
    assert_eq!(block.row_count(), 2);
    client
        .insert_block("worker_events", block)
        .expect("insert row block");

    let blocks = client
        .query_blocks("SELECT id, name FROM worker_events ORDER BY id")
        .expect("query rows");
    let mut ids = Vec::new();
    let mut names = Vec::new();
    for result in &blocks {
        let id_column = result.column_index("id").expect("id column present");
        let name_column = result.column_index("name").expect("name column present");
        ids.extend_from_slice(result.column_slice::<u64>(id_column).expect("id slice"));
        for row in 0..result.row_count() {
            names.push(
                result
                    .column_str(name_column, row)
                    .expect("name string")
                    .to_string(),
            );
        }
    }
    assert_eq!(ids, vec![1_u64, 2]);
    assert_eq!(names, vec![String::from("abc"), String::from("abc")]);
}

/// Different handles nest on one thread; recursion on one handle is rejected.
#[test]
fn nested_handles_and_panic_recovery() {
    let server = docker::Server::start();
    let first = Client::thread_local(config(&server)).expect("first factory");
    let second = Client::thread_local(config(&server)).expect("second factory");
    first
        .with(|client| {
            client.execute("SELECT 1")?;
            second.with(|client| client.execute("SELECT 2"))?;
            assert!(matches!(first.with(|_| Ok(())), Err(Error::Config { .. })));
            Ok(())
        })
        .expect("independent factories may nest");
    let worker = Client::connect_workers(config(&server)).expect("worker handle");
    let nested = Client::connect_workers(config(&server)).expect("nested worker");
    worker
        .with(|client| {
            client.execute("SELECT 1")?;
            nested.with(|client| client.execute("SELECT 2"))?;
            assert!(matches!(worker.with(|_| Ok(())), Err(Error::Config { .. })));
            Ok(())
        })
        .expect("independent workers may nest");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        worker.with::<()>(|_| panic!("user callback failed"))
    }));
    assert!(panic.is_err());
    worker
        .execute("SELECT 3")
        .expect("lease and client restored");
}

fn wait_flag(flag: &AtomicBool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !flag.load(Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "worker coordination deadline"
        );
        thread::yield_now();
    }
}

fn rendezvous(arrived: &AtomicUsize, participants: usize) {
    arrived.fetch_add(1, Ordering::AcqRel);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while arrived.load(Ordering::Acquire) < participants {
        assert!(
            std::time::Instant::now() < deadline,
            "peer rendezvous deadline"
        );
        thread::yield_now();
    }
}
