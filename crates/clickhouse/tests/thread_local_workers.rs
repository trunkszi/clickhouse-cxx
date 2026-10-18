//! Thread-local clients and locally balanced worker slots against a live server.
//!
//! The transport handles stay single-threaded. These tests exercise the two
//! public escape hatches: one client per OS thread, and a `Send + Sync` handle
//! that leases one worker id per thread and balances calls across that thread's
//! own local connection slots.

#[path = "support/docker.rs"]
mod docker;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Barrier, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, ThreadId},
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

/// Record the current thread's client address and return it.
fn record_client(pointers: &Mutex<HashMap<ThreadId, Vec<usize>>>, client: &mut Client) -> usize {
    let address = client as *mut Client as usize;
    pointers
        .lock()
        .expect("pointer map")
        .entry(thread::current().id())
        .or_default()
        .push(address);
    address
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
    let pointers = Mutex::new(HashMap::<ThreadId, Vec<usize>>::new());
    // The barrier keeps all four thread-local clients alive at the same time,
    // so pointer identity across threads cannot be confused with allocator
    // reuse after a thread has already exited.
    let barrier = Barrier::new(4);

    thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                let first = local.with(|client| {
                    client.execute("SELECT 1")?;
                    Ok(record_client(&pointers, client))
                });
                barrier.wait();
                let first = first.expect("first thread-local SELECT 1");
                let second = local.with(|client| {
                    client.execute("SELECT 1")?;
                    Ok(record_client(&pointers, client))
                });
                let second = second.expect("second thread-local SELECT 1");
                assert_eq!(
                    first, second,
                    "repeated calls on one thread must reuse the same client"
                );
            });
        }
    });

    let pointers = pointers.into_inner().expect("pointer map");
    assert_eq!(pointers.len(), 4, "one entry per OS thread");
    for (thread_id, addresses) in &pointers {
        assert_eq!(addresses.len(), 2, "two calls on {thread_id:?}");
        assert_eq!(
            addresses[0], addresses[1],
            "repeated calls on {thread_id:?} must reuse the same client"
        );
    }
    let distinct: HashSet<usize> = pointers.values().map(|addresses| addresses[0]).collect();
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
    let (ready_tx, ready_rx): (Sender<()>, Receiver<()>) = mpsc::channel();
    let (done_tx, done_rx): (Sender<()>, Receiver<()>) = mpsc::channel();

    let holder = Arc::clone(&client);
    let thread = thread::spawn(move || {
        holder
            .with(|_| {
                ready_tx.send(()).expect("announce lease");
                done_rx.recv().expect("wait for contender");
                Ok(())
            })
            .expect("holder call");
    });
    ready_rx.recv().expect("holder is ready");
    let error = client.with(|_| Ok(())).expect_err("slot must be exhausted");
    match error {
        Error::WorkerSlotsExhausted { workers } => assert_eq!(workers, 1),
        other => panic!("expected WorkerSlotsExhausted, got {other:?}"),
    }
    done_tx.send(()).expect("release holder");
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
    let seen = Mutex::new(Vec::new());
    // Hold both leases at once so the endpoint comparison cannot see a
    // released slot reused by the second thread.
    let barrier = Barrier::new(2);

    thread::scope(|scope| {
        for _ in 0..2 {
            let seen = &seen;
            let barrier = &barrier;
            let client = &client;
            scope.spawn(move || {
                client
                    .with(|client| {
                        let endpoint =
                            format!("{}:{}", client.endpoint().host(), client.endpoint().port());
                        seen.lock().expect("endpoint log").push(endpoint);
                        barrier.wait();
                        Ok(())
                    })
                    .expect("striped worker call");
            });
        }
    });

    let seen = seen.into_inner().expect("endpoint log");
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
