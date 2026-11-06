//! In-thread connection balancing and calling-thread client factories.
use std::{
    cell::RefCell,
    collections::{HashMap, hash_map::Entry},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use uarch::prelude::*;

use crate::{Block, Client, ClientConfig, Endpoint, Error, Result, row::RowClient};

/// Strategy used to choose a local connection slot inside one thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkerBalance {
    /// Rotate through local slots in dispatch order.
    #[default]
    RoundRobin,
    /// Use the local slot with the fewest active calls.
    ///
    /// Ties rotate so synchronous calls still spread across slots.
    LeastInFlight,
    /// Hash an explicit request key with FNV-1a modulo the slot count.
    ///
    /// Unkeyed calls fall back to round robin.
    KeyHash,
}

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

mod config;
use config::OwnedConfig;

/// Source of unique factory and pool identities.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> Result<u64> {
    // Reusing an identity could select a connection retained by an older factory.
    NEXT_CLIENT_ID
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| identity_exhausted())
}

#[cold_sink]
fn identity_exhausted() -> Error {
    Error::Config {
        reason: "client factory identity namespace is exhausted".into(),
    }
}

thread_local! {
    /// Clients owned by the current thread, keyed by [`ThreadLocalClient`] id.
    static THREAD_CLIENTS: RefCell<HashMap<u64, Option<Client>>> = RefCell::new(HashMap::new());
    /// Worker slots leased by the current thread, keyed by [`WorkerClient`] id.
    static LOCAL_WORKERS: RefCell<HashMap<u64, Option<LocalWorker>>> = RefCell::new(HashMap::new());
}

/// Creates and reuses one [`Client`] per calling OS thread.
///
/// Construction neither connects nor allocates a client. The first [`with`]
/// call on a thread opens a connection, and later calls on the same thread
/// reuse it. Different threads never share a client.
///
/// [`with`]: Self::with
pub struct ThreadLocalClient {
    id: u64,
    config: OwnedConfig,
}

impl ThreadLocalClient {
    /// Create a thread-local client factory from an endpoint list and options.
    ///
    /// # Errors
    /// Returns a configuration error for an empty endpoint list, invalid
    /// endpoint fields, or an invalid timeout.
    pub fn new(config: ClientConfig<'_>) -> Result<Self> {
        let config = OwnedConfig::capture(&config)?;
        Ok(Self {
            id: next_id()?,
            config,
        })
    }

    /// Configured endpoint list; each thread dials it in this order.
    pub fn endpoints(&self) -> Vec<Endpoint> {
        self.config.endpoints()
    }

    /// Run `f` against this thread's client, creating it on first use.
    ///
    /// The closure runs on the calling thread, so it may use `NativeBlock`,
    /// `WireBlock`, `BlockStream`, `BlockView`, and non-`Send` callbacks
    /// directly. Do not call [`with`] recursively on the same factory.
    ///
    /// # Errors
    /// Returns a connection or closure error, or a configuration error for
    /// recursion on this same handle.
    ///
    /// [`with`]: Self::with
    #[hot_inline]
    pub fn with<R>(&self, f: impl FnOnce(&mut Client) -> Result<R>) -> Result<R> {
        THREAD_CLIENTS.with(|cell| {
            let mut checked = Checkout::take(cell, self.id)?;
            if checked.value.is_none() {
                checked.value = Some(Client::new(self.config.to_config(0))?);
            }
            f(checked.value.as_mut().ok_or_else(reentrant_error)?)
        })
    }

    /// Execute a statement on this thread's client.
    ///
    /// # Errors
    /// Returns a connection or query error.
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.with(|client| client.execute(sql))
    }

    /// Collect row-layer blocks from this thread's client.
    ///
    /// # Errors
    /// Returns a connection, query, or conversion error.
    pub fn query_blocks(&self, sql: &str) -> Result<Vec<Block>> {
        self.with(|client| <Client as RowClient>::query_blocks(client, sql))
    }

    /// Insert one row-layer block on this thread's client.
    ///
    /// # Errors
    /// Returns a connection or insert error.
    pub fn insert_block(&self, table: &str, block: Block) -> Result<()> {
        self.with(|client| <Client as RowClient>::insert_block(client, table, &block))
    }
}

bitword! {
    struct WorkerLeaseWord(u64) {
        [0] LEASED,
    }
}
assert_layout!(WorkerLeaseWord, size = 8, align = align_of::<u64>());
assert_layout!(Isolated<AtomicU64>, size = 128, align = 128);

/// State shared by every caller of one [`WorkerClient`].
struct PoolShared {
    config: OwnedConfig,
    workers: usize,
    balance: WorkerBalance,
    leased: Vec<Isolated<AtomicU64>>,
    dispatched: Vec<Isolated<AtomicU64>>,
}

impl PoolShared {
    /// Claim one worker slot without blocking.
    #[hot_inline]
    fn lease(&self) -> Option<usize> {
        for (index, slot) in self.leased.iter().enumerate() {
            let state = WorkerLeaseWord::new(slot.load(Ordering::Acquire));
            if state.matches(pattern!(WorkerLeaseWord { LEASED })) {
                continue;
            }
            let leased = state.patch(patch!(WorkerLeaseWord { LEASED: true }));
            if slot
                .compare_exchange(
                    state.raw(),
                    leased.raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Some(index);
            }
        }
        None
    }

    #[hot_inline]
    fn release(&self, index: usize) {
        let state = WorkerLeaseWord::new(self.leased[index].load(Ordering::Relaxed));
        assert!(
            state.matches(pattern!(WorkerLeaseWord { LEASED })),
            "worker lease released twice"
        );
        self.leased[index].store(
            state.patch(patch!(WorkerLeaseWord { LEASED: false })).raw(),
            Ordering::Release,
        );
    }

    /// Initial endpoint offset for one worker and local slot.
    #[hot_inline]
    fn stripe(&self, worker_id: usize, slot: usize) -> usize {
        self.config.endpoint_slot(worker_id + slot)
    }
}

/// One thread's leased worker and its local connection slots.
struct LocalWorker {
    shared: Arc<PoolShared>,
    worker_id: usize,
    slots: Vec<Option<Client>>,
    next: usize,
    in_flight: Vec<usize>,
}

impl LocalWorker {
    fn new(shared: Arc<PoolShared>, worker_id: usize) -> Self {
        let workers = shared.workers;
        Self {
            shared,
            worker_id,
            slots: (0..workers).map(|_| None).collect(),
            next: 0,
            in_flight: vec![0; workers],
        }
    }

    fn dispatch<R>(
        &mut self,
        key: Option<u64>,
        f: impl FnOnce(&mut Client) -> Result<R>,
    ) -> Result<R> {
        let slot = self.select(key);
        self.shared.dispatched[slot].fetch_add(1, Ordering::Relaxed);
        if self.slots[slot].is_none() {
            let rotation = self.shared.stripe(self.worker_id, slot);
            self.slots[slot] = Some(Client::new(self.shared.config.to_config(rotation))?);
        }
        self.in_flight[slot] += 1;
        let client = match self.slots[slot].as_mut() {
            Some(client) => client,
            None => {
                self.in_flight[slot] -= 1;
                return Err(Error::Config {
                    reason: "local worker slot is unavailable".into(),
                });
            }
        };
        let result = catch_unwind(AssertUnwindSafe(|| f(client)));
        self.in_flight[slot] -= 1;
        match result {
            Ok(result) => result,
            Err(payload) => resume_unwind(payload),
        }
    }

    #[hot_inline]
    fn select(&mut self, key: Option<u64>) -> usize {
        let workers = self.shared.workers;
        match self.shared.balance {
            WorkerBalance::RoundRobin => self.round_robin(workers),
            WorkerBalance::LeastInFlight => {
                let least = self.in_flight.iter().copied().min().unwrap_or(0);
                // Synchronous calls never overlap on one thread, so ties are
                // common. Rotate the tie-break to keep every slot in use.
                for _ in 0..workers {
                    let slot = self.round_robin(workers);
                    if self.in_flight[slot] == least {
                        return slot;
                    }
                }
                self.round_robin(workers)
            }
            WorkerBalance::KeyHash => match key {
                Some(key) => self.shared.config.worker_slot(hash_key(key)),
                None => self.round_robin(workers),
            },
        }
    }

    #[hot_inline]
    fn round_robin(&mut self, workers: usize) -> usize {
        debug_assert_eq!(workers, self.shared.workers);
        let slot = self.shared.config.worker_slot(self.next as u64);
        let next = slot + 1;
        self.next = predicate(next == workers).select_register(0, next);
        slot
    }
}

impl Drop for LocalWorker {
    fn drop(&mut self) {
        self.shared.release(self.worker_id);
    }
}

/// A [`Send`] + [`Sync`] handle whose calls run on the calling thread.
///
/// Each OS thread lazily leases one worker slot in `0..workers` and opens up to
/// `workers` local, strictly single-threaded connections. The leased worker id
/// offsets the initial endpoint order so concurrent workers start on distinct
/// endpoints. Calls are balanced across the thread's own slots; no request or
/// result crosses a thread boundary, so `NativeBlock`, `WireBlock`,
/// `BlockStream`, `BlockView`, and non-`Send` callbacks remain usable.
pub struct WorkerClient {
    id: u64,
    shared: Arc<PoolShared>,
}

impl WorkerClient {
    pub(crate) fn new(config: ClientConfig<'_>) -> Result<Self> {
        let config = OwnedConfig::capture(&config)?;
        let workers = config.workers.max(1);
        let balance = config.balance;
        Ok(Self {
            id: next_id()?,
            shared: Arc::new(PoolShared {
                config,
                workers,
                balance,
                leased: (0..workers)
                    .map(|_| Isolated::new(AtomicU64::new(0)))
                    .collect(),
                dispatched: (0..workers)
                    .map(|_| Isolated::new(AtomicU64::new(0)))
                    .collect(),
            }),
        })
    }

    /// Number of worker slots and local connections available per thread.
    pub fn workers(&self) -> usize {
        self.shared.workers
    }

    /// Configured in-thread load-balancing strategy.
    pub fn balance(&self) -> WorkerBalance {
        self.shared.balance
    }

    /// Dispatch counts per local slot, aggregated across all calling threads.
    pub fn dispatch_counts(&self) -> Vec<u64> {
        self.shared
            .dispatched
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }

    /// Run `f` against a balanced local slot on the calling thread.
    ///
    /// The closure may return or consume `!Send` values because it never
    /// leaves the calling thread. Do not call [`with`] recursively on the same
    /// handle.
    ///
    /// # Errors
    /// Returns [`Error::WorkerSlotsExhausted`] when every worker slot is leased
    /// by another live thread, or a connection or closure error. Recursive calls
    /// on the same handle return a configuration error.
    ///
    /// [`with`]: Self::with
    #[hot_inline]
    pub fn with<R>(&self, f: impl FnOnce(&mut Client) -> Result<R>) -> Result<R> {
        self.with_worker(None, f)
    }

    /// Run `f` against the slot selected by an explicit key.
    ///
    /// The key participates in selection when [`balance`] is
    /// [`WorkerBalance::KeyHash`]; other strategies ignore it.
    ///
    /// # Errors
    /// Returns [`Error::WorkerSlotsExhausted`] when every worker slot is leased
    /// by another live thread, or a connection or closure error. Recursive calls
    /// on the same handle return a configuration error.
    ///
    /// [`balance`]: Self::balance
    pub fn with_keyed<R>(&self, key: u64, f: impl FnOnce(&mut Client) -> Result<R>) -> Result<R> {
        self.with_worker(Some(key), f)
    }

    fn with_worker<R>(
        &self,
        key: Option<u64>,
        f: impl FnOnce(&mut Client) -> Result<R>,
    ) -> Result<R> {
        LOCAL_WORKERS.with(|cell| {
            let mut checked = Checkout::take(cell, self.id)?;
            if checked.value.is_none() {
                let worker_id = self.shared.lease().ok_or(Error::WorkerSlotsExhausted {
                    workers: self.shared.workers,
                })?;
                checked.value = Some(LocalWorker::new(self.shared.clone(), worker_id));
            }
            let worker = checked.value.as_mut().ok_or_else(reentrant_error)?;
            let result = worker.dispatch(key, f);
            if worker.slots.iter().all(Option::is_none) {
                // No connection was established; release the lease before returning.
                checked.value = None;
            }
            result
        })
    }

    /// Execute a statement on a balanced local slot.
    ///
    /// # Errors
    /// Returns a configuration, connection, or query error.
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.with(|client| client.execute(sql))
    }

    /// Execute a statement on the slot selected by `key`.
    ///
    /// # Errors
    /// Returns a configuration, connection, or query error.
    pub fn execute_keyed(&self, key: u64, sql: &str) -> Result<()> {
        self.with_keyed(key, |client| client.execute(sql))
    }

    /// Collect row-layer blocks from a balanced local slot.
    ///
    /// # Errors
    /// Returns a configuration, connection, query, or conversion error.
    pub fn query_blocks(&self, sql: &str) -> Result<Vec<Block>> {
        self.with(|client| <Client as RowClient>::query_blocks(client, sql))
    }

    /// Collect row-layer blocks from the slot selected by `key`.
    ///
    /// # Errors
    /// Returns a configuration, connection, query, or conversion error.
    pub fn query_blocks_keyed(&self, key: u64, sql: &str) -> Result<Vec<Block>> {
        self.with_keyed(key, |client| {
            <Client as RowClient>::query_blocks(client, sql)
        })
    }

    /// Insert one row-layer block on a balanced local slot.
    ///
    /// # Errors
    /// Returns a configuration, connection, or insert error.
    pub fn insert_block(&self, table: &str, block: Block) -> Result<()> {
        self.with(|client| <Client as RowClient>::insert_block(client, table, &block))
    }

    /// Insert one row-layer block on the slot selected by `key`.
    ///
    /// # Errors
    /// Returns a configuration, connection, or insert error.
    pub fn insert_block_keyed(&self, key: u64, table: &str, block: Block) -> Result<()> {
        self.with_keyed(key, |client| {
            <Client as RowClient>::insert_block(client, table, &block)
        })
    }
}

fn hash_key(key: u64) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// A checked-out entry leaves a marker that rejects recursion on the same handle.
struct Checkout<'a, T> {
    cell: &'a RefCell<HashMap<u64, Option<T>>>,
    id: u64,
    value: Option<T>,
}

impl<'a, T> Checkout<'a, T> {
    #[hot_inline]
    fn take(cell: &'a RefCell<HashMap<u64, Option<T>>>, id: u64) -> Result<Self> {
        let value = match cell.borrow_mut().entry(id) {
            Entry::Occupied(mut entry) => entry
                .get_mut()
                .take()
                .ok_or_else(reentrant_error)
                .map(Some)?,
            Entry::Vacant(entry) => {
                entry.insert(None);
                None
            }
        };
        Ok(Self { cell, id, value })
    }
}

impl<T> Drop for Checkout<'_, T> {
    fn drop(&mut self) {
        let mut entries = self.cell.borrow_mut();
        if let Some(value) = self.value.take() {
            entries.insert(self.id, Some(value));
        } else {
            entries.remove(&self.id);
        }
    }
}

#[cold_sink]
fn reentrant_error() -> Error {
    Error::Config {
        reason: "reentrant with() call on the same handle is not allowed".into(),
    }
}

impl Drop for ThreadLocalClient {
    fn drop(&mut self) {
        // TLS teardown already released the cached clients and their resources.
        if let Ok(()) = THREAD_CLIENTS.try_with(|cell| {
            if let Ok(mut entries) = cell.try_borrow_mut() {
                entries.remove(&self.id);
            }
        }) {}
    }
}

impl Drop for WorkerClient {
    fn drop(&mut self) {
        // TLS teardown already released each cached worker and its lease.
        if let Ok(()) = LOCAL_WORKERS.try_with(|cell| {
            if let Ok(mut entries) = cell.try_borrow_mut() {
                entries.remove(&self.id);
            }
        }) {}
    }
}

#[cfg(test)]
#[path = "../../tests/threading/local.rs"]
mod tests;
