//! Clients remain local to each calling OS thread; only factory configuration is shared.
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{HashMap, hash_map::Entry},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use super::WorkerBalance;
use crate::{
    Block, Client, ClientConfig, CompressionMethod, Endpoint, EndpointSelection, Error,
    FailoverPolicy, Result, Scheme, TlsSettings, row::RowClient,
};

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Thread-safe copy of one failover policy.
///
/// [`FailoverPolicy`] itself is `!Send` because it is created and consumed by
/// one client. A worker handle reconstructs one policy per calling thread.
#[derive(Clone, Copy)]
struct PolicySnapshot {
    selection: EndpointSelection,
    retries: u32,
    delay: Duration,
    ping: bool,
    threshold: u32,
    cooldown: Duration,
    random_seed: Option<u64>,
}

impl PolicySnapshot {
    fn capture(policy: &FailoverPolicy) -> Self {
        Self {
            selection: policy.selection,
            retries: policy.retries,
            delay: policy.delay,
            ping: policy.ping,
            threshold: policy.threshold,
            cooldown: policy.cooldown,
            random_seed: policy.random_seed,
        }
    }

    fn restore(&self) -> FailoverPolicy {
        let mut policy = FailoverPolicy::new()
            .with_selection(self.selection)
            .with_send_retries(self.retries)
            .with_retry_timeout(self.delay)
            .with_ping_before_query(self.ping)
            .with_circuit(self.threshold, self.cooldown);
        if let Some(seed) = self.random_seed {
            policy = policy.with_random_seed(seed);
        }
        policy
    }
}

/// Thread-safe endpoint identity; [`Endpoint`] itself is `!Send` and `!Sync`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EndpointSpec {
    scheme: Scheme,
    host: String,
    port: u16,
}

impl EndpointSpec {
    fn capture(endpoint: &Endpoint) -> Self {
        Self {
            scheme: endpoint.scheme(),
            host: endpoint.host().to_string(),
            port: endpoint.port(),
        }
    }

    fn restore(&self) -> Endpoint {
        Endpoint::from_parts(self.scheme, self.host.clone(), self.port)
    }
}

/// Owned connection settings shared by thread-local factories and worker handles.
#[derive(Clone)]
struct OwnedConfig {
    endpoints: Vec<EndpointSpec>,
    policy: PolicySnapshot,
    user: String,
    password: String,
    database: String,
    tls: TlsSettings<'static>,
    compression: CompressionMethod,
    timeout: Option<Duration>,
    workers: usize,
    balance: WorkerBalance,
}

impl OwnedConfig {
    fn capture(config: &ClientConfig<'_>) -> Result<Self> {
        if config.callback.is_some() {
            return Err(Error::Config {
                reason: "failover callbacks are per-thread state and require a plain Client".into(),
            });
        }
        config.validate()?;
        Ok(Self {
            endpoints: config.endpoints.iter().map(EndpointSpec::capture).collect(),
            policy: PolicySnapshot::capture(&config.policy),
            user: config.user.to_string(),
            password: config.password.to_string(),
            database: config.database.to_string(),
            tls: own_tls(&config.tls),
            compression: config.compression,
            timeout: config.timeout,
            workers: config.workers.max(1),
            balance: config.balance,
        })
    }

    /// Rebuild a client configuration whose endpoint order starts at `rotate`.
    fn to_config(&self, rotate: usize) -> ClientConfig<'_> {
        let mut endpoints = self.endpoints.clone();
        let count = endpoints.len();
        endpoints.rotate_left(rotate % count);
        if matches!(endpoints[0].scheme, Scheme::Http | Scheme::Https) {
            // HTTP workers stripe independent sessions; HTTP failover is unsupported.
            endpoints.truncate(1);
        }
        ClientConfig {
            callback: None,
            endpoints: endpoints.iter().map(EndpointSpec::restore).collect(),
            policy: self.policy.restore(),
            user: Cow::Borrowed(self.user.as_str()),
            password: Cow::Borrowed(self.password.as_str()),
            database: Cow::Borrowed(self.database.as_str()),
            tls: self.tls.clone(),
            compression: self.compression,
            timeout: self.timeout,
            workers: self.workers,
            balance: self.balance,
        }
    }
}

fn own_tls(tls: &TlsSettings<'_>) -> TlsSettings<'static> {
    let mut owned = TlsSettings::new()
        .with_ca_pem(tls.ca_pem().to_vec())
        .with_native_roots(tls.use_native_roots())
        .with_skip_verification(tls.skip_verification())
        .with_sni(tls.sni().to_string())
        .with_connect_timeout(tls.connect_timeout())
        .with_read_timeout(tls.read_timeout())
        .with_write_timeout(tls.write_timeout())
        .with_tcp_nodelay(tls.tcp_nodelay());
    if tls.tcp_keepalive() {
        owned = owned.with_keepalive(
            tls.keepalive_idle(),
            tls.keepalive_interval(),
            tls.keepalive_count(),
        );
    }
    owned
}

/// Source of unique factory and pool identities.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
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
        Ok(Self {
            id: next_id(),
            config: OwnedConfig::capture(&config)?,
        })
    }

    /// Configured endpoint list; each thread dials it in this order.
    pub fn endpoints(&self) -> Vec<Endpoint> {
        self.config
            .endpoints
            .iter()
            .map(EndpointSpec::restore)
            .collect()
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

/// State shared by every caller of one [`WorkerClient`].
struct PoolShared {
    config: OwnedConfig,
    workers: usize,
    balance: WorkerBalance,
    leased: Vec<AtomicBool>,
    dispatched: Vec<AtomicU64>,
}

impl PoolShared {
    /// Claim one worker slot without blocking.
    fn lease(&self) -> Option<usize> {
        for (index, slot) in self.leased.iter().enumerate() {
            if slot
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(index);
            }
        }
        None
    }

    fn release(&self, index: usize) {
        self.leased[index].store(false, Ordering::Release);
    }

    /// Initial endpoint offset for one worker and local slot.
    fn stripe(&self, worker_id: usize, slot: usize) -> usize {
        (worker_id + slot) % self.config.endpoints.len()
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
                Some(key) => (hash_key(key) % workers as u64) as usize,
                None => self.round_robin(workers),
            },
        }
    }

    fn round_robin(&mut self, workers: usize) -> usize {
        let workers = workers.max(1);
        let slot = self.next % workers;
        self.next = self.next.wrapping_add(1);
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
            id: next_id(),
            shared: Arc::new(PoolShared {
                config,
                workers,
                balance,
                leased: (0..workers).map(|_| AtomicBool::new(false)).collect(),
                dispatched: (0..workers).map(|_| AtomicU64::new(0)).collect(),
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

fn reentrant_error() -> Error {
    Error::Config {
        reason: "reentrant with() call on the same handle is not allowed".into(),
    }
}

impl Drop for ThreadLocalClient {
    fn drop(&mut self) {
        match THREAD_CLIENTS.try_with(|cell| {
            if let Ok(mut entries) = cell.try_borrow_mut() {
                entries.remove(&self.id);
            }
        }) {
            Ok(()) => {}
            // TLS teardown already released the cached clients and their resources.
            Err(_) => {}
        }
    }
}

impl Drop for WorkerClient {
    fn drop(&mut self) {
        match LOCAL_WORKERS.try_with(|cell| {
            if let Ok(mut entries) = cell.try_borrow_mut() {
                entries.remove(&self.id);
            }
        }) {
            Ok(()) => {}
            // TLS teardown already released each cached worker and its lease.
            Err(_) => {}
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
