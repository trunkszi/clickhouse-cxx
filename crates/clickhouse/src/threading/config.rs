//! Cold, owned connection settings and cached routing divisors.
use std::{borrow::Cow, num::NonZeroU64, time::Duration};

use uarch::prelude::*;

use super::WorkerBalance;
use crate::{
    ClientConfig, CompressionMethod, Endpoint, EndpointSelection, Error, FailoverPolicy, Result,
    Scheme, TlsSettings,
};

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
pub(super) struct OwnedConfig {
    endpoints: Vec<EndpointSpec>,
    endpoint_divisor: FastDivisorU64,
    worker_divisor: FastDivisorU64,
    policy: PolicySnapshot,
    user: String,
    password: String,
    database: String,
    tls: TlsSettings<'static>,
    compression: CompressionMethod,
    pub(super) timeout: Option<Duration>,
    pub(super) workers: usize,
    pub(super) balance: WorkerBalance,
}

impl OwnedConfig {
    pub(super) fn capture(config: &ClientConfig<'_>) -> Result<Self> {
        if config.callback.is_some() {
            return Err(Error::Config {
                reason: "failover callbacks are per-thread state and require a plain Client".into(),
            });
        }
        config.validate()?;
        let endpoint_count =
            NonZeroU64::new(config.endpoints.len() as u64).ok_or_else(empty_endpoints)?;
        let worker_count =
            NonZeroU64::new(config.workers.max(1) as u64).ok_or_else(empty_workers)?;
        Ok(Self {
            endpoints: config.endpoints.iter().map(EndpointSpec::capture).collect(),
            endpoint_divisor: FastDivisorU64::new(endpoint_count),
            worker_divisor: FastDivisorU64::new(worker_count),
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

    #[hot_inline]
    pub(super) fn endpoint_slot(&self, index: usize) -> usize {
        self.endpoint_divisor.rem(index as u64) as usize
    }
    #[hot_inline]
    pub(super) fn worker_slot(&self, index: u64) -> usize {
        self.worker_divisor.rem(index) as usize
    }

    pub(super) fn endpoints(&self) -> Vec<Endpoint> {
        self.endpoints.iter().map(EndpointSpec::restore).collect()
    }

    /// Rebuild a client configuration whose endpoint order starts at `rotate`.
    pub(super) fn to_config(&self, rotate: usize) -> ClientConfig<'_> {
        let mut endpoints = self.endpoints.clone();
        endpoints.rotate_left(self.endpoint_slot(rotate));
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

#[cold_sink]
fn empty_endpoints() -> Error {
    Error::Config {
        reason: "worker endpoint list is empty".into(),
    }
}
#[cold_sink]
fn empty_workers() -> Error {
    Error::Config {
        reason: "worker slot count is zero".into(),
    }
}
