//! Endpoint choice and circuit state shared by synchronous connection attempts.
use std::time::{Duration, Instant};

/// Endpoint choice among currently healthy, closed circuits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EndpointSelection {
    /// Walk endpoints in configured order.
    #[default]
    RoundRobin,
    /// Rotate using a local deterministic pseudorandom sequence.
    Random,
    /// Prefer the lowest measured connection or probe latency.
    LatencyAware,
    /// Keep the current endpoint until it fails.
    Sticky,
}
/// Thread-bound retry and circuit-breaker settings. Query writes are never replayed.
#[derive(Clone, Copy, Debug)]
pub struct FailoverPolicy {
    pub(crate) selection: EndpointSelection,
    pub(crate) retries: u32,
    pub(crate) delay: Duration,
    pub(crate) ping: bool,
    pub(crate) threshold: u32,
    pub(crate) cooldown: Duration,
    pub(crate) random_seed: Option<u64>,
    thread_bound: std::marker::PhantomData<*const ()>,
}
impl Default for FailoverPolicy {
    fn default() -> Self {
        Self::new()
    }
}
impl FailoverPolicy {
    /// Select ordered failover without a retry delay.
    pub const fn new() -> Self {
        Self {
            selection: EndpointSelection::RoundRobin,
            retries: 1,
            delay: Duration::ZERO,
            ping: false,
            threshold: 3,
            cooldown: Duration::from_secs(30),
            random_seed: None,
            thread_bound: std::marker::PhantomData,
        }
    }
    /// Choose the endpoint selection strategy.
    #[must_use]
    pub fn with_selection(mut self, value: EndpointSelection) -> Self {
        self.selection = value;
        self
    }
    /// Fix the random-selection sequence for reproducible tests or controlled deployments.
    /// Zero is normalized to one so the generator cannot remain in its zero state.
    #[must_use]
    pub fn with_random_seed(mut self, seed: u64) -> Self {
        self.random_seed = Some(seed.max(1));
        self
    }
    /// Set complete connection passes; zero still permits one initial walk.
    #[must_use]
    pub fn with_send_retries(mut self, value: u32) -> Self {
        self.retries = value;
        self
    }
    /// Set the cold reconnect delay.
    #[must_use]
    pub fn with_retry_timeout(mut self, value: Duration) -> Self {
        self.delay = value;
        self
    }
    /// Probe the active connection before an operation.
    #[must_use]
    pub fn with_ping_before_query(mut self, value: bool) -> Self {
        self.ping = value;
        self
    }
    /// Open a circuit after consecutive failures until its cooldown expires.
    #[must_use]
    pub fn with_circuit(mut self, threshold: u32, cooldown: Duration) -> Self {
        self.threshold = threshold.max(1);
        self.cooldown = cooldown;
        self
    }
}
/// Last measured endpoint health and circuit status.
#[derive(Clone, Debug)]
pub struct EndpointHealth {
    /// Index in the configured endpoint list.
    pub index: usize,
    /// Whether the latest connection or probe succeeded.
    pub healthy: bool,
    /// Consecutive failed attempts.
    pub failures: u32,
    /// Most recent measured latency.
    pub latency: Option<Duration>,
    /// Whether failures have opened the circuit.
    pub circuit_open: bool,
    /// Earliest instant when a half-open probe may run.
    pub retry_at: Option<Instant>,
    /// Last probe or dial failure retained for diagnostics.
    pub last_error: Option<String>,
}
impl EndpointHealth {
    pub(crate) fn new(index: usize) -> Self {
        Self {
            index,
            healthy: false,
            failures: 0,
            latency: None,
            circuit_open: false,
            retry_at: None,
            last_error: None,
        }
    }
    pub(crate) fn available(&self, now: Instant) -> bool {
        !self.circuit_open || self.retry_at.is_some_and(|deadline| now >= deadline)
    }
    pub(crate) fn success(&mut self, elapsed: Duration) {
        self.healthy = true;
        self.failures = 0;
        self.latency = Some(elapsed);
        self.circuit_open = false;
        self.retry_at = None;
        self.last_error = None;
    }
    pub(crate) fn failure(&mut self, policy: FailoverPolicy, error: String) {
        self.healthy = false;
        self.last_error = Some(error);
        self.failures = self.failures.saturating_add(1);
        if self.failures >= policy.threshold {
            self.circuit_open = true;
            self.retry_at = Instant::now().checked_add(policy.cooldown);
        }
    }
}
/// Observable transition between configured endpoints.
#[derive(Debug)]
pub struct FailoverEvent<'a> {
    /// Source endpoint index.
    pub old_index: usize,
    /// Destination endpoint index.
    pub new_index: usize,
    /// Source endpoint address.
    pub old_endpoint: std::borrow::Cow<'a, str>,
    /// Destination endpoint address.
    pub new_endpoint: std::borrow::Cow<'a, str>,
}
