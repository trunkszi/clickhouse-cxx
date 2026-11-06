//! Endpoint choice and circuit state shared by synchronous connection attempts.
use std::{
    num::NonZeroU64,
    time::{Duration, Instant},
};

use uarch::prelude::*;

mod circuit;
pub(crate) use circuit::CircuitStateWord;

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
/// Dial masks at most 1024 endpoints and returns `Error::Config` above that.
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
    ///
    /// A cooldown that `Instant::checked_add` cannot represent leaves `retry_at == None`.
    /// That deadline never expires, so the phase stays [`CircuitPhase::OpenCooldown`].
    #[must_use]
    pub fn with_circuit(mut self, threshold: u32, cooldown: Duration) -> Self {
        self.threshold = threshold.max(1);
        self.cooldown = cooldown;
        self
    }

    /// Starting index for one dial. The caller walks the ring from here.
    ///
    /// Random selection advances `random_state` once and leaves a zero state at
    /// zero. Round-robin uses `(current + rotate) % count`. Sticky keeps an
    /// in-range `current` and folds an out-of-range `current` with modulo so
    /// the ring walk is defined. Latency selection returns the configured index
    /// of the first minimum eligible endpoint, or that folded current index
    /// when none are eligible.
    #[must_use]
    pub fn select_index(
        &self,
        health: &[EndpointHealth],
        current: usize,
        now: Instant,
        rotate: bool,
        random_state: &mut u64,
    ) -> Option<usize> {
        EndpointSelector::new(health.len())
            .map(|selector| selector.select(self, health, current, now, rotate, random_state))
    }
}

/// Invariant endpoint-count arithmetic shared by all walks of a configured pool.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EndpointSelector {
    divisor: FastDivisorU64,
}
impl EndpointSelector {
    pub(crate) fn new(count: usize) -> Option<Self> {
        NonZeroU64::new(count as u64).map(|count| Self {
            divisor: FastDivisorU64::new(count),
        })
    }
    #[hot_inline]
    fn fold(self, index: usize) -> usize {
        self.divisor.rem(index as u64) as usize
    }
    pub(crate) fn select(
        self,
        policy: &FailoverPolicy,
        health: &[EndpointHealth],
        current: usize,
        now: Instant,
        rotate: bool,
        random: &mut u64,
    ) -> usize {
        let selected = match policy.selection {
            EndpointSelection::Sticky => self.fold(current),
            EndpointSelection::RoundRobin => self.fold(current.wrapping_add(usize::from(rotate))),
            EndpointSelection::Random => self.fold(step_random(random) as usize),
            EndpointSelection::LatencyAware => latency_start(health, current, now, self),
        };
        if let Some(endpoint) = health.get(selected) {
            prefetch_read::<L1, _>(endpoint);
        }
        selected
    }
    #[cfg(feature = "tcp")]
    pub(crate) fn select_stored(
        self,
        policy: &FailoverPolicy,
        snapshot: (&[EndpointHealth], &[CircuitStateWord]),
        current: usize,
        now: Instant,
        rotate: bool,
        random: &mut u64,
    ) -> usize {
        let (health, states) = snapshot;
        if policy.selection == EndpointSelection::LatencyAware {
            latency_start_by(health, current, self, |index, endpoint| {
                states[index].available(endpoint, now)
            })
        } else {
            self.select(policy, health, current, now, rotate, random)
        }
    }
}

fn step_random(state: &mut u64) -> u64 {
    let mut random = *state;
    random ^= random << 13;
    random ^= random >> 7;
    random ^= random << 17;
    *state = random;
    random
}

fn latency_start(
    health: &[EndpointHealth],
    current: usize,
    now: Instant,
    selector: EndpointSelector,
) -> usize {
    latency_start_by(health, current, selector, |_, endpoint| {
        endpoint.phase(now).is_eligible()
    })
}

fn latency_start_by(
    health: &[EndpointHealth],
    current: usize,
    selector: EndpointSelector,
    eligible: impl Fn(usize, &EndpointHealth) -> bool,
) -> usize {
    let count = health.len();
    health
        .iter()
        .enumerate()
        .inspect(|(index, _)| {
            if let Some(next) = health.get(index + 1) {
                prefetch_read::<L1, _>(next);
            }
        })
        .filter(|(index, endpoint)| eligible(*index, endpoint))
        .min_by_key(|(_, endpoint)| endpoint.latency.unwrap_or(Duration::MAX))
        .map(|(position, endpoint)| {
            if endpoint.index < count {
                endpoint.index
            } else {
                position
            }
        })
        .unwrap_or_else(|| selector.fold(current))
}

/// Endpoints that fit in the fixed eligibility snapshot.
#[cfg(any(feature = "tcp", test))]
pub(crate) const ELIGIBILITY_LIMIT: usize = 1024;

/// Eligibility frozen at one instant.
///
/// A cooldown that elapses after the snapshot stays excluded. The dial still
/// skips an endpoint that was eligible here and later becomes unavailable.
#[cfg(any(feature = "tcp", test))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct EligibilityMask {
    words: [Bitmap64; ELIGIBILITY_LIMIT / 64],
    len: usize,
}

#[cfg(any(feature = "tcp", test))]
impl EligibilityMask {
    #[cfg(test)]
    pub(crate) fn snapshot(health: &[EndpointHealth], now: Instant) -> Option<Self> {
        Self::from_eligible(health.len(), |index| health[index].phase(now).is_eligible())
    }
    #[cfg(feature = "tcp")]
    pub(crate) fn snapshot_stored(
        health: &[EndpointHealth],
        states: &[CircuitStateWord],
        now: Instant,
    ) -> Option<Self> {
        assert_eq!(
            health.len(),
            states.len(),
            "circuit diagnostics and state diverged"
        );
        Self::from_eligible(health.len(), |index| {
            states[index].available(&health[index], now)
        })
    }
    fn from_eligible(len: usize, eligible: impl Fn(usize) -> bool) -> Option<Self> {
        if len > ELIGIBILITY_LIMIT {
            return None;
        }
        let mut words = [Bitmap64::EMPTY; ELIGIBILITY_LIMIT / 64];
        for index in 0..len {
            if eligible(index) {
                let word = &mut words[PowerOfTwo::<6>::div_usize(index)];
                *word = word.set(PowerOfTwo::<6>::rem_usize(index) as u32);
            }
        }
        Some(Self { words, len })
    }

    pub(crate) const fn contains(self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        self.words[PowerOfTwo::<6>::div_usize(index)].test(PowerOfTwo::<6>::rem_usize(index) as u32)
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn is_empty(self) -> bool {
        reduce_four_way!(
            &self.words,
            true,
            true,
            |empty, word| empty & word.is_empty(),
            |a, b| a & b
        )
    }
}

/// Ring order a dial walks, skipping endpoints absent from the snapshot.
#[cfg(any(feature = "tcp", test))]
pub(crate) struct DialOrder {
    range: EndpointRange,
    selector: Option<EndpointSelector>,
    offset: usize,
    mask: EligibilityMask,
}

#[cfg(any(feature = "tcp", test))]
#[register_pass]
#[derive(Clone, Copy)]
struct EndpointRange {
    start: usize,
    count: usize,
}
#[cfg(any(feature = "tcp", test))]
assert_layout!(EndpointRange, size = 2 * size_of::<usize>(), align = align_of::<usize>(),
    fields = { start: 0, count: size_of::<usize>() });

#[cfg(any(feature = "tcp", test))]
impl DialOrder {
    #[cfg(test)]
    pub(crate) fn new(start: usize, mask: EligibilityMask) -> Self {
        let selector = EndpointSelector::new(mask.len);
        Self {
            range: EndpointRange {
                start: selector.map_or(0, |selector| selector.fold(start)),
                count: mask.len,
            },
            selector,
            offset: 0,
            mask,
        }
    }
    pub(crate) fn with_selector(
        start: usize,
        mask: EligibilityMask,
        selector: EndpointSelector,
    ) -> Self {
        Self {
            range: EndpointRange {
                start: selector.fold(start),
                count: mask.len,
            },
            selector: Some(selector),
            offset: 0,
            mask,
        }
    }
}

#[cfg(any(feature = "tcp", test))]
impl Iterator for DialOrder {
    type Item = usize;

    #[hot_inline]
    fn next(&mut self) -> Option<usize> {
        let selector = self.selector?;
        while self.offset < self.range.count {
            let index = selector.fold(self.range.start.wrapping_add(self.offset));
            self.offset += 1;
            if self.mask.contains(index) {
                return Some(index);
            }
        }
        None
    }
}
/// Closed and half-open phases can accept a connection attempt.
/// Open cooldown cannot.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitSuperstate {
    /// The endpoint may be dialed.
    Eligible = 0,
    /// The endpoint stays out of the candidate set.
    Ineligible = 1,
}

/// Circuit health derived from [`EndpointHealth`] fields.
///
/// `Closed*` is only reported when `circuit_open` is false. `OpenCooldown`
/// and `HalfOpenProbe` are only reported when `circuit_open` is true, so a
/// closed circuit cannot be paired with an active cooldown.
///
/// An unrepresentable cooldown deadline (`Instant::checked_add` overflow,
/// `retry_at == None`) never expires and remains `OpenCooldown`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitPhase {
    /// No outstanding failures and the last completed attempt succeeded.
    ClosedHealthy = 0,
    /// The circuit is closed, but the endpoint is not fully healthy.
    ClosedDegraded = 1,
    /// Cooldown has elapsed and one probe may run.
    HalfOpenProbe = 2,
    /// Consecutive failures are still inside the cooldown window.
    OpenCooldown = 3,
}

impl CircuitPhase {
    /// Parent state used by candidate selection.
    #[must_use]
    pub const fn superstate(self) -> CircuitSuperstate {
        match self {
            Self::ClosedHealthy | Self::ClosedDegraded | Self::HalfOpenProbe => {
                CircuitSuperstate::Eligible
            }
            Self::OpenCooldown => CircuitSuperstate::Ineligible,
        }
    }

    /// Whether dial may attempt this endpoint.
    #[must_use]
    pub const fn is_eligible(self) -> bool {
        matches!(self.superstate(), CircuitSuperstate::Eligible)
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
    /// Project the circuit phase from the public health fields.
    #[must_use]
    pub fn phase(&self, now: Instant) -> CircuitPhase {
        if self.circuit_open {
            if self.retry_at.is_some_and(|deadline| now >= deadline) {
                CircuitPhase::HalfOpenProbe
            } else {
                CircuitPhase::OpenCooldown
            }
        } else if ilp_all!(self.healthy, self.failures == 0) {
            CircuitPhase::ClosedHealthy
        } else {
            CircuitPhase::ClosedDegraded
        }
    }

    #[cfg(all(test, feature = "tcp"))]
    pub(crate) fn available(&self, now: Instant) -> bool {
        self.phase(now).is_eligible()
    }
    /// Record a completed handshake without forgiving failed application operations.
    #[cfg(feature = "tcp")]
    pub(crate) fn connected(&mut self, elapsed: Duration) {
        self.latency = Some(elapsed);
        if self.failures == 0 {
            self.success(elapsed);
        }
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

#[cfg(test)]
#[path = "../tests/failover/selection.rs"]
mod selection;
#[cfg(test)]
#[path = "../tests/failover/boundaries.rs"]
mod selector_boundaries;
#[cfg(all(test, feature = "tcp"))]
#[path = "../tests/failover/handshake.rs"]
mod tests;
