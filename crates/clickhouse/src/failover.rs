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
        let count = health.len();
        if count == 0 {
            return None;
        }
        let start = match self.selection {
            EndpointSelection::Sticky => folded_index(current, count),
            EndpointSelection::RoundRobin => current.wrapping_add(usize::from(rotate)) % count,
            EndpointSelection::Random => (step_random(random_state) as usize) % count,
            EndpointSelection::LatencyAware => latency_start(health, current, now),
        };
        Some(start)
    }
}

fn folded_index(current: usize, count: usize) -> usize {
    if current < count {
        current
    } else {
        current % count
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

fn latency_start(health: &[EndpointHealth], current: usize, now: Instant) -> usize {
    let count = health.len();
    health
        .iter()
        .enumerate()
        .filter(|(_, endpoint)| endpoint.phase(now).is_eligible())
        .min_by_key(|(_, endpoint)| endpoint.latency.unwrap_or(Duration::MAX))
        .map(|(position, endpoint)| {
            if endpoint.index < count {
                endpoint.index
            } else {
                position
            }
        })
        .unwrap_or_else(|| folded_index(current, count))
}

/// Endpoints that fit in the fixed eligibility snapshot.
pub(crate) const ELIGIBILITY_LIMIT: usize = 1024;

/// Eligibility frozen at one instant.
///
/// A cooldown that elapses after the snapshot stays excluded. The dial still
/// skips an endpoint that was eligible here and later becomes unavailable.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EligibilityMask {
    words: [u64; ELIGIBILITY_LIMIT / 64],
    len: usize,
}

impl EligibilityMask {
    pub(crate) fn snapshot(health: &[EndpointHealth], now: Instant) -> Option<Self> {
        if health.len() > ELIGIBILITY_LIMIT {
            return None;
        }
        let mut words = [0u64; ELIGIBILITY_LIMIT / 64];
        for (index, endpoint) in health.iter().enumerate() {
            if endpoint.phase(now).is_eligible() {
                words[index / 64] |= 1u64 << (index % 64);
            }
        }
        Some(Self {
            words,
            len: health.len(),
        })
    }

    pub(crate) const fn contains(self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        let bit = index % 64;
        self.words[index / 64] & (1u64 << bit) != 0
    }

    pub(crate) fn is_empty(self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }
}

/// Ring order a dial walks, skipping endpoints absent from the snapshot.
pub(crate) struct DialOrder {
    start: usize,
    count: usize,
    offset: usize,
    mask: EligibilityMask,
}

impl DialOrder {
    pub(crate) const fn new(start: usize, mask: EligibilityMask) -> Self {
        Self {
            start,
            count: mask.len,
            offset: 0,
            mask,
        }
    }
}

impl Iterator for DialOrder {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.count == 0 {
            return None;
        }
        while self.offset < self.count {
            let index = self.start.wrapping_add(self.offset) % self.count;
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
        } else if self.healthy && self.failures == 0 {
            CircuitPhase::ClosedHealthy
        } else {
            CircuitPhase::ClosedDegraded
        }
    }

    #[cfg(feature = "tcp")]
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
mod selection {
    use std::time::{Duration, Instant};

    use super::{DialOrder, EligibilityMask, EndpointHealth, EndpointSelection, FailoverPolicy};

    fn row(index: usize, eligible: bool, latency: Option<Duration>) -> EndpointHealth {
        let now = Instant::now();
        EndpointHealth {
            index,
            healthy: eligible,
            failures: u32::from(!eligible),
            latency,
            circuit_open: !eligible,
            retry_at: (!eligible).then(|| now + Duration::from_secs(60)),
            last_error: None,
        }
    }

    #[test]
    fn select_start_matches_dial_rules() {
        let now = Instant::now();
        let health = vec![
            row(0, true, Some(Duration::from_micros(40))),
            row(1, false, Some(Duration::from_micros(1))),
            row(2, true, Some(Duration::from_micros(10))),
            row(3, true, Some(Duration::from_micros(25))),
        ];
        let mut state = 0u64;
        let sticky = FailoverPolicy::new().with_selection(EndpointSelection::Sticky);
        assert_eq!(
            sticky.select_index(&health, 1, now, false, &mut state),
            Some(1)
        );
        assert_eq!(
            sticky.select_index(&health, 6, now, false, &mut state),
            Some(2)
        );
        assert_eq!(state, 0);

        let robin = FailoverPolicy::new().with_selection(EndpointSelection::RoundRobin);
        assert_eq!(
            robin.select_index(&health, 6, now, true, &mut state),
            Some(3)
        );
        assert_eq!(
            robin.select_index(&health, 6, now, false, &mut state),
            Some(2)
        );
        assert_eq!(
            robin.select_index(&health, 3, now, true, &mut state),
            Some(0)
        );

        let random = FailoverPolicy::new().with_selection(EndpointSelection::Random);
        let mut random_state = 0u64;
        assert_eq!(
            random.select_index(&health, 0, now, false, &mut random_state),
            Some(0)
        );
        assert_eq!(random_state, 0);
        random_state = 7;
        let once = random.select_index(&health, 0, now, false, &mut random_state);
        let mut manual = 7u64;
        manual ^= manual << 13;
        manual ^= manual >> 7;
        manual ^= manual << 17;
        assert_eq!(random_state, manual);
        assert_eq!(once, Some((manual as usize) % health.len()));
        let advanced = random_state;
        assert!(
            random
                .select_index(&health, 0, now, false, &mut random_state)
                .is_some()
        );
        assert_ne!(random_state, advanced);

        let mut skewed = health.clone();
        skewed[2].index = 3;
        let latency = FailoverPolicy::new().with_selection(EndpointSelection::LatencyAware);
        assert_eq!(
            latency.select_index(&skewed, 0, now, false, &mut state),
            Some(3)
        );
        skewed[2].index = 99;
        assert_eq!(
            latency.select_index(&skewed, 0, now, false, &mut state),
            Some(2)
        );
        let closed: Vec<_> = (0..4).map(|index| row(index, false, None)).collect();
        assert_eq!(
            latency.select_index(&closed, 6, now, false, &mut state),
            Some(2)
        );
        assert!(
            sticky
                .select_index(&[], 0, now, false, &mut state)
                .is_none()
        );
    }

    #[test]
    fn snapshot_order_stays_frozen() {
        let now = Instant::now();
        let mut health = vec![
            row(0, true, None),
            row(1, false, None),
            row(2, true, None),
            row(3, true, None),
        ];
        let mask = EligibilityMask::snapshot(&health, now).expect("mask fits");
        let mut state = 0u64;
        let sticky_start = FailoverPolicy::new()
            .with_selection(EndpointSelection::Sticky)
            .select_index(&health, 1, now, false, &mut state)
            .expect("start");
        assert_eq!(sticky_start, 1);
        assert!(!mask.contains(sticky_start));
        let order: Vec<_> = DialOrder::new(sticky_start, mask).collect();
        assert_eq!(order, vec![2, 3, 0]);

        health[1].circuit_open = true;
        health[1].retry_at = Some(now);
        assert!(health[1].phase(now).is_eligible());
        assert!(!mask.contains(1));
        let frozen: Vec<_> = DialOrder::new(sticky_start, mask).collect();
        assert_eq!(frozen, vec![2, 3, 0]);

        health[2].circuit_open = true;
        health[2].retry_at = Some(now + Duration::from_secs(30));
        assert!(mask.contains(2));
        let live: Vec<_> = DialOrder::new(0, mask)
            .filter(|index| health[*index].phase(now).is_eligible())
            .collect();
        assert_eq!(live, vec![0, 3]);

        let wide = vec![EndpointHealth::new(0); super::ELIGIBILITY_LIMIT + 1];
        assert!(EligibilityMask::snapshot(&wide, now).is_none());
    }
}

#[cfg(all(test, feature = "tcp"))]
mod tests {
    use std::time::{Duration, Instant};

    use super::{EndpointHealth, FailoverPolicy};

    #[test]
    fn unit_handshake_keeps_failure_streak() {
        let policy = FailoverPolicy::new().with_circuit(3, Duration::from_secs(60));
        let mut health = EndpointHealth::new(0);
        for expected in 1..=3 {
            health.failure(policy, String::from("stream socket disconnected"));
            health.connected(Duration::from_millis(1));
            assert_eq!(health.failures, expected);
        }
        assert!(health.circuit_open);
        assert!(!health.available(Instant::now()));
        health.success(Duration::from_millis(2));
        assert_eq!(health.failures, 0);
        assert!(health.available(Instant::now()));
        assert!(health.last_error.is_none());
    }

    #[test]
    fn unit_half_open_requires_operation_success() {
        let policy = FailoverPolicy::new().with_circuit(1, Duration::ZERO);
        let mut health = EndpointHealth::new(0);
        health.failure(policy, String::from("transport failed"));
        assert!(health.available(Instant::now()));
        health.connected(Duration::from_millis(1));
        assert!(health.circuit_open);
        assert_eq!(health.failures, 1);
        health.failure(policy, String::from("stream failed after handshake"));
        assert_eq!(health.failures, 2);
        health.success(Duration::from_millis(2));
        assert!(!health.circuit_open);
    }
}
