//! Owner-held circuit state; public health values remain diagnostic snapshots.
use std::time::Duration;
#[cfg(any(feature = "tcp", test))]
use std::time::Instant;

use uarch::prelude::*;

use super::{EndpointHealth, FailoverPolicy};

bitword! {
    pub(crate) struct CircuitStateWord(u64) {
        [0] HEALTHY,
        [1] OPEN,
        [2] HALF_OPEN_IN_FLIGHT,
        [32..=63] CONSECUTIVE_FAILURES: u32,
    }
}
assert_layout!(CircuitStateWord, size = 8, align = align_of::<u64>());
const _: () = uarch::layout::assert_no_padding::<CircuitStateWord>();
const _: () = uarch::layout::assert_register_leaf::<CircuitStateWord>();

impl CircuitStateWord {
    #[cfg(any(feature = "tcp", test))]
    #[hot_inline]
    pub(crate) fn available(self, health: &EndpointHealth, now: Instant) -> bool {
        if self.matches(pattern!(CircuitStateWord {
            HALF_OPEN_IN_FLIGHT
        })) {
            return false;
        }
        !self.matches(pattern!(CircuitStateWord { OPEN }))
            || health.retry_at.is_some_and(|deadline| now >= deadline)
    }

    #[cfg(any(feature = "tcp", test))]
    #[hot_inline]
    pub(crate) fn try_acquire_probe(&mut self, health: &EndpointHealth, now: Instant) -> bool {
        if !self.available(health, now) {
            return false;
        }
        if self.matches(pattern!(CircuitStateWord { OPEN })) {
            self.begin_probe();
        }
        true
    }

    #[hot_inline]
    pub(crate) fn begin_probe(&mut self) {
        *self = self.patch(patch!(CircuitStateWord {
            HALF_OPEN_IN_FLIGHT: true
        }));
    }

    #[hot_inline]
    pub(crate) fn record_success(&mut self, health: &mut EndpointHealth, elapsed: Duration) {
        health.success(elapsed);
        *self = self.patch(patch!(CircuitStateWord {
            HEALTHY: true,
            OPEN: false,
            HALF_OPEN_IN_FLIGHT: false,
            CONSECUTIVE_FAILURES: 0,
        }));
    }

    pub(crate) fn record_failure(
        &mut self,
        health: &mut EndpointHealth,
        policy: FailoverPolicy,
        error: String,
    ) {
        let failures = self.get(Self::CONSECUTIVE_FAILURES).saturating_add(1);
        health.failure(policy, error);
        *self = self.patch(if health.circuit_open {
            patch!(CircuitStateWord { OPEN: true })
        } else {
            patch!(CircuitStateWord { OPEN: false })
        });
        debug_assert_eq!(health.failures, failures);
        *self = self.patch(
            patch!(CircuitStateWord {
                HEALTHY: false,
                HALF_OPEN_IN_FLIGHT: false
            })
            .with_field(Self::CONSECUTIVE_FAILURES, u64::from(failures)),
        );
    }

    #[cfg(feature = "tcp")]
    #[hot_inline]
    pub(crate) fn record_connected(&mut self, health: &mut EndpointHealth, elapsed: Duration) {
        // A handshake releases its probe but does not forgive an application failure.
        health.latency = Some(elapsed);
        *self = self.patch(patch!(CircuitStateWord {
            HALF_OPEN_IN_FLIGHT: false
        }));
        if self.get(Self::CONSECUTIVE_FAILURES) == 0 {
            self.record_success(health, elapsed);
        } else {
            health.connected(elapsed);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/failover/circuit.rs"]
mod tests;
