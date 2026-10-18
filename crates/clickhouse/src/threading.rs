//! In-thread connection balancing and calling-thread client factories.

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

mod native;
pub use native::{ThreadLocalClient, WorkerClient};
