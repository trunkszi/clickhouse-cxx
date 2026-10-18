//! Optional cold-path telemetry.
//!
//! Logging is compiled only with the `tracing` feature and is restricted to
//! connection lifecycle and statement boundaries. Hot data paths never log.

#[cfg(feature = "tracing")]
macro_rules! control_log {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*)
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! control_log {
    ($($arg:tt)*) => {{}};
}

pub(crate) use control_log;

/// Emit one structured warning for an endpoint transition.
pub(crate) fn warn_failover(event: &crate::FailoverEvent<'_>) {
    #[cfg(feature = "tracing")]
    tracing::warn!(target:"clickhouse::failover",old_index=event.old_index,new_index=event.new_index,old_endpoint=%event.old_endpoint,new_endpoint=%event.new_endpoint,"endpoint failover");
    #[cfg(not(feature = "tracing"))]
    {
        let _event = event;
    }
}
