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
