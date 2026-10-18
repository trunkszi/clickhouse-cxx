//! Synchronous ClickHouse client.
//!
//! Ticket 01 exposes the Native TCP tracer bullet: ping, DDL execution,
//! block insert, block selection, fixed-width column slices, and row-buffer
//! capacity retention.

pub use clickhouse_macros::Row;
pub use clickhouse_sys as sys;

pub mod block;
pub mod client;
pub mod endpoint;
pub mod error;
mod telemetry;

pub use block::{Block, FixedColumn};
pub use client::Client;
pub use endpoint::{Endpoint, Scheme};
pub use error::Error;
