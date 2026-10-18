//! Synchronous ClickHouse client with derived row batches and borrowed views.
//!
//! The public `Block` is the owned row-layer representation used by
//! `#[derive(Row)]`; `NativeBlock` is the `clickhouse-cpp` transport block.

extern crate self as clickhouse;

pub use clickhouse_macros::Row;
pub use clickhouse_sys as sys;

pub mod batch;
pub mod block;
pub mod client;
pub mod column;
pub mod endpoint;
pub mod error;
pub mod native_block;
pub mod row;
mod telemetry;

pub use batch::Batch;
pub use block::Block;
pub use client::Client;
pub use column::Column;
pub use endpoint::{Endpoint, Scheme};
pub use error::{Error, Result};
pub use native_block::{FixedColumn, NativeBlock};
pub use row::{BlockView, Row};
