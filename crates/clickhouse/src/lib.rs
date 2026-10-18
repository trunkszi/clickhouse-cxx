//! Synchronous ClickHouse client with derived row batches and borrowed views.
//!
//! The public `Block` is the owned row-layer representation used by
//! `#[derive(Row)]`; `NativeBlock` is the native transport block.

extern crate self as clickhouse;

pub use clickhouse_macros::Row;
pub use clickhouse_sys as sys;

pub mod buffer_policy;
pub use buffer_policy::{BufferPolicy, MemoryBuffer};
pub mod batch;
pub mod block;
pub mod client;
pub mod column;
pub mod endpoint;
pub mod error;
pub mod native_block;
pub mod native_client;
pub mod row;
pub mod serde_bridge;
mod telemetry;
pub mod wire_block;
mod wire_error;

pub use batch::Batch;
pub use block::Block;
pub use client::Client;
pub use column::*;
pub use endpoint::{Endpoint, Scheme};
pub use error::{Error, Result};
pub use native_block::{FixedColumn, NativeBlock};
pub use native_client::NativeClient;
pub use row::{BlockView, Row};
pub use serde_bridge::SerdeBridge;
pub use wire_block::WireBlock;
pub use wire_error::{WireError, WireResult};
