//! Synchronous ClickHouse client with derived row batches and borrowed views.
//!
//! The public `Block` is the owned row-layer representation used by
//! `#[derive(Row)]`; `NativeBlock` is the `clickhouse-cpp` transport block.

extern crate self as clickhouse;

pub use clickhouse_macros::Row;
pub use clickhouse_sys as sys;

pub mod buffer_policy;
pub use buffer_policy::{BufferPolicy, MemoryBuffer};
pub mod batch;
pub mod block;
pub mod client;
pub mod column;
pub mod cpp_block;
pub mod cpp_client;
pub mod cpp_column;
mod cpp_error;
pub mod endpoint;
pub mod error;
pub mod native_block;
pub mod row;
mod telemetry;

pub use batch::Batch;
pub use block::Block;
pub use client::Client;
pub use column::Column;
pub use cpp_block::CppBlock;
pub use cpp_client::NativeClient;
pub use cpp_column::*;
pub use cpp_error::{CppError, CppResult};
pub use endpoint::{Endpoint, Scheme};
pub use error::{Error, Result};
pub use native_block::{FixedColumn, NativeBlock};
pub use row::{BlockView, Row};
