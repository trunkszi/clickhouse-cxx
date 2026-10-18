//! Synchronous ClickHouse client with derived row batches, streaming queries,
//! and typed server events.
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
mod callbacks;
pub mod client;
pub mod column;
pub mod endpoint;
pub mod error;
pub mod native_block;
pub mod native_client;
pub mod query;
pub mod row;
pub mod serde_bridge;
pub mod streaming;
mod telemetry;
pub mod threading;
pub mod wire_block;
mod wire_error;

pub use batch::Batch;
pub use block::Block;
pub use client::{Client, ClientBuilder};
pub use column::*;
pub use endpoint::{Endpoint, Scheme};
pub use error::{Error, Result};
pub use native_block::{FixedColumn, NativeBlock};
pub use native_client::NativeClient;
pub use query::{
    CUSTOM, CompressionMethod, ExternalTable, IMPORTANT, OBSOLETE, Profile, Progress, Query,
    QueryId, QueryParams, QuerySettings, QuerySettingsField, ServerException, ServerInfo,
    TracingContext,
};
pub use row::{BlockView, Row};
pub use serde_bridge::SerdeBridge;
pub use streaming::{BlockStream, Inserter};
pub use threading::{ThreadLocalClient, WorkerBalance, WorkerClient};
pub use wire_block::WireBlock;
pub use wire_error::{WireError, WireResult};

pub mod config;
pub mod failover;
pub mod tls;
pub use config::ClientConfig;
pub use failover::{EndpointHealth, EndpointSelection, FailoverEvent, FailoverPolicy};
pub use tls::TlsSettings;
