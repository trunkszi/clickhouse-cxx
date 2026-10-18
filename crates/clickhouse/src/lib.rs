//! Synchronous ClickHouse client with derived row batches, streaming queries,
//! and typed server events.
//!
//! The public `Block` is the owned row-layer representation used by
//! `#[derive(Row)]`; `NativeBlock` is the native transport block.

extern crate self as clickhouse;

pub use clickhouse_macros::Row;
#[cfg(feature = "tcp")]
pub use clickhouse_sys as sys;

pub mod buffer_policy;
pub use buffer_policy::{BufferPolicy, MemoryBuffer};
pub mod batch;
pub mod block;
#[cfg(feature = "tcp")]
mod callbacks;
pub mod client;
pub mod column;
pub mod endpoint;
pub mod error;
#[cfg(feature = "tcp")]
pub mod native_block;
#[cfg(feature = "tcp")]
pub mod native_client;
pub mod query;
pub mod row;
pub mod serde_bridge;
#[cfg(feature = "tcp")]
pub mod streaming;
mod telemetry;
pub mod threading;
#[cfg(feature = "tcp")]
pub mod wire_block;
#[cfg(feature = "tcp")]
mod wire_error;

pub use batch::Batch;
pub use block::Block;
pub use client::{Client, ClientBuilder};
pub use column::*;
pub use endpoint::{Endpoint, Scheme};
pub use error::{Error, Result};
#[cfg(feature = "tcp")]
pub use native_block::{FixedColumn, NativeBlock};
#[cfg(feature = "tcp")]
pub use native_client::NativeClient;
pub use query::{
    CUSTOM, CompressionMethod, IMPORTANT, OBSOLETE, Profile, Progress, Query, QueryId, QueryParams,
    QuerySettings, QuerySettingsField, ServerException, ServerInfo, TracingContext,
};
pub use row::{BlockView, Row};
pub use serde_bridge::SerdeBridge;
#[cfg(feature = "tcp")]
pub use streaming::{BlockStream, Inserter};
pub use threading::{ThreadLocalClient, WorkerBalance, WorkerClient};
#[cfg(feature = "tcp")]
pub use wire_block::WireBlock;
#[cfg(feature = "tcp")]
pub use wire_error::{WireError, WireResult};

pub mod config;
pub mod failover;
pub mod tls;
pub use config::ClientConfig;
pub use failover::{EndpointHealth, EndpointSelection, FailoverEvent, FailoverPolicy};
#[cfg(feature = "tcp")]
pub use query::ExternalTable;
pub use query::{Compression, HttpVersionChoice};
pub use tls::TlsSettings;
pub mod arrow;
pub use arrow::ArrowBlockView;
pub mod native_format;
pub use native_format::{HTTP_NATIVE_REVISION, decode_blocks, encode_block};
#[cfg(feature = "http")]
pub mod inserter;
#[cfg(feature = "http")]
pub mod stream;
#[cfg(feature = "http")]
pub(crate) mod transport;
pub(crate) mod type_layout;
pub(crate) mod varint;
#[cfg(feature = "http")]
pub use inserter::HttpInserter;
#[cfg(feature = "http")]
pub use stream::HttpBlockStream;
#[cfg(feature = "http")]
pub use transport::HttpPreferences;
