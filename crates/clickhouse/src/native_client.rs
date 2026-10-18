//! Minimal synchronous Native TCP client used by the column round-trip tests.

use std::marker::PhantomData;

use clickhouse_sys as sys;
use cxx::UniquePtr;

use crate::{
    wire_block::WireBlock,
    wire_error::{WireError, WireResult},
};

/// A synchronous Native TCP client over the native ClickHouse client library.
///
/// This thread-bound session reads and writes native column blocks directly.
/// It cannot be sent to another thread or shared across threads.
pub struct NativeClient {
    handle: UniquePtr<sys::ClientHandle>,
    /// The native client and its blocks are single-threaded.
    _not_send: PhantomData<*const ()>,
}

impl NativeClient {
    /// Connect to a Native TCP endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn connect(host: &str, port: u16) -> WireResult<Self> {
        Ok(Self {
            handle: sys::new_client(host, port).map_err(WireError::from)?,
            _not_send: PhantomData,
        })
    }

    /// Ping the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn ping(&mut self) -> WireResult<()> {
        self.handle.pin_mut().ping().map_err(WireError::from)
    }

    /// Execute a statement.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn execute(&mut self, query: &str) -> WireResult<()> {
        self.handle
            .pin_mut()
            .execute(query)
            .map_err(WireError::from)
    }

    /// Insert one block into a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn insert(&mut self, table: &str, block: &WireBlock) -> WireResult<()> {
        self.handle
            .pin_mut()
            .insert(table, block.wire_handle())
            .map_err(WireError::from)
    }

    /// Select the first non-empty block of a query.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn select_one(&mut self, query: &str) -> WireResult<Option<WireBlock>> {
        let handle = self
            .handle
            .pin_mut()
            .select_one(query)
            .map_err(WireError::from)?;
        if handle.is_null() {
            Ok(None)
        } else {
            WireBlock::from_wire(handle).map(Some)
        }
    }
}
