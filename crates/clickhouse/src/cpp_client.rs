//! Minimal synchronous Native TCP client used by the column round-trip tests.

use clickhouse_sys as sys;
use cxx::UniquePtr;

use crate::{
    cpp_block::CppBlock,
    cpp_error::{CppError, CppResult},
};

/// A synchronous Native TCP client over `clickhouse-cpp`.
///
/// The full transport, failover, and query-event surface is owned by the
/// transport tickets. This type intentionally exposes only the operations the
/// column type system needs for end-to-end verification.
pub struct NativeClient {
    handle: UniquePtr<sys::ClientHandle>,
}

impl NativeClient {
    /// Connect to a Native TCP endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn connect(host: &str, port: u16) -> CppResult<Self> {
        Ok(Self {
            handle: sys::new_client(host, port).map_err(CppError::from)?,
        })
    }

    /// Ping the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn ping(&mut self) -> CppResult<()> {
        self.handle.pin_mut().ping().map_err(CppError::from)
    }

    /// Execute a statement.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn execute(&mut self, query: &str) -> CppResult<()> {
        self.handle.pin_mut().execute(query).map_err(CppError::from)
    }

    /// Insert one block into a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn insert(&mut self, table: &str, block: &CppBlock) -> CppResult<()> {
        self.handle
            .pin_mut()
            .insert(table, block.cpp_handle())
            .map_err(CppError::from)
    }

    /// Select the first non-empty block of a query.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn select_one(&mut self, query: &str) -> CppResult<Option<CppBlock>> {
        let handle = self
            .handle
            .pin_mut()
            .select_one(query)
            .map_err(CppError::from)?;
        if handle.is_null() {
            Ok(None)
        } else {
            CppBlock::from_cpp(handle).map(Some)
        }
    }
}
