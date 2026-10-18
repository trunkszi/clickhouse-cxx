//! Synchronous Native TCP client.

use cxx::UniquePtr;

use crate::{block::Block, endpoint::Endpoint, error::Error, sys, telemetry::control_log};

/// A synchronous client bound to one `tcp://` endpoint.
pub struct Client {
    inner: UniquePtr<sys::ffi::Client>,
    endpoint: Endpoint,
}

impl Client {
    /// Open a Native TCP connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when `clickhouse-cpp` cannot construct the
    /// client or reach the endpoint.
    pub fn connect(
        endpoint: Endpoint,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, Error> {
        control_log!(
            "connect endpoint={}:{} database={database}",
            endpoint.host(),
            endpoint.port()
        );
        let inner =
            sys::ffi::new_client(endpoint.host(), endpoint.port(), user, password, database)
                .map_err(|error| Error::native(format!("connect failed: {error}")))?;
        Ok(Self { inner, endpoint })
    }

    /// Endpoint used by this client.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Ping the server.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server is unreachable.
    pub fn ping(&mut self) -> Result<(), Error> {
        sys::ffi::ping(self.inner.pin_mut())
            .map_err(|error| Error::native(format!("ping failed: {error}")))
    }

    /// Execute a statement such as a DDL query.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server rejects the statement.
    pub fn execute(&mut self, sql: &str) -> Result<(), Error> {
        control_log!("execute statement_bytes={}", sql.len());
        sys::ffi::execute(self.inner.pin_mut(), sql)
            .map_err(|error| Error::native(format!("execute failed: {error}")))
    }

    /// Insert one block into a table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server rejects the insert.
    pub fn insert_block(&mut self, table: &str, block: &mut Block) -> Result<(), Error> {
        control_log!("insert table={table} rows={}", block.rows());
        block.refresh()?;
        sys::ffi::insert_block(self.inner.pin_mut(), table, block.inner())
            .map_err(|error| Error::native(format!("insert failed: {error}")))
    }

    /// Execute a select and collect every returned block.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server rejects the query.
    pub fn query_blocks(&mut self, sql: &str) -> Result<Vec<Block>, Error> {
        control_log!("query statement_bytes={}", sql.len());
        let list = sys::ffi::query_blocks(self.inner.pin_mut(), sql)
            .map_err(|error| Error::native(format!("query failed: {error}")))?;
        let count = sys::ffi::block_list_len(&list);
        let mut blocks = Vec::with_capacity(count);
        for index in 0..count {
            blocks.push(Block::from_inner(sys::ffi::block_list_get(&list, index)));
        }
        Ok(blocks)
    }
}
