//! Synchronous Native TCP client.

use cxx::UniquePtr;

use crate::{endpoint::Endpoint, error::Error, native_block::NativeBlock, sys};

/// A synchronous Native TCP or TCPS client with managed endpoint failover.
/// Connections remain bound to their creating thread and cannot be sent or shared.
pub struct Client {
    inner: UniquePtr<sys::ffi::Client>,
    managed: Managed,
    thread_bound: std::marker::PhantomData<*const ()>,
}

impl Client {
    /// Open a Native TCP connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the native client cannot construct the
    /// client or reach the endpoint.
    pub fn connect(
        endpoint: Endpoint,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, Error> {
        Self::new(
            crate::ClientConfig::from_endpoints(vec![endpoint])?
                .with_user(user)
                .with_password(password)
                .with_database(database),
        )
    }

    /// Endpoint used by this client.
    pub fn endpoint(&self) -> &Endpoint {
        &self.managed.endpoints[self.managed.current]
    }

    /// Ping the server.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server is unreachable.
    pub fn ping(&mut self) -> Result<(), Error> {
        self.prepare()?;
        sys::ffi::ping(self.inner.pin_mut()).map_err(|error| self.operation_error(error))
    }

    /// Execute a statement such as a DDL query.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server rejects the statement.
    pub fn execute(&mut self, sql: &str) -> Result<(), Error> {
        self.prepare()?;
        sys::ffi::execute(self.inner.pin_mut(), sql).map_err(|error| self.operation_error(error))
    }

    /// Insert one block into a table.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server rejects the insert.
    pub fn insert_block(&mut self, table: &str, block: &mut NativeBlock) -> Result<(), Error> {
        self.prepare()?;
        block.refresh()?;
        sys::ffi::insert_block(self.inner.pin_mut(), table, block.inner())
            .map_err(|error| self.operation_error(error))
    }

    /// Execute a select and collect every returned block.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Native`] when the server rejects the query.
    pub fn query_blocks(&mut self, sql: &str) -> Result<Vec<NativeBlock>, Error> {
        self.prepare()?;
        let list = sys::ffi::query_blocks(self.inner.pin_mut(), sql)
            .map_err(|error| self.operation_error(error))?;
        let count = sys::ffi::block_list_len(&list);
        let mut blocks = Vec::with_capacity(count);
        for index in 0..count {
            blocks.push(NativeBlock::from_inner(sys::ffi::block_list_get(
                &list, index,
            )));
        }
        Ok(blocks)
    }
}

mod managed;
use managed::Managed;
