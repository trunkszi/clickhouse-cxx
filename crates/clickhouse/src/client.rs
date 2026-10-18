//! Synchronous Native connection lifecycle, block transport, and query dispatch.

use std::marker::PhantomData;

use cxx::UniquePtr;

use crate::{
    BlockStream, Error, ExternalTable, Inserter, NativeBlock, Query, Result, ServerInfo,
    callbacks::{self, Frame},
    config::ClientConfig,
    endpoint::Endpoint,
    sys,
    telemetry::control_log,
    threading::{ThreadLocalClient, WorkerBalance, WorkerClient},
};

/// A client admits at most one borrowed interactive session at a time.
pub struct Client {
    pub(crate) inner: UniquePtr<sys::ffi::Client>,
    managed: Managed,
    usable: bool,
    cleanup_error: Option<Error>,
    /// The native client owns sockets and blocks that must stay on one thread.
    _not_send: PhantomData<*const ()>,
}

impl Client {
    /// Open a Native TCP connection with default credentials.
    ///
    /// # Errors
    /// Returns endpoint, handshake, authentication, or connection errors.
    pub fn connect(endpoint: Endpoint, user: &str, password: &str, database: &str) -> Result<Self> {
        Self::new(
            ClientConfig::from_endpoints(vec![endpoint])?
                .with_user(user)
                .with_password(password)
                .with_database(database),
        )
    }

    /// Connect with an explicit endpoint list and full connection configuration.
    ///
    /// # Errors
    /// Rejects malformed addresses or connection and handshake errors.
    pub fn connect_with_config(endpoint: &str, mut config: ClientConfig<'_>) -> Result<Self> {
        config.endpoints = Endpoint::parse_list(endpoint)?;
        Self::new(config)
    }

    /// Endpoint used by this client.
    pub fn endpoint(&self) -> &Endpoint {
        &self.managed.endpoints[self.managed.current]
    }

    pub(crate) fn ready(&self) -> Result<()> {
        if !self.usable {
            return Err(Error::ConnectionPoisoned);
        }
        if self.cleanup_error.is_some() {
            return Err(Error::PendingCleanup);
        }
        Ok(())
    }

    /// Check the server on the existing idle connection.
    ///
    /// # Errors
    /// Returns pending cleanup, poisoned connection, or transport errors.
    pub fn ping(&mut self) -> Result<()> {
        self.ready()?;
        self.prepare()?;
        sys::ffi::ping(self.inner.pin_mut()).map_err(|error| self.operation_error(error))
    }

    /// Explicitly reset and acknowledge any previous cleanup failure.
    ///
    /// # Errors
    /// If reconnecting fails, the client remains poisoned.
    pub fn reset_connection(&mut self) -> Result<()> {
        self.usable = false;
        match self.managed.dial(true) {
            Ok(inner) => {
                self.inner = inner;
                self.usable = true;
                self.cleanup_error = None;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Retrieve a Drop cleanup failure; taking it acknowledges the diagnostic.
    /// A failed reconnect still requires an explicit successful reset.
    pub fn take_cleanup_error(&mut self) -> Option<Error> {
        self.cleanup_error.take()
    }

    /// Whether the underlying connection has not been poisoned by a failed reset.
    pub fn is_usable(&self) -> bool {
        self.usable
    }

    pub(crate) fn recover(&mut self) {
        self.usable = false;
        match self.managed.dial(true) {
            Ok(inner) => {
                self.inner = inner;
                self.usable = true;
            }
            Err(error) => self.cleanup_error = Some(error),
        }
    }

    pub(crate) fn record_cleanup(&mut self, error: Error) {
        self.cleanup_error = Some(match self.cleanup_error.take() {
            Some(recovery) => Error::Cleanup {
                operation: Box::new(error),
                recovery: Box::new(recovery),
            },
            None => error,
        });
    }

    /// Inspect handshake information from the connected server.
    pub fn get_server_info(&self) -> ServerInfo {
        let info = sys::ffi::get_server_info(&self.inner);
        ServerInfo {
            name: info.name,
            timezone: info.timezone,
            display_name: info.display_name,
            version_major: info.version_major,
            version_minor: info.version_minor,
            version_patch: info.version_patch,
            revision: info.revision,
        }
    }

    /// Handshake information from the connected server.
    pub fn server_info(&self) -> ServerInfo {
        self.get_server_info()
    }

    /// Inspect the statically linked C++ client version.
    pub fn get_version() -> sys::ffi::Version {
        sys::ffi::get_version()
    }

    /// Statically linked C++ client version.
    pub fn server_version() -> sys::ffi::Version {
        Self::get_version()
    }

    /// Begin a bounded-memory, block-at-a-time query.
    ///
    /// # Errors
    /// Returns request, server, callback, or connection errors; failures reset the connection.
    pub fn select_stream<'a>(&'a mut self, query: impl Into<Query<'a>>) -> Result<BlockStream<'a>> {
        self.ready()?;
        self.prepare()?;
        let mut query = query.into();
        let mut native = callbacks::native_query(&query);
        let mask = callbacks::event_mask(&query);
        let mut frame = Frame::new(&mut query, None);
        // SAFETY: frame is uniquely borrowed for this synchronous call; C++
        // EventScope clears its callback pointer before the call returns.
        let result = unsafe {
            sys::ffi::begin_select(
                self.inner.pin_mut(),
                native.pin_mut(),
                mask,
                frame.pointer(),
                callbacks::dispatch,
            )
        };
        if let Err(error) = frame.finish(result) {
            self.recover();
            return Err(error);
        }
        Ok(BlockStream {
            client: self,
            query,
            active: true,
        })
    }

    /// Execute a statement while consuming all server output and events.
    ///
    /// # Errors
    /// Returns any query, server, callback, or connection error.
    pub fn execute<'a>(&mut self, query: impl Into<Query<'a>>) -> Result<()> {
        let query = query.into();
        control_log!("execute statement_bytes={}", query.sql.len());
        for block in self.select_stream(query.shorten())? {
            block?;
        }
        Ok(())
    }

    /// Collect query output; use select_stream when output size is unbounded.
    ///
    /// # Errors
    /// Returns any query, server, callback, or connection error.
    pub fn query_blocks<'a>(&mut self, query: impl Into<Query<'a>>) -> Result<Vec<NativeBlock>> {
        self.query_native_blocks(query)
    }

    pub(crate) fn query_native_blocks<'a>(
        &mut self,
        query: impl Into<Query<'a>>,
    ) -> Result<Vec<NativeBlock>> {
        let query = query.into();
        control_log!("query statement_bytes={}", query.sql.len());
        let mut blocks = Vec::new();
        for block in self.select_stream(query.shorten())? {
            blocks.push(block?);
        }
        Ok(blocks)
    }

    /// Deliver each result block to a caller callback without accumulating them.
    ///
    /// Returns the number of blocks delivered.
    ///
    /// # Errors
    /// Returns any query, server, callback, or connection error, including a
    /// callback error, which stops the stream and cancels the server query.
    pub fn query_blocks_for_each<'a, F>(
        &mut self,
        query: impl Into<Query<'a>>,
        mut on_block: F,
    ) -> Result<usize>
    where
        F: FnMut(NativeBlock) -> Result<()>,
    {
        let mut delivered = 0;
        for block in self.select_stream(query.into().shorten())? {
            on_block(block?)?;
            delivered += 1;
        }
        Ok(delivered)
    }

    /// Append each result block to caller-supplied storage.
    ///
    /// Returns the number of blocks appended.
    ///
    /// # Errors
    /// Returns any query, server, or connection error.
    pub fn query_blocks_into<'a>(
        &mut self,
        query: impl Into<Query<'a>>,
        dst: &mut Vec<NativeBlock>,
    ) -> Result<usize> {
        let mut appended = 0;
        for block in self.select_stream(query.into().shorten())? {
            dst.push(block?);
            appended += 1;
        }
        Ok(appended)
    }

    /// Insert one block into a table.
    ///
    /// # Errors
    /// Returns [`Error::Native`] when the server rejects the insert.
    pub fn insert_block(&mut self, table: &str, block: &mut NativeBlock) -> Result<()> {
        self.ready()?;
        self.prepare()?;
        control_log!("insert table={table} rows={}", block.rows());
        block.refresh()?;
        sys::ffi::insert_block(self.inner.pin_mut(), table, block.inner())
            .map_err(|error| self.operation_error(error))
    }

    /// Stop a select when the callback returns false, canceling and draining it.
    ///
    /// # Errors
    /// Returns query or cancellation errors.
    pub fn select_cancelable<'a, F>(
        &mut self,
        query: impl Into<Query<'a>>,
        mut on_block: F,
    ) -> Result<()>
    where
        F: FnMut(NativeBlock) -> bool,
    {
        let mut stream = self.select_stream(query.into().shorten())?;
        while let Some(block) = stream.next() {
            let block = block?;
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| on_block(block))) {
                Ok(true) => {}
                Ok(false) => return stream.cancel(),
                Err(_) => {
                    stream.cancel()?;
                    return Err(Error::CallbackPanicked);
                }
            }
        }
        Ok(())
    }

    /// Execute a query with borrowed external tables and collect its blocks.
    ///
    /// # Errors
    /// Returns external schema, server, callback, or connection errors.
    pub fn select_with_external_data<'a>(
        &mut self,
        query: impl Into<Query<'a>>,
        tables: &[ExternalTable<'_>],
    ) -> Result<Vec<NativeBlock>> {
        let mut blocks = Vec::new();
        self.select_with_external_data_cancelable(query, tables, |block| {
            blocks.push(block);
            true
        })?;
        Ok(blocks)
    }

    /// Cancel external-data execution on the server when a callback returns false.
    ///
    /// # Errors
    /// Returns invalid table names, server, callback, or transport errors.
    pub fn select_with_external_data_cancelable<'a, F>(
        &mut self,
        query: impl Into<Query<'a>>,
        tables: &[ExternalTable<'_>],
        mut on_block: F,
    ) -> Result<()>
    where
        F: FnMut(NativeBlock) -> bool,
    {
        self.ready()?;
        self.prepare()?;
        if tables
            .iter()
            .enumerate()
            .any(|(i, t)| t.name.is_empty() || tables[..i].iter().any(|other| other.name == t.name))
        {
            return Err(Error::Invalid(
                "external table names must be nonempty and unique".into(),
            ));
        }
        let mut query = query.into();
        let mut native = callbacks::native_query(&query);
        let mask = callbacks::event_mask(&query);
        sys::ffi::clear_external(self.inner.pin_mut());
        for table in tables {
            sys::ffi::add_external(self.inner.pin_mut(), &table.name, table.data.inner());
        }
        let mut frame = Frame::new(&mut query, Some(&mut on_block));
        // SAFETY: context and borrowed tables outlive this blocking call; C++
        // never retains the context after EventScope exits, including on failure.
        let result = unsafe {
            sys::ffi::select_external(
                self.inner.pin_mut(),
                native.pin_mut(),
                mask,
                frame.pointer(),
                callbacks::dispatch,
            )
        };
        sys::ffi::clear_external(self.inner.pin_mut());
        let result = frame.finish(result);
        if result.is_err() {
            self.recover();
        }
        result
    }

    /// Begin one streaming INSERT; all blocks belong to the same statement.
    ///
    /// # Errors
    /// Event handlers are rejected before sending; schema and connection errors reset the
    /// connection.
    pub fn inserter<'a>(&'a mut self, query: impl Into<Query<'a>>) -> Result<Inserter<'a>> {
        self.ready()?;
        self.prepare()?;
        let query = query.into();
        if query.has_event_callbacks() {
            return Err(Error::InsertEventCallbacks);
        }
        let native = callbacks::native_query(&query);
        match sys::ffi::begin_insert(self.inner.pin_mut(), &native) {
            Ok(inner) => Ok(Inserter {
                client: self,
                schema: NativeBlock::from_inner(inner),
                active: true,
            }),
            Err(error) => {
                self.recover();
                Err(error.into())
            }
        }
    }
}

/// Builder shared by single clients, worker pools, and thread-local factories.
#[derive(Default)]
pub struct ClientBuilder {
    endpoints: Option<Vec<Endpoint>>,
    config: ClientConfig<'static>,
}

impl ClientBuilder {
    /// Set one target endpoint.
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoints = Some(vec![endpoint]);
        self
    }

    /// Set the ordered endpoint list used for initial striping and failover.
    pub fn endpoints(mut self, endpoints: Vec<Endpoint>) -> Self {
        self.endpoints = Some(endpoints);
        self
    }

    /// Replace the connection configuration.
    pub fn config(mut self, config: ClientConfig<'static>) -> Self {
        self.config = config;
        self
    }

    /// Set the number of local connection slots per calling thread.
    pub fn workers(mut self, workers: usize) -> Self {
        self.config.workers = workers;
        self
    }

    /// Set the in-thread load-balancing strategy.
    pub fn balance(mut self, balance: WorkerBalance) -> Self {
        self.config.balance = balance;
        self
    }

    /// Connect one client on the calling thread.
    ///
    /// # Errors
    /// Returns [`Error::Invalid`] when no endpoint is set, or a connection error.
    pub fn connect(self) -> Result<Client> {
        Client::new(self.into_config()?)
    }

    /// Create a worker handle whose slots are opened lazily on each calling thread.
    ///
    /// # Errors
    /// Returns [`Error::Invalid`] when no endpoint is set or the configuration
    /// is invalid.
    pub fn connect_workers(self) -> Result<WorkerClient> {
        WorkerClient::new(self.into_config()?)
    }

    /// Create a factory that keeps one client per calling OS thread.
    ///
    /// # Errors
    /// Returns [`Error::Invalid`] when no endpoint is set or the configuration
    /// is invalid.
    pub fn thread_local(self) -> Result<ThreadLocalClient> {
        ThreadLocalClient::new(self.into_config()?)
    }

    fn into_config(mut self) -> Result<ClientConfig<'static>> {
        let endpoints = self
            .endpoints
            .take()
            .ok_or_else(|| Error::Invalid("client endpoint is required".into()))?;
        if endpoints.is_empty() {
            return Err(Error::Invalid("endpoint list is empty".into()));
        }
        self.config.endpoints = endpoints;
        Ok(self.config)
    }
}

impl Client {
    /// Create a worker handle over the configured endpoints and slot count.
    ///
    /// Connections are opened lazily on each calling thread.
    ///
    /// # Errors
    /// Returns a configuration error when the endpoint list or timeout is invalid.
    pub fn connect_workers(config: ClientConfig<'_>) -> Result<WorkerClient> {
        WorkerClient::new(config)
    }

    /// Create a factory that keeps one client per calling OS thread.
    ///
    /// # Errors
    /// Returns a configuration error when the endpoint list or timeout is invalid.
    pub fn thread_local(config: ClientConfig<'_>) -> Result<ThreadLocalClient> {
        ThreadLocalClient::new(config)
    }
}

mod managed;
use managed::Managed;
