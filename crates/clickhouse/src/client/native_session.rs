//! Synchronous Native connection lifecycle, block transport, and query dispatch.

use std::marker::PhantomData;

use cxx::UniquePtr;

use super::{Backend, Client};
use crate::{
    BlockStream, Error, ExternalTable, Inserter, NativeBlock, Query, Result, ServerInfo,
    callbacks::{self, Frame},
    config::ClientConfig,
    endpoint::Endpoint,
    sys,
    telemetry::control_log,
};

/// A client admits at most one borrowed interactive session at a time.
pub(crate) struct NativeSession {
    pub(crate) inner: UniquePtr<sys::ffi::Client>,
    managed: Managed,
    server_info: ServerInfo,
    usable: bool,
    cleanup_error: Option<Error>,
    /// The native client owns sockets and blocks that must stay on one thread.
    _not_send: PhantomData<*const ()>,
}

impl NativeSession {
    /// Open a Native TCP connection with explicit credentials.
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
                self.server_info = read_server_info(&inner);
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

    /// Reconnect without replaying a failed request or counting application rejections.
    pub(crate) fn recover(&mut self, error: &Error) {
        if matches!(error, Error::Native { .. } | Error::Io(_)) {
            self.record_transport_failure(error);
        }
        self.usable = false;
        match self.managed.dial(true) {
            Ok(inner) => {
                self.server_info = read_server_info(&inner);
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

    /// Recover structured INSERT rejections before inspecting transport circuit state.
    pub(crate) fn insert_error(&mut self, error: cxx::Exception) -> Error {
        let event = sys::ffi::take_insert_exception(self.inner.pin_mut());
        if event.is_null() {
            return Error::from(error);
        }
        let fields = sys::ffi::event_exception(&event);
        Error::Server(crate::ServerException {
            code: fields.code,
            name: fields.name,
            display_text: fields.display_text,
            stack_trace: fields.stack_trace,
        })
    }

    /// Inspect handshake information from the connected server.
    pub fn get_server_info(&self) -> ServerInfo {
        self.server_info.clone()
    }

    /// Borrow metadata from the latest successful Native handshake.
    /// # Errors
    /// Returns a poisoned-connection error if recovery did not establish a session.
    pub(crate) fn try_server_info(&self) -> Result<&ServerInfo> {
        self.ready()?;
        Ok(&self.server_info)
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
        let mut query = query.into();
        validate_query(&query)?;
        self.ready()?;
        self.prepare()?;
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
            self.recover(&error);
            return Err(error);
        }
        Ok(BlockStream {
            client: self,
            query,
            active: true,
            _not_send: PhantomData,
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

    /// Insert one block into a table.
    ///
    /// # Errors
    /// Returns structured server rejections or transport failures; failed sessions are reset.
    pub fn insert_block(&mut self, table: &str, block: &mut NativeBlock) -> Result<()> {
        self.ready()?;
        self.prepare()?;
        control_log!("insert table={table} rows={}", block.rows());
        block.refresh()?;
        match sys::ffi::insert_block_checked(self.inner.pin_mut(), table, block.inner()) {
            Ok(()) => {
                self.operation_succeeded();
                Ok(())
            }
            Err(error) => {
                let error = self.insert_error(error);
                self.recover(&error);
                Err(error)
            }
        }
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
        validate_query(&query)?;
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
        if let Err(error) = &result {
            self.recover(error);
        } else {
            self.operation_succeeded();
        }
        result
    }

    /// Begin one streaming INSERT; all blocks belong to the same statement.
    ///
    /// # Errors
    /// Event handlers are rejected before sending; schema and connection errors reset the
    /// connection.
    pub fn inserter<'a>(&'a mut self, query: impl Into<Query<'a>>) -> Result<Inserter<'a>> {
        let query = query.into();
        validate_query(&query)?;
        self.ready()?;
        self.prepare()?;
        if query.has_event_callbacks() {
            return Err(Error::InsertEventCallbacks);
        }
        let native = callbacks::native_query(&query);
        match sys::ffi::begin_insert(self.inner.pin_mut(), &native) {
            Ok(inner) => Ok(Inserter {
                client: self,
                schema: NativeBlock::from_inner(inner),
                active: true,
                _not_send: PhantomData,
            }),
            Err(error) => {
                let error = self.insert_error(error);
                self.recover(&error);
                Err(error)
            }
        }
    }
}

mod managed;
use managed::Managed;

/// Copy authenticated metadata while its Native connection is alive.
fn read_server_info(inner: &UniquePtr<sys::ffi::Client>) -> ServerInfo {
    let info = sys::ffi::get_server_info(inner);
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

/// Reject HTTP-only per-request options instead of discarding them on Native transport.
fn validate_query(query: &Query<'_>) -> Result<()> {
    if query.compression_override().is_some() || query.deduplication_token_value().is_some() {
        return Err(Error::unsupported(
            "per-query compression and deduplication tokens require an HTTP endpoint; configure \
             Native compression on ClientConfig and deduplication with query settings",
        ));
    }
    Ok(())
}

impl Client {
    /// Reset a Native connection and acknowledge pending cleanup failures.
    /// # Errors
    /// HTTP transport is unsupported; failed Native reconnects leave the connection poisoned.
    pub fn reset_connection(&mut self) -> Result<()> {
        match &mut self.backend {
            Backend::Native(session) => session.reset_connection(),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native connection reset requires a Native endpoint",
            )),
        }
    }
    /// Retrieve a contained Native session cleanup failure.
    pub fn take_cleanup_error(&mut self) -> Option<Error> {
        match &mut self.backend {
            Backend::Native(session) => session.take_cleanup_error(),
            #[cfg(feature = "http")]
            Backend::Http(_) => None,
        }
    }
    /// Whether the Native connection is usable; HTTP state is managed by its request API.
    pub fn is_usable(&self) -> bool {
        match &self.backend {
            Backend::Native(session) => session.is_usable(),
            #[cfg(feature = "http")]
            Backend::Http(_) => true,
        }
    }
    /// Inspect the authenticated Native handshake metadata.
    /// # Panics
    /// HTTP endpoints have no Native handshake and must not use this interface.
    pub fn get_server_info(&self) -> ServerInfo {
        match &self.backend {
            Backend::Native(session) => session.get_server_info(),
            #[cfg(feature = "http")]
            Backend::Http(_) => panic!("Native server information requires a Native endpoint"),
        }
    }
    /// Inspect the authenticated Native handshake metadata.
    /// # Panics
    /// HTTP endpoints have no Native handshake and must not use this interface.
    pub fn server_info(&self) -> ServerInfo {
        match &self.backend {
            Backend::Native(session) => session.server_info(),
            #[cfg(feature = "http")]
            Backend::Http(_) => panic!("Native server information requires a Native endpoint"),
        }
    }
    /// Statically linked Native client library version.
    pub fn get_version() -> crate::sys::ffi::Version {
        NativeSession::get_version()
    }
    /// Statically linked Native client library version.
    pub fn server_version() -> crate::sys::ffi::Version {
        NativeSession::server_version()
    }
    /// Begin an actual bounded-memory Native query session.
    /// # Errors
    /// HTTP endpoints are unsupported; Native server, callback, and connection failures propagate.
    pub fn select_stream<'a>(&'a mut self, query: impl Into<Query<'a>>) -> Result<BlockStream<'a>> {
        match &mut self.backend {
            Backend::Native(session) => session.select_stream(query),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native block streaming requires a Native endpoint; use select_http_stream for \
                 buffered HTTP blocks",
            )),
        }
    }
    /// Begin a Native INSERT statement with server-owned schema validation.
    /// # Errors
    /// HTTP endpoints are unsupported; invalid callbacks, schema, and transport failures propagate.
    pub fn inserter<'a>(&'a mut self, query: impl Into<Query<'a>>) -> Result<Inserter<'a>> {
        match &mut self.backend {
            Backend::Native(session) => session.inserter(query),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native insert streaming requires a Native endpoint; use http_inserter for HTTP \
                 staging",
            )),
        }
    }
    /// Stop, cancel, and drain a Native query when its callback returns false.
    /// # Errors
    /// Native callback or cancellation errors propagate; HTTP endpoints are unsupported.
    pub fn select_cancelable<'a, F>(
        &mut self,
        query: impl Into<Query<'a>>,
        on_block: F,
    ) -> Result<()>
    where
        F: FnMut(NativeBlock) -> bool,
    {
        match &mut self.backend {
            Backend::Native(session) => session.select_cancelable(query, on_block),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native cancellation requires a Native endpoint",
            )),
        }
    }
    /// Execute a Native query with borrowed external tables.
    /// # Errors
    /// Native schema or query failures propagate; HTTP endpoints are unsupported.
    pub fn select_with_external_data<'a>(
        &mut self,
        query: impl Into<Query<'a>>,
        tables: &[ExternalTable<'_>],
    ) -> Result<Vec<NativeBlock>> {
        match &mut self.backend {
            Backend::Native(session) => session.select_with_external_data(query, tables),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native external tables require a Native endpoint",
            )),
        }
    }
    /// Cancel a Native external-table query when its callback returns false.
    /// # Errors
    /// Native callback or cancellation errors propagate; HTTP endpoints are unsupported.
    pub fn select_with_external_data_cancelable<'a, F>(
        &mut self,
        query: impl Into<Query<'a>>,
        tables: &[ExternalTable<'_>],
        on_block: F,
    ) -> Result<()>
    where
        F: FnMut(NativeBlock) -> bool,
    {
        match &mut self.backend {
            Backend::Native(session) => {
                session.select_with_external_data_cancelable(query, tables, on_block)
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native external tables require a Native endpoint",
            )),
        }
    }
}
