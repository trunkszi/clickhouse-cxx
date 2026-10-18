//! Native protocol APIs retain their interactive session and callback contracts.
use super::{Backend, Client};
use crate::{BlockStream, Error, ExternalTable, Inserter, NativeBlock, Query, Result, ServerInfo};
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
        super::native_session::NativeSession::get_version()
    }
    /// Statically linked Native client library version.
    pub fn server_version() -> crate::sys::ffi::Version {
        super::native_session::NativeSession::server_version()
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
