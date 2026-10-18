//! Synchronous Native and HTTP transport entry points.
#[cfg(feature = "tcp")]
pub(crate) mod native_session;
#[cfg(feature = "tcp")]
use native_session::NativeSession;

#[cfg(feature = "tcp")]
mod builder;
#[cfg(feature = "tcp")]
mod native_api;
#[cfg(feature = "tcp")]
pub use builder::ClientBuilder;

#[cfg(feature = "tcp")]
use crate::native_block::NativeBlock;
#[cfg(feature = "http")]
mod http_api;

use crate::{
    Block, Result,
    endpoint::{Endpoint, Scheme},
    error::Error,
    query::Query,
    serde_bridge::SerdeBridge,
    telemetry::control_log,
};
#[cfg(feature = "http")]
use crate::{
    query::{Compression, HttpVersionChoice},
    transport::{HttpPreferences, HttpTransport},
};

enum Backend {
    #[cfg(feature = "tcp")]
    Native(NativeSession),
    #[cfg(feature = "http")]
    Http(HttpTransport),
}
/// A synchronous client whose transport follows the parsed endpoint scheme.
///
/// Create, use, and drop the client on its owning thread. HTTP connection state
/// and its event loop remain on that thread throughout the client lifetime.
/// The client cannot cross thread boundaries or be shared between threads.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<clickhouse::Client>();
/// ```
///
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<clickhouse::Client>();
/// ```
pub struct Client {
    backend: Backend,
    #[cfg(feature = "http")]
    endpoint: Endpoint,
    #[cfg(feature = "http")]
    database: String,
    #[cfg(feature = "http")]
    compression: Compression,
    #[cfg(feature = "http")]
    dedup: Option<String>,
    #[cfg(feature = "http")]
    http_health: [crate::EndpointHealth; 1],
    #[cfg(feature = "http")]
    http_observer: Option<crate::config::FailoverCallback>,
}
impl Client {
    /// Connect using the endpoint scheme and explicit credentials.
    /// # Errors
    /// Invalid transport options, native handshake, or HTTP setup failures propagate.
    pub fn connect(endpoint: Endpoint, user: &str, password: &str, database: &str) -> Result<Self> {
        control_log!(
            "connect endpoint={}:{} database={database}",
            endpoint.host(),
            endpoint.port()
        );
        let backend = match endpoint.scheme() {
            Scheme::Tcp | Scheme::Tcps => {
                #[cfg(feature = "tcp")]
                {
                    #[cfg(feature = "http")]
                    let native_endpoint = endpoint.clone();
                    #[cfg(not(feature = "http"))]
                    let native_endpoint = endpoint;
                    Backend::Native(NativeSession::connect(
                        native_endpoint,
                        user,
                        password,
                        database,
                    )?)
                }
                #[cfg(not(feature = "tcp"))]
                {
                    return Err(Error::unsupported(
                        "Native TCP transport is disabled; enable the tcp feature",
                    ));
                }
            }
            Scheme::Http | Scheme::Https => {
                #[cfg(feature = "http")]
                {
                    Backend::Http(HttpTransport::open(
                        endpoint.scheme(),
                        endpoint.host(),
                        endpoint.port(),
                        user,
                        password,
                        if endpoint.scheme() == Scheme::Https {
                            HttpVersionChoice::Http2Tls
                        } else {
                            HttpVersionChoice::Negotiate
                        },
                    )?)
                }
                #[cfg(not(feature = "http"))]
                {
                    return Err(Error::unsupported(
                        "HTTP transport is disabled; enable the http feature",
                    ));
                }
            }
        };
        Ok(Self {
            backend,
            #[cfg(feature = "http")]
            endpoint,
            #[cfg(feature = "http")]
            database: database.to_owned(),
            #[cfg(feature = "http")]
            compression: Compression::None,
            #[cfg(feature = "http")]
            dedup: None,
            #[cfg(feature = "http")]
            http_health: [crate::EndpointHealth::new(0)],
            #[cfg(feature = "http")]
            http_observer: None,
        })
    }
    /// Connect to a managed Native TCP or TCPS endpoint list.
    /// # Errors
    /// HTTP lists and Native connection or TLS configuration failures are rejected.
    pub fn new(config: crate::ClientConfig<'_>) -> Result<Self> {
        Self::with_config(config)
    }
    /// Connect with Native TLS, endpoint selection, and observer configuration.
    /// # Errors
    /// HTTP endpoint lists are unsupported here; use `connect` for HTTP.
    pub fn with_config(config: crate::ClientConfig<'_>) -> Result<Self> {
        if config
            .endpoints
            .iter()
            .any(|e| !matches!(e.scheme(), Scheme::Tcp | Scheme::Tcps))
        {
            return Err(Error::unsupported(
                "managed failover configuration requires Native TCP or TCPS endpoints",
            ));
        }
        #[cfg(not(feature = "tcp"))]
        {
            drop(config);
            Err(Error::unsupported(
                "managed Native configuration requires the tcp feature",
            ))
        }
        #[cfg(feature = "tcp")]
        {
            #[cfg(feature = "http")]
            let endpoint = config
                .endpoints
                .first()
                .cloned()
                .ok_or_else(|| Error::Config {
                    reason: "endpoint list is empty".into(),
                })?;
            #[cfg(feature = "http")]
            let database = config.database.as_ref().to_owned();
            let native = NativeSession::new(config)?;
            Ok(Self {
                backend: Backend::Native(native),
                #[cfg(feature = "http")]
                endpoint,
                #[cfg(feature = "http")]
                database,
                #[cfg(feature = "http")]
                compression: Compression::None,
                #[cfg(feature = "http")]
                dedup: None,
                #[cfg(feature = "http")]
                http_health: [crate::EndpointHealth::new(0)],
                #[cfg(feature = "http")]
                http_observer: None,
            })
        }
    }
    /// Observe Native endpoint transitions; a single HTTP endpoint never transitions.
    pub fn on_failover(&mut self, callback: impl FnMut(&crate::FailoverEvent<'_>) + 'static) {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.on_failover(callback),
            #[cfg(feature = "http")]
            Backend::Http(_) => self.http_observer = Some(Box::new(callback)),
        }
    }
    /// Current Native failover endpoint or the fixed HTTP endpoint.
    pub fn current_endpoint(&self) -> &Endpoint {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.current_endpoint(),
            #[cfg(feature = "http")]
            Backend::Http(_) => &self.endpoint,
        }
    }
    /// Native policy, or sticky single-endpoint HTTP behavior without automatic retries.
    pub fn policy(&self) -> crate::FailoverPolicy {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.policy(),
            #[cfg(feature = "http")]
            Backend::Http(_) => Self::http_policy(),
        }
    }
    #[cfg(feature = "http")]
    fn http_policy() -> crate::FailoverPolicy {
        crate::FailoverPolicy::new()
            .with_selection(crate::EndpointSelection::Sticky)
            .with_send_retries(0)
            .with_circuit(u32::MAX, std::time::Duration::ZERO)
    }
    /// Reconnect a managed Native endpoint without replaying an operation.
    /// # Errors
    /// Native dial failure propagates; HTTP manual endpoint reset is unsupported.
    pub fn reset_connection_endpoint(&mut self) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.reset_connection_endpoint(),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "manual endpoint reset requires a managed Native connection",
            )),
        }
    }
    /// Last Native diagnostics or HTTP results from explicit health probes.
    /// HTTP health is unobserved until `probe_health` runs; no positive state is fabricated.
    pub fn endpoint_health(&self) -> &[crate::EndpointHealth] {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.endpoint_health(),
            #[cfg(feature = "http")]
            Backend::Http(_) => &self.http_health,
        }
    }
    /// Probe Native endpoints or execute a real query on the single HTTP endpoint.
    /// HTTP failures are recorded without adding automatic replay or endpoint rotation.
    pub fn probe_health(&mut self) -> Vec<crate::EndpointHealth> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.probe_health(),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let started = std::time::Instant::now();
                match self.ping() {
                    Ok(()) => self.http_health[0].success(started.elapsed()),
                    Err(error) => {
                        self.http_health[0].failure(Self::http_policy(), format!("{error}"))
                    }
                }
                self.http_health.to_vec()
            }
        }
    }
    /// Read the first available UInt64 cell through the selected transport.
    /// # Errors
    /// Query failures, wrong types, or missing cells return an error.
    pub fn query_u64(&mut self, sql: &str) -> Result<u64> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.query_u64(sql),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let blocks = self.query_owned_blocks(sql)?;
                for block in &blocks {
                    if block.row_count() > 0 {
                        let column = block.column(0)?;
                        if column.type_name.as_ref() != "UInt64"
                            || column.len() != block.row_count()
                        {
                            return Err(Error::block(
                                "query_u64 requires a complete UInt64 first column",
                            ));
                        }
                        return block
                            .column_slice::<u64>(0)?
                            .first()
                            .copied()
                            .ok_or_else(|| Error::native("query returned no UInt64 cell"));
                    }
                }
                Err(Error::native("query returned no UInt64 cell"))
            }
        }
    }
    /// Retrieve a contained Native failover observer panic before reuse.
    /// HTTP has no endpoint transitions and therefore no observer callback errors.
    pub fn take_callback_error(&mut self) -> Option<Error> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.take_callback_error(),
            #[cfg(feature = "http")]
            Backend::Http(_) => None,
        }
    }
    /// Parsed endpoint associated with this transport.
    pub fn endpoint(&self) -> &Endpoint {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.endpoint(),
            #[cfg(feature = "http")]
            Backend::Http(_) => &self.endpoint,
        }
    }
    /// HTTP connection options, absent for Native TCP.
    #[cfg(feature = "http")]
    pub fn http_preferences(&self) -> Option<HttpPreferences> {
        match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => Some(h.preferences()),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => None,
        }
    }
    /// Last observed HTTP protocol, absent before the first request or on TCP.
    #[cfg(feature = "http")]
    pub fn negotiated_http_version(&self) -> Option<HttpVersionChoice> {
        match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => h.negotiated_version(),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => None,
        }
    }
    /// Set the requested HTTP protocol for subsequent requests.
    /// # Errors
    /// Native endpoints and unsupported curl options reject the change.
    #[cfg(feature = "http")]
    pub fn set_http_version(&mut self, version: HttpVersionChoice) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => h.set_version(version),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported("HTTP protocol selection requires HTTP")),
        }
    }
    /// Configure compression for subsequent HTTP requests.
    /// # Errors
    /// Native endpoints reject HTTP compression settings explicitly.
    #[cfg(feature = "http")]
    pub fn set_compression(&mut self, compression: Compression) -> Result<()> {
        #[cfg(feature = "tcp")]
        if matches!(self.backend, Backend::Native(_)) {
            return Err(Error::unsupported(
                "HTTP compression requires an HTTP endpoint",
            ));
        }
        self.compression = compression;
        Ok(())
    }
    /// Configure the default HTTP server insert deduplication token.
    /// # Errors
    /// Native endpoints reject settings unavailable through the native bridge.
    #[cfg(feature = "http")]
    pub fn set_insert_deduplication_token(&mut self, token: impl Into<String>) -> Result<()> {
        #[cfg(feature = "tcp")]
        if matches!(self.backend, Backend::Native(_)) {
            return Err(Error::unsupported(
                "Native insert deduplication settings are not supported",
            ));
        }
        self.dedup = Some(token.into());
        Ok(())
    }
    /// Check server responsiveness using the connected transport.
    /// # Errors
    /// Connection, protocol, or server failures propagate.
    pub fn ping(&mut self) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.ping(),
            #[cfg(feature = "http")]
            Backend::Http(_) => self.execute("SELECT 1"),
        }
    }
    /// Execute SQL through the selected transport.
    /// # Errors
    /// Server rejection or transport errors propagate.
    pub fn execute<'a>(&mut self, query: impl Into<Query<'a>>) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.execute(query),
            #[cfg(feature = "http")]
            Backend::Http(_) => self.execute_query(&query.into()),
        }
    }
    /// Execute SQL with HTTP settings or Native defaults.
    /// # Errors
    /// Unsupported Native per-query settings and transport failures are explicit.
    pub fn execute_query(&mut self, query: &Query<'_>) -> Result<()> {
        control_log!("execute statement_bytes={}", query.sql().len());
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(inner) => {
                if !query.settings().is_empty()
                    || query.has_event_callbacks()
                    || !query.query_id.is_empty()
                    || !query.params.is_empty()
                    || query.tracing_context.is_some()
                    || query.compression_override().is_some()
                    || query.deduplication_token_value().is_some()
                {
                    return Err(Error::unsupported(
                        "Native query settings are not supported",
                    ));
                }
                inner.execute(query.sql())
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                self.http_call(query, query.sql(), &[])?;
                Ok(())
            }
        }
    }
    /// Deliver raw HTTP response chunks incrementally on the owning thread.
    /// The callback borrows each chunk only for the duration of that call. No
    /// complete response buffer is assembled by this interface. A callback
    /// error aborts the transfer; previously delivered chunks remain observable.
    /// # Errors
    /// Native endpoints, compressed streaming responses, callback errors,
    /// transport failures, and unsuccessful HTTP responses reject the operation.
    /// # Panics
    /// A callback panic resumes unwinding after the borrowed receiver has been
    /// revoked, so the transport cannot subsequently invoke the expired borrow.
    #[cfg(feature = "http")]
    pub fn query_raw_with<F>(&mut self, query: &Query<'_>, receive: F) -> Result<usize>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        match &mut self.backend {
            #[cfg(feature = "http")]
            Backend::Http(http) => http.execute_with(
                &self.database,
                query,
                &[],
                self.compression,
                self.dedup.as_deref(),
                receive,
            ),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "raw HTTP response streaming requires an HTTP endpoint",
            )),
        }
    }
    /// Clear and refill a caller-owned byte buffer through incremental delivery.
    /// Existing capacity is retained; enough preallocated capacity avoids response
    /// buffer growth. On failure the buffer may retain a partial response.
    /// # Errors
    /// Native endpoints, compression, transport, and server failures propagate.
    #[cfg(feature = "http")]
    pub fn query_raw_into(&mut self, query: &Query<'_>, dst: &mut Vec<u8>) -> Result<usize> {
        dst.clear();
        self.query_raw_with(query, |chunk| {
            dst.extend_from_slice(chunk);
            Ok(())
        })
    }
    /// Write raw HTTP chunks into a bounded caller-owned slice without truncation.
    /// Unused trailing bytes are preserved. On failure a written prefix may remain.
    /// # Errors
    /// Insufficient capacity returns a configuration error. Native endpoints,
    /// compression, transport, and server failures propagate.
    #[cfg(feature = "http")]
    pub fn query_raw_into_slice(&mut self, query: &Query<'_>, dst: &mut [u8]) -> Result<usize> {
        let mut written = 0usize;
        self.query_raw_with(query, |chunk| {
            let end = written
                .checked_add(chunk.len())
                .filter(|end| *end <= dst.len())
                .ok_or_else(|| Error::Config {
                    reason: "raw response exceeds destination slice capacity".into(),
                })?;
            dst[written..end].copy_from_slice(chunk);
            written = end;
            Ok(())
        })
    }
    /// Query raw HTTP bytes with an explicitly selected server format.
    /// # Errors
    /// Native endpoints or HTTP/server errors reject the call.
    #[cfg(feature = "http")]
    pub fn fetch_bytes(&mut self, query: &Query<'_>) -> Result<Vec<u8>> {
        self.http_call(query, query.sql(), &[])
    }
    /// Insert an encoded HTTP body using the format stated in SQL.
    /// # Errors
    /// Native endpoints, malformed payloads, and server errors reject insertion.
    #[cfg(feature = "http")]
    pub fn insert_formatted(&mut self, query: &Query<'_>, bytes: &[u8]) -> Result<()> {
        self.http_call(query, query.sql(), bytes)?;
        Ok(())
    }
    /// Insert a Native transport block through either endpoint scheme.
    /// # Errors
    /// Invalid blocks, conversion failures, or server rejection propagate.
    #[cfg(feature = "tcp")]
    pub fn insert_block(&mut self, table: &str, block: &mut NativeBlock) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.insert_block(table, block),
            #[cfg(feature = "http")]
            Backend::Http(_) => self.insert_owned_block(table, &Block::from_native(block)?),
        }
    }
    /// Query Native transport blocks through either endpoint scheme.
    /// # Errors
    /// Transport, protocol, and unsupported conversion failures propagate.
    #[cfg(feature = "tcp")]
    pub fn query_blocks<'a>(&mut self, query: impl Into<Query<'a>>) -> Result<Vec<NativeBlock>> {
        let query = query.into();
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.query_blocks(query),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(query.sql(), "Native");
                let bytes = self.http_call(&query, &sql, &[])?;
                crate::native_format::decode_blocks(&bytes, 0)?
                    .iter()
                    .map(Block::to_native)
                    .collect()
            }
        }
    }
    /// Insert serde rows without changing the derived RowClient API.
    /// # Errors
    /// Unsupported serde shapes, schema mismatch, or server errors reject insertion.
    pub fn serde_insert_rows<T: serde::Serialize>(
        &mut self,
        table: &str,
        rows: &[T],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                self.insert_owned_block(table, &SerdeBridge::rows_to_block(rows)?)
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let (names, body) = SerdeBridge::to_rowbinary(rows)?;
                let names = names
                    .iter()
                    .map(|n| quote_ident(n))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let sql = format!(
                    "INSERT INTO {} ({names}) FORMAT RowBinary",
                    quote_table(table)?
                );
                self.http_call(&Query::new(""), &sql, &body)?;
                Ok(())
            }
        }
    }
    /// Materialize serde rows from HTTP RowBinary or shared Native columns.
    /// # Errors
    /// Invalid response shapes, unsupported settings, or transport failures propagate.
    pub fn serde_query_rows<T: serde::de::DeserializeOwned>(
        &mut self,
        query: &Query<'_>,
    ) -> Result<Vec<T>> {
        match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(query.sql(), "RowBinaryWithNamesAndTypes");
                let bytes = self.http_call(query, &sql, &[])?;
                SerdeBridge::block_to_rows(&crate::native_format::decode_rowbinary(&bytes)?)
            }
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                if !query.settings().is_empty()
                    || query.has_event_callbacks()
                    || !query.query_id.is_empty()
                    || !query.params.is_empty()
                    || query.tracing_context.is_some()
                    || query.compression_override().is_some()
                    || query.deduplication_token_value().is_some()
                {
                    return Err(Error::unsupported(
                        "Native query settings are not supported",
                    ));
                }
                let mut rows = Vec::new();
                for b in self.query_owned_blocks(query.sql())? {
                    rows.extend(SerdeBridge::block_to_rows(&b)?);
                }
                Ok(rows)
            }
        }
    }
    /// Buffer a complete HTTP query response before exposing an iterator.
    /// # Errors
    /// Unsupported Native settings, conversion, or server failures reject construction.
    #[cfg(feature = "http")]
    pub fn select_http_stream<'a>(
        &'a mut self,
        query: &Query<'_>,
    ) -> Result<crate::stream::HttpBlockStream<'a>> {
        let blocks = match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(query.sql(), "Native");
                let bytes = self.http_call(query, &sql, &[])?;
                crate::native_format::decode_blocks(&bytes, 0)?
            }
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                return Err(Error::unsupported(
                    "buffered HTTP iteration requires an HTTP endpoint",
                ));
            }
        };
        Ok(crate::stream::HttpBlockStream {
            client: self,
            blocks: blocks.into_iter(),
        })
    }
    /// Create a buffered inserter that sends nothing until explicitly finalized.
    /// # Errors
    /// Invalid identifiers reject this staging interface.
    #[cfg(feature = "http")]
    pub fn http_inserter<'a>(
        &'a mut self,
        table: &str,
    ) -> Result<crate::inserter::HttpInserter<'a>> {
        #[cfg(feature = "tcp")]
        if matches!(self.backend, Backend::Native(_)) {
            return Err(Error::unsupported(
                "HTTP insert staging requires an HTTP endpoint",
            ));
        }
        quote_table(table)?;
        Ok(crate::inserter::HttpInserter::new(self, table))
    }
    #[cfg(feature = "http")]
    pub(crate) fn insert_native_payload(&mut self, table: &str, bytes: &[u8]) -> Result<()> {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "HTTP insert staging requires an HTTP endpoint",
            )),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = format!("INSERT INTO {} FORMAT Native", quote_table(table)?);
                self.http_call(&Query::new(""), &sql, bytes)?;
                Ok(())
            }
        }
    }
    pub(crate) fn insert_owned_block(&mut self, table: &str, block: &Block) -> Result<()> {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                let mut native = block.to_native()?;
                self.insert_block(table, &mut native)
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                if block.row_count() == 0 {
                    return Ok(());
                }
                let mut names = Vec::new();
                for i in 0..block.column_count() {
                    names.push(quote_ident(&block.column(i)?.name)?);
                }
                let sql = format!(
                    "INSERT INTO {} ({}) FORMAT Native",
                    quote_table(table)?,
                    names.join(", ")
                );
                let mut body = Vec::new();
                crate::native_format::encode_block(block, 0, &mut body)?;
                self.http_call(&Query::new(""), &sql, &body)?;
                Ok(())
            }
        }
    }
    pub(crate) fn query_owned_blocks(&mut self, sql: &str) -> Result<Vec<Block>> {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(_) => self
                .query_blocks(sql)?
                .iter()
                .filter(|b| b.rows() > 0)
                .map(Block::from_native)
                .collect(),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(sql, "Native");
                let bytes = self.http_call(&Query::new(""), &sql, &[])?;
                crate::native_format::decode_blocks(&bytes, 0)
            }
        }
    }
    #[cfg(feature = "http")]
    fn http_call(&mut self, query: &Query<'_>, sql: &str, body: &[u8]) -> Result<Vec<u8>> {
        match &mut self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => h.execute(
                &self.database,
                query,
                sql,
                body,
                self.compression,
                self.dedup.as_deref(),
            ),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "formatted HTTP bytes require an HTTP endpoint",
            )),
        }
    }
}
#[cfg(feature = "http")]
fn ensure_format(sql: &str, format: &str) -> String {
    if sql.to_ascii_lowercase().contains(" format ") {
        sql.to_owned()
    } else {
        format!("{sql} FORMAT {format}")
    }
}
#[cfg(feature = "http")]
fn quote_ident(name: &str) -> Result<String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::protocol("invalid SQL identifier"));
    }
    Ok(format!("`{name}`"))
}
#[cfg(feature = "http")]
fn quote_table(table: &str) -> Result<String> {
    Ok(table
        .split('.')
        .map(quote_ident)
        .collect::<Result<Vec<_>>>()?
        .join("."))
}
