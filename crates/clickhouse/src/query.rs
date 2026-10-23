//! Borrowed query metadata and synchronous server event handlers.

use std::{borrow::Cow, cell::RefCell, collections::BTreeMap, marker::PhantomData, rc::Rc};

#[cfg(feature = "tcp")]
use crate::NativeBlock;

/// Compression shared by Native blocks and HTTP request bodies.
pub type Compression = CompressionMethod;

/// Which HTTP version the HTTP transport asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpVersionChoice {
    /// Cleartext HTTP/2 prior knowledge, with multiplexing.
    Http2PriorKnowledge,
    /// HTTP/2 when the scheme is HTTPS.
    Http2Tls,
    /// HTTP/1.1 for peers that do not speak HTTP/2.
    Http1,
    /// Prefer HTTP/2 and allow an HTTP/1.1 response.
    Negotiate,
}

/// Reject a query when the server does not recognize the setting.
pub const IMPORTANT: u64 = 0x01;
/// Identify a custom setting supplied by the application.
pub const CUSTOM: u64 = 0x02;
/// Mark a setting as obsolete in the server setting protocol.
pub const OBSOLETE: u64 = 0x04;

/// A per-query setting with the upstream Native protocol flags.
#[derive(Debug, PartialEq, Eq)]
pub struct QuerySettingsField<'a> {
    /// Serialized setting value; literals remain borrowed.
    pub value: Cow<'a, str>,
    /// Bitwise combination of [`IMPORTANT`], [`CUSTOM`], and [`OBSOLETE`].
    pub flags: u64,
}

/// Settings are serialized in a deterministic key order.
pub type QuerySettings<'a> = BTreeMap<Cow<'a, str>, QuerySettingsField<'a>>;
/// `None` is a NULL parameter, distinct from an empty string.
pub type QueryParams<'a> = BTreeMap<Cow<'a, str>, Option<Cow<'a, str>>>;
/// Caller-supplied identity sent without generating or replacing a value.
pub type QueryId<'a> = Cow<'a, str>;

/// OpenTelemetry context attached to a Native query.
#[derive(Debug, PartialEq, Eq)]
pub struct TracingContext<'a> {
    /// Complete 128-bit trace identifier, preserving leading zero bits.
    pub trace_id: u128,
    /// Parent span identifier.
    pub span_id: u64,
    /// W3C tracestate text.
    pub tracestate: Cow<'a, str>,
    /// W3C trace flags, including the sampled bit.
    pub trace_flags: u8,
}

/// Server progress deltas; these values are not client-side estimates.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Rows read in this progress event.
    pub rows: u64,
    /// Bytes read in this progress event.
    pub bytes: u64,
    /// Server estimate of total input rows.
    pub total_rows: u64,
    /// Rows written in this event.
    pub written_rows: u64,
    /// Bytes written in this event.
    pub written_bytes: u64,
}

/// Query profile reported by the server.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    /// Result rows.
    pub rows: u64,
    /// Result blocks.
    pub blocks: u64,
    /// Result bytes.
    pub bytes: u64,
    /// Rows before the server applied a limit.
    pub rows_before_limit: u64,
    /// Whether the server applied a limit.
    pub applied_limit: bool,
    /// Whether rows before limit were calculated.
    pub calculated_rows_before_limit: bool,
}

/// An exception packet retaining the server's diagnostic fields.
#[derive(Debug, PartialEq, Eq)]
pub struct ServerException {
    /// ClickHouse error code.
    pub code: i32,
    /// Server exception type.
    pub name: String,
    /// Human-readable diagnostic supplied by the server.
    pub display_text: String,
    /// Stack trace supplied by the server, possibly empty.
    pub stack_trace: String,
}

/// Native block compression configured when a client is created.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    /// Send uncompressed Native blocks.
    #[default]
    None,
    /// Compress blocks with LZ4.
    Lz4,
    /// Compress blocks with Zstandard.
    Zstd,
}

/// Information obtained during the server handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    /// Server product name.
    pub name: String,
    /// Server timezone identifier.
    pub timezone: String,
    /// Server display name.
    pub display_name: String,
    /// Server major version.
    pub version_major: u64,
    /// Server minor version.
    pub version_minor: u64,
    /// Server patch version.
    pub version_patch: u64,
    /// Negotiated server protocol revision.
    pub revision: u64,
}

/// An in-memory named table borrowed for one query execution.
#[cfg(feature = "tcp")]
pub struct ExternalTable<'a> {
    /// Name referenced by SQL; it is not interpolated into the SQL text.
    pub name: Cow<'a, str>,
    /// Data whose columns define the external table schema.
    pub data: &'a NativeBlock,
}

type ValueHandler<'a, T> = Rc<dyn Fn(T) + 'a>;
#[cfg(feature = "tcp")]
type BlockHandler<'a> = Rc<dyn Fn(&NativeBlock) + 'a>;
type ExceptionHandler<'a> = Rc<dyn Fn(&ServerException) + 'a>;

/// A synchronous request retaining borrowed text and scoped callbacks.
///
/// Callbacks run on the thread advancing the query. Callback panics are contained
/// and returned as `Error::CallbackPanicked`; no panic unwinds across C++.
pub struct Query<'a> {
    /// SQL sent as supplied, without statement interpolation.
    pub sql: Cow<'a, str>,
    /// Caller-supplied query identity.
    pub query_id: QueryId<'a>,
    /// Per-query settings.
    pub settings: QuerySettings<'a>,
    /// Typed placeholders resolved by the server.
    pub params: QueryParams<'a>,
    /// Optional distributed tracing context.
    pub tracing_context: Option<TracingContext<'a>>,
    pub(crate) on_progress: Option<ValueHandler<'a, Progress>>,
    pub(crate) on_profile: Option<ValueHandler<'a, Profile>>,
    #[cfg(feature = "tcp")]
    pub(crate) on_profile_events: Option<BlockHandler<'a>>,
    #[cfg(feature = "tcp")]
    pub(crate) on_server_log: Option<BlockHandler<'a>>,
    pub(crate) on_exception: Option<ExceptionHandler<'a>>,
    pub(crate) deduplication_token: Option<Cow<'a, str>>,
    pub(crate) compression: Option<Compression>,
    /// Callbacks run on the thread that advances the query; the request is not Send.
    _not_send: PhantomData<*const ()>,
}

impl<'a> Query<'a> {
    /// Narrow the borrowing interval to one synchronous operation.
    #[cfg(feature = "tcp")]
    pub(crate) fn shorten<'b>(self) -> Query<'b>
    where
        'a: 'b,
    {
        self
    }

    /// Borrow metadata and share thread-local handlers for one synchronous execution.
    #[cfg(feature = "tcp")]
    pub(crate) fn reborrow(&self) -> Query<'_> {
        Query {
            sql: Cow::Borrowed(&self.sql),
            query_id: Cow::Borrowed(&self.query_id),
            settings: self
                .settings
                .iter()
                .map(|(key, value)| {
                    (
                        Cow::Borrowed(key.as_ref()),
                        QuerySettingsField {
                            value: Cow::Borrowed(value.value.as_ref()),
                            flags: value.flags,
                        },
                    )
                })
                .collect(),
            params: self
                .params
                .iter()
                .map(|(key, value)| {
                    (
                        Cow::Borrowed(key.as_ref()),
                        value.as_deref().map(Cow::Borrowed),
                    )
                })
                .collect(),
            tracing_context: self.tracing_context.as_ref().map(|value| TracingContext {
                trace_id: value.trace_id,
                span_id: value.span_id,
                tracestate: Cow::Borrowed(value.tracestate.as_ref()),
                trace_flags: value.trace_flags,
            }),
            on_progress: self.on_progress.as_ref().map(Rc::clone),
            on_profile: self.on_profile.as_ref().map(Rc::clone),
            #[cfg(feature = "tcp")]
            on_profile_events: self.on_profile_events.as_ref().map(Rc::clone),
            #[cfg(feature = "tcp")]
            on_server_log: self.on_server_log.as_ref().map(Rc::clone),
            on_exception: self.on_exception.as_ref().map(Rc::clone),
            deduplication_token: self.deduplication_token.as_deref().map(Cow::Borrowed),
            compression: self.compression,
            _not_send: PhantomData,
        }
    }

    /// Create a query; string literals and borrowed SQL do not allocate.
    pub fn new(sql: impl Into<Cow<'a, str>>) -> Self {
        Self {
            sql: sql.into(),
            query_id: Cow::Borrowed(""),
            settings: BTreeMap::new(),
            params: BTreeMap::new(),
            tracing_context: None,
            on_progress: None,
            on_profile: None,
            #[cfg(feature = "tcp")]
            on_profile_events: None,
            #[cfg(feature = "tcp")]
            on_server_log: None,
            on_exception: None,
            deduplication_token: None,
            compression: None,
            _not_send: PhantomData,
        }
    }

    /// Attach a caller-supplied query identity.
    pub fn with_id(mut self, id: impl Into<Cow<'a, str>>) -> Self {
        self.query_id = id.into();
        self
    }

    /// Insert or replace one flagged setting.
    pub fn with_setting(
        mut self,
        name: impl Into<Cow<'a, str>>,
        value: impl Into<Cow<'a, str>>,
        flags: u64,
    ) -> Self {
        self.settings.insert(
            name.into(),
            QuerySettingsField {
                value: value.into(),
                flags,
            },
        );
        self
    }

    /// Bind a value to a server-side typed placeholder.
    pub fn with_param(
        mut self,
        name: impl Into<Cow<'a, str>>,
        value: impl Into<Cow<'a, str>>,
    ) -> Self {
        self.params.insert(name.into(), Some(value.into()));
        self
    }

    /// Bind a NULL value without conflating it with an empty string.
    pub fn with_null_param(mut self, name: impl Into<Cow<'a, str>>) -> Self {
        self.params.insert(name.into(), None);
        self
    }

    /// Attach the complete tracing context.
    pub fn with_tracing(mut self, context: TracingContext<'a>) -> Self {
        self.tracing_context = Some(context);
        self
    }

    /// Receive server progress while the caller advances the query.
    pub fn on_progress(mut self, callback: impl FnMut(Progress) + 'a) -> Self {
        let callback = RefCell::new(callback);
        self.on_progress = Some(Rc::new(move |value| (callback.borrow_mut())(value)));
        self
    }

    /// Receive the server's result profile.
    pub fn on_profile(mut self, callback: impl FnMut(Profile) + 'a) -> Self {
        let callback = RefCell::new(callback);
        self.on_profile = Some(Rc::new(move |value| (callback.borrow_mut())(value)));
        self
    }

    /// Receive profile event tables without projecting or copying their columns.
    #[cfg(feature = "tcp")]
    pub fn on_profile_events(mut self, callback: impl FnMut(&NativeBlock) + 'a) -> Self {
        let callback = RefCell::new(callback);
        self.on_profile_events = Some(Rc::new(move |value| (callback.borrow_mut())(value)));
        self
    }

    /// Receive structured server log tables.
    #[cfg(feature = "tcp")]
    pub fn on_server_log(mut self, callback: impl FnMut(&NativeBlock) + 'a) -> Self {
        let callback = RefCell::new(callback);
        self.on_server_log = Some(Rc::new(move |value| (callback.borrow_mut())(value)));
        self
    }

    /// Observe a server exception before it is returned as an error.
    pub fn on_exception(mut self, callback: impl FnMut(&ServerException) + 'a) -> Self {
        let callback = RefCell::new(callback);
        self.on_exception = Some(Rc::new(move |value| (callback.borrow_mut())(value)));
        self
    }

    /// Whether this query requires server event handlers.
    pub fn has_event_callbacks(&self) -> bool {
        self.on_progress.is_some()
            || self.on_profile.is_some()
            || self.has_block_callbacks()
            || self.on_exception.is_some()
    }
    /// Borrow SQL without changing its ownership.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Insert or replace an unflagged setting for either transport.
    pub fn setting(self, key: impl Into<Cow<'a, str>>, value: impl Into<Cow<'a, str>>) -> Self {
        self.with_setting(key, value, 0)
    }

    /// Select the HTTP insert deduplication token for one request.
    pub fn deduplication_token(mut self, token: impl Into<Cow<'a, str>>) -> Self {
        self.deduplication_token = Some(token.into());
        self
    }

    /// Override HTTP body compression for one request.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    pub(crate) fn settings(&self) -> &QuerySettings<'a> {
        &self.settings
    }
    pub(crate) fn deduplication_token_value(&self) -> Option<&str> {
        self.deduplication_token.as_deref()
    }
    pub(crate) fn compression_override(&self) -> Option<Compression> {
        self.compression
    }

    fn has_block_callbacks(&self) -> bool {
        #[cfg(feature = "tcp")]
        {
            self.on_profile_events.is_some() || self.on_server_log.is_some()
        }
        #[cfg(not(feature = "tcp"))]
        {
            false
        }
    }
}

impl<'a> From<&'a str> for Query<'a> {
    fn from(sql: &'a str) -> Self {
        Self::new(sql)
    }
}

impl From<String> for Query<'_> {
    fn from(sql: String) -> Self {
        Self::new(sql)
    }
}

impl std::fmt::Display for ServerException {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} ({}): {}",
            self.name, self.code, self.display_text
        )
    }
}

impl std::error::Error for ServerException {}
