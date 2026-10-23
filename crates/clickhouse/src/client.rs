//! Caller-owned connections, validated configuration, and transport lifecycle.
#[cfg(feature = "tcp")]
pub(crate) mod native_session;
#[cfg(feature = "tcp")]
use native_session::NativeSession;

mod data;
#[cfg(feature = "http")]
mod http;

use crate::{
    ClientConfig, Result,
    endpoint::{Endpoint, Scheme},
    error::Error,
    query::Query,
    telemetry::control_log,
    threading::{ThreadLocalClient, WorkerBalance, WorkerClient},
};
#[cfg(feature = "http")]
use crate::{
    query::{Compression, HttpVersionChoice},
    transport::HttpTransport,
};

enum Backend {
    #[cfg(feature = "tcp")]
    Native(Box<NativeSession>),
    #[cfg(feature = "http")]
    Http(Box<HttpTransport>),
}
/// A thread-affine client with convenience calls and explicit HTTP progress.
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
                    Backend::Native(Box::new(NativeSession::connect(
                        native_endpoint,
                        user,
                        password,
                        database,
                    )?))
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
                    Backend::Http(Box::new(HttpTransport::open(
                        endpoint.scheme(),
                        endpoint.host(),
                        endpoint.port(),
                        user,
                        password,
                        HttpVersionChoice::Negotiate,
                    )?))
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
            Backend::Native(inner) => inner.execute(query.reborrow()),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                self.http_call(query, query.sql(), &[])?;
                Ok(())
            }
        }
    }
}

impl Client {
    /// Connect with validated options for the selected transport.
    /// # Errors
    /// Disabled schemes, invalid deadlines, and connection failures propagate.
    pub fn new(config: ClientConfig<'_>) -> Result<Self> {
        Self::with_config(config)
    }

    /// Connect without silently discarding a caller-selected transport or deadline.
    /// # Errors
    /// Mixed transports and multi-endpoint HTTP failover are unsupported. HTTP
    /// workers select their individual endpoint before constructing each client.
    /// Custom Native TLS/socket settings and failover policies are rejected for
    /// HTTP; use `with_timeout` for its request deadline.
    pub fn with_config(config: ClientConfig<'_>) -> Result<Self> {
        config.validate()?;
        let endpoint = config.endpoints.first().ok_or_else(|| Error::Config {
            reason: "endpoint list is empty".into(),
        })?;
        if matches!(endpoint.scheme(), Scheme::Http | Scheme::Https) {
            #[cfg(feature = "http")]
            {
                validate_http_options(&config)?;
                if config.endpoints.len() != 1 {
                    return Err(Error::unsupported(
                        "HTTP sessions require one endpoint; use worker endpoint striping",
                    ));
                }
                let mut client = Self::connect(
                    endpoint.clone(),
                    &config.user,
                    &config.password,
                    &config.database,
                )?;
                if let Some(timeout) = config.timeout {
                    match &mut client.backend {
                        Backend::Http(http) => {
                            http.set_timeout(timeout);
                            http.reset()?;
                        }
                        #[cfg(feature = "tcp")]
                        Backend::Native(_) => {
                            return Err(Error::unsupported(
                                "HTTP deadline requires an HTTP endpoint",
                            ));
                        }
                    }
                }
                client.compression = config.compression;
                client.http_observer = config.callback;
                return Ok(client);
            }
            #[cfg(not(feature = "http"))]
            return Err(Error::unsupported(
                "HTTP transport is disabled; enable the http feature",
            ));
        }
        #[cfg(feature = "tcp")]
        {
            #[cfg(feature = "http")]
            let endpoint = endpoint.clone();
            #[cfg(feature = "http")]
            let database = config.database.as_ref().to_owned();
            let native = NativeSession::new(config)?;
            Ok(Self {
                backend: Backend::Native(Box::new(native)),
                #[cfg(feature = "http")]
                endpoint,
                #[cfg(feature = "http")]
                database,
                #[cfg(feature = "http")]
                compression: crate::CompressionMethod::None,
                #[cfg(feature = "http")]
                dedup: None,
                #[cfg(feature = "http")]
                http_health: [crate::EndpointHealth::new(0)],
                #[cfg(feature = "http")]
                http_observer: None,
            })
        }
        #[cfg(not(feature = "tcp"))]
        Err(Error::unsupported(
            "Native TCP transport is disabled; enable the tcp feature",
        ))
    }

    /// Parse endpoints and apply the same configuration to either enabled transport.
    /// # Errors
    /// Invalid endpoint lists or transport configuration errors propagate.
    pub fn connect_with_config(endpoint: &str, mut config: ClientConfig<'_>) -> Result<Self> {
        config.endpoints = Endpoint::parse_list(endpoint)?;
        Self::with_config(config)
    }
}

/// Reject Native-only options instead of silently weakening HTTP configuration.
#[cfg(feature = "http")]
fn validate_http_options(config: &ClientConfig<'_>) -> Result<()> {
    let tls = &config.tls;
    let default = crate::TlsSettings::new();
    if tls.ca_pem() != default.ca_pem()
        || tls.use_native_roots() != default.use_native_roots()
        || tls.skip_verification() != default.skip_verification()
        || tls.sni() != default.sni()
        || tls.connect_timeout() != default.connect_timeout()
        || tls.read_timeout() != default.read_timeout()
        || tls.write_timeout() != default.write_timeout()
        || tls.tcp_nodelay() != default.tcp_nodelay()
        || tls.tcp_keepalive() != default.tcp_keepalive()
        || tls.keepalive_idle() != default.keepalive_idle()
        || tls.keepalive_interval() != default.keepalive_interval()
        || tls.keepalive_count() != default.keepalive_count()
    {
        return Err(Error::unsupported(
            "custom Native TLS/socket settings are unsupported for HTTP; use with_timeout for the \
             HTTP deadline",
        ));
    }
    let policy = config.policy;
    let default = crate::FailoverPolicy::new();
    if policy.selection != default.selection
        || policy.retries != default.retries
        || policy.delay != default.delay
        || policy.ping != default.ping
        || policy.threshold != default.threshold
        || policy.cooldown != default.cooldown
        || policy.random_seed != default.random_seed
    {
        return Err(Error::unsupported(
            "custom failover policies are unsupported for HTTP sessions",
        ));
    }
    Ok(())
}

impl Client {
    /// Borrow actual Native handshake information without panicking on HTTP.
    /// # Errors
    /// HTTP endpoints do not have a Native handshake and return Unsupported.
    pub fn try_server_info(&self) -> Result<&crate::ServerInfo> {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.try_server_info(),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native server information requires a Native endpoint",
            )),
        }
    }
}

/// Builder shared by single clients, worker pools, and thread-local factories.
#[derive(Default)]
pub struct ClientBuilder {
    config: ClientConfig<'static>,
}

impl ClientBuilder {
    /// Set one target endpoint, replacing the current endpoint list.
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.config.endpoints = vec![endpoint];
        self
    }

    /// Set the ordered endpoint list used for initial striping and failover.
    pub fn endpoints(mut self, endpoints: Vec<Endpoint>) -> Self {
        self.config.endpoints = endpoints;
        self
    }

    /// Replace all connection settings, including endpoints set by earlier calls.
    pub fn config(mut self, config: ClientConfig<'static>) -> Self {
        self.config = config;
        self
    }

    /// Set the number of local connection slots per calling thread; zero becomes one.
    pub fn workers(mut self, workers: usize) -> Self {
        self.config.workers = workers.max(1);
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

    fn into_config(self) -> Result<ClientConfig<'static>> {
        if self.config.endpoints.is_empty() {
            return Err(Error::Invalid("client endpoint is required".into()));
        }
        self.config.validate()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(port: u16) -> Endpoint {
        let scheme = if cfg!(feature = "http") {
            "http"
        } else {
            "tcp"
        };
        Endpoint::parse(format!("{scheme}://localhost:{port}")).expect("valid endpoint")
    }

    #[test]
    fn later_config_replaces_earlier_endpoint() {
        let config = ClientConfig::from_endpoints(vec![endpoint(8124)]).expect("configuration");
        let built = ClientBuilder::default()
            .endpoint(endpoint(8123))
            .config(config)
            .into_config()
            .expect("valid builder");
        assert_eq!(built.endpoints[0].port(), 8124);
    }

    #[test]
    fn later_endpoint_overrides_config_endpoints() {
        let config = ClientConfig::from_endpoints(vec![endpoint(8124)]).expect("configuration");
        let built = ClientBuilder::default()
            .config(config)
            .endpoint(endpoint(8123))
            .workers(0)
            .into_config()
            .expect("valid builder");
        assert_eq!(built.endpoints[0].port(), 8123);
        assert_eq!(built.workers, 1);
    }
}
