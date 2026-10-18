//! Transport-aware construction from validated owned session options.
use super::{Backend, Client};
use crate::{ClientConfig, Endpoint, Error, Result, Scheme};

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
                        Backend::Http(http) => http.set_timeout(timeout),
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
            let native = super::native_session::NativeSession::new(config)?;
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
