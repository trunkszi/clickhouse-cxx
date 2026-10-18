//! Transport endpoint credentials, security, and failover configuration.
use std::{borrow::Cow, time::Duration};

use crate::{
    CompressionMethod, Endpoint, Error, FailoverPolicy, TlsSettings, threading::WorkerBalance,
};

pub(crate) type FailoverCallback = Box<dyn FnMut(&crate::FailoverEvent<'_>)>;

/// Connection options owned by a synchronous client.
pub struct ClientConfig<'a> {
    pub(crate) callback: Option<FailoverCallback>,
    pub(crate) endpoints: Vec<Endpoint>,
    pub(crate) policy: FailoverPolicy,
    pub(crate) user: Cow<'a, str>,
    pub(crate) password: Cow<'a, str>,
    pub(crate) database: Cow<'a, str>,
    pub(crate) tls: TlsSettings<'a>,
    pub(crate) compression: CompressionMethod,
    pub(crate) timeout: Option<Duration>,
    pub(crate) workers: usize,
    pub(crate) balance: WorkerBalance,
}

impl Default for ClientConfig<'static> {
    fn default() -> Self {
        Self {
            callback: None,
            endpoints: Vec::new(),
            policy: FailoverPolicy::new(),
            user: "default".into(),
            password: "".into(),
            database: "default".into(),
            tls: TlsSettings::new(),
            compression: CompressionMethod::None,
            timeout: None,
            workers: 1,
            balance: WorkerBalance::RoundRobin,
        }
    }
}

impl<'a> ClientConfig<'a> {
    /// Parse TCP, TCPS, HTTP, or HTTPS endpoint addresses.
    /// # Errors
    /// Returns an endpoint error when any address is invalid.
    pub fn parse(text: &str) -> Result<Self, Error> {
        Self::from_endpoints(Endpoint::parse_list(text)?)
    }
    /// Configure a nonempty endpoint list.
    /// # Errors
    /// Returns a configuration error for an empty list.
    pub fn from_endpoints(endpoints: Vec<Endpoint>) -> Result<Self, Error> {
        if endpoints.is_empty() {
            return Err(Error::Config {
                reason: "endpoint list is empty".into(),
            });
        }
        Ok(Self {
            callback: None,
            endpoints,
            policy: FailoverPolicy::new(),
            user: "default".into(),
            password: "".into(),
            database: "default".into(),
            tls: TlsSettings::new(),
            compression: CompressionMethod::None,
            timeout: None,
            workers: 1,
            balance: WorkerBalance::RoundRobin,
        })
    }
    /// Validate endpoint features and representable, nonzero deadlines.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.endpoints.is_empty() {
            return Err(Error::Config {
                reason: "endpoint list is empty".into(),
            });
        }
        let native = matches!(
            self.endpoints[0].scheme(),
            crate::Scheme::Tcp | crate::Scheme::Tcps
        );
        for endpoint in &self.endpoints {
            if matches!(endpoint.scheme(), crate::Scheme::Tcp | crate::Scheme::Tcps) != native {
                return Err(Error::Config {
                    reason: "cross-transport failover is unsupported; use homogeneous endpoints"
                        .into(),
                });
            }
            let enabled = match endpoint.scheme() {
                crate::Scheme::Tcp | crate::Scheme::Tcps => cfg!(feature = "tcp"),
                crate::Scheme::Http | crate::Scheme::Https => cfg!(feature = "http"),
            };
            if !enabled {
                return Err(Error::unsupported(
                    "requested endpoint transport feature is disabled",
                ));
            }
            if endpoint.port() == 0 || endpoint.host().contains('\0') {
                return Err(Error::Config {
                    reason: "endpoint host or port is invalid".into(),
                });
            }
        }
        if let Some(timeout) = self.timeout
            && (timeout.is_zero() || timeout.as_millis().max(1) > i64::MAX as u128)
        {
            return Err(Error::Config {
                reason: "timeout must be nonzero and fit in a signed 64-bit millisecond count"
                    .into(),
            });
        }
        Ok(())
    }
    /// Observe endpoint transitions, including initial connection failover.
    #[must_use]
    pub fn on_failover(
        mut self,
        callback: impl FnMut(&crate::FailoverEvent<'_>) + 'static,
    ) -> Self {
        self.callback = Some(Box::new(callback));
        self
    }
    /// Set the endpoint-selection and retry policy.
    #[must_use]
    pub fn with_policy(mut self, policy: FailoverPolicy) -> Self {
        self.policy = policy;
        self
    }
    /// Set the database user.
    #[must_use]
    pub fn with_user(mut self, value: impl Into<Cow<'a, str>>) -> Self {
        self.user = value.into();
        self
    }
    /// Set the database password.
    #[must_use]
    pub fn with_password(mut self, value: impl Into<Cow<'a, str>>) -> Self {
        self.password = value.into();
        self
    }
    /// Set the default database.
    #[must_use]
    pub fn with_database(mut self, value: impl Into<Cow<'a, str>>) -> Self {
        self.database = value.into();
        self
    }
    /// Set certificate trust, SNI, and socket deadlines.
    #[must_use]
    pub fn with_tls(mut self, tls: TlsSettings<'a>) -> Self {
        self.tls = tls;
        self
    }
    /// Set the block compression method for every connection.
    #[must_use]
    pub fn with_compression(mut self, value: CompressionMethod) -> Self {
        self.compression = value;
        self
    }
    /// Override connect, receive, and send deadlines with one duration.
    #[must_use]
    pub fn with_timeout(mut self, value: Duration) -> Self {
        self.timeout = Some(value);
        self
    }
    /// Set the number of local connection slots per calling thread.
    ///
    /// Zero is normalized to one.
    #[must_use]
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers.max(1);
        self
    }
    /// Set the load-balancing strategy used across local slots.
    #[must_use]
    pub fn with_balance(mut self, balance: WorkerBalance) -> Self {
        self.balance = balance;
        self
    }
}
