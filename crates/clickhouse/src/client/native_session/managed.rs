//! Native session construction, failover, and cold-path health probes.
use std::{collections::hash_map::RandomState, hash::BuildHasher, time::Instant};

use clickhouse_sys::NativeOptions;

use super::NativeSession;
use crate::{
    ClientConfig, CompressionMethod, Endpoint, EndpointHealth, EndpointSelection, Error,
    FailoverEvent, FailoverPolicy, Scheme, TlsSettings, config::FailoverCallback, sys,
};
pub(super) struct Managed {
    pub endpoints: Vec<Endpoint>,
    pub current: usize,
    options: Vec<NativeOptions>,
    health: Vec<EndpointHealth>,
    policy: FailoverPolicy,
    callback: Option<FailoverCallback>,
    random: u64,
    needs_probe: bool,
    callback_error: Option<Error>,
}
impl NativeSession {
    /// Open a Native session using configured failover and certificate trust.
    /// # Errors
    /// Returns the last connection failure when no available endpoint connects.
    pub fn new(config: ClientConfig<'_>) -> Result<Self, Error> {
        Self::with_config(config)
    }
    /// Open a Native session with endpoint policy and TLS options.
    /// # Errors
    /// Returns a connection error after every eligible endpoint has failed.
    pub fn with_config(config: ClientConfig<'_>) -> Result<Self, Error> {
        if Instant::now().checked_add(config.policy.cooldown).is_none() {
            return Err(Error::Config {
                reason: "circuit cooldown exceeds the monotonic clock range".into(),
            });
        }
        let options = config
            .endpoints
            .iter()
            .map(|endpoint| options(endpoint, &config))
            .collect();
        let health = (0..config.endpoints.len())
            .map(EndpointHealth::new)
            .collect();
        let mut managed = Managed {
            endpoints: config.endpoints,
            current: 0,
            options,
            health,
            policy: config.policy,
            callback: config.callback,
            random: config.policy.random_seed.unwrap_or_else(random_seed),
            needs_probe: false,
            callback_error: None,
        };
        let inner = managed.dial(false)?;
        crate::telemetry::control_log!("Native connection ready");
        Ok(Self {
            inner,
            managed,
            usable: true,
            cleanup_error: None,
            _not_send: std::marker::PhantomData,
        })
    }
    /// Register an observer for endpoint transitions after construction.
    pub fn on_failover(&mut self, callback: impl FnMut(&FailoverEvent<'_>) + 'static) {
        self.managed.callback = Some(Box::new(callback));
    }
    /// Return the active configured endpoint.
    pub fn current_endpoint(&self) -> &Endpoint {
        self.endpoint()
    }
    /// Return the active endpoint-selection policy.
    pub fn policy(&self) -> FailoverPolicy {
        self.managed.policy
    }
    /// Reconnect to a healthy endpoint without replaying a statement.
    /// # Errors
    /// Returns a connection error if every eligible endpoint fails.
    pub fn reset_connection_endpoint(&mut self) -> Result<(), Error> {
        self.managed.needs_probe = true;
        self.inner = self.managed.dial(true)?;
        self.managed.needs_probe = false;
        Ok(())
    }
    /// Read accumulated health and circuit diagnostics without probing or clearing them.
    pub fn endpoint_health(&self) -> &[EndpointHealth] {
        &self.managed.health
    }
    /// Probe all configured endpoints and close circuits after successful recovery.
    ///
    /// Probes establish independent authenticated sessions and never replace a healthy active
    /// session.
    pub fn probe_health(&mut self) -> Vec<EndpointHealth> {
        for index in 0..self.managed.options.len() {
            let started = Instant::now();
            match sys::new_native_client(&self.managed.options[index]) {
                Ok(mut connection) => match sys::ffi::ping(connection.pin_mut()) {
                    Ok(()) => self.managed.health[index].success(started.elapsed()),
                    Err(error) => {
                        self.managed.health[index].failure(self.managed.policy, error.to_string())
                    }
                },
                Err(error) => self.managed.health[index].failure(self.managed.policy, error),
            }
        }
        self.managed.health.clone()
    }
    /// Query one nonempty UInt64 cell through the canonical block transport.
    /// # Errors
    /// Returns a query or schema error if the first result has no UInt64 cell.
    pub fn query_u64(&mut self, sql: &str) -> Result<u64, Error> {
        let blocks = self.query_blocks(sql)?;
        for block in &blocks {
            if block.rows() > 0 {
                return block
                    .column_slice::<u64>(&block.column_name(0))?
                    .first()
                    .copied()
                    .ok_or_else(|| Error::native("query returned no UInt64 cell"));
            }
        }
        Err(Error::native("query returned no UInt64 cell"))
    }
    /// Retrieve a contained failover-observer panic before allowing more operations.
    pub fn take_callback_error(&mut self) -> Option<Error> {
        self.managed.callback_error.take()
    }
    pub(super) fn operation_error(&mut self, error: cxx::Exception) -> Error {
        self.managed.needs_probe = true;
        self.managed.health[self.managed.current].failure(self.managed.policy, error.to_string());
        Error::native(error.to_string())
    }
    /// Reject operations until a contained observer failure is acknowledged.
    fn observer_ready(&self) -> Result<(), Error> {
        if self.managed.callback_error.is_some() {
            return Err(Error::native(
                "failover observer panicked; retrieve callback error before reuse",
            ));
        }
        Ok(())
    }

    pub(super) fn prepare(&mut self) -> Result<(), Error> {
        self.ready()?;
        self.observer_ready()?;
        if self.managed.needs_probe || self.managed.policy.ping {
            let started = Instant::now();
            match sys::ffi::ping(self.inner.pin_mut()) {
                Ok(()) => {
                    self.managed.health[self.managed.current].success(started.elapsed());
                    self.managed.needs_probe = false;
                }
                Err(error) => {
                    self.managed.health[self.managed.current]
                        .failure(self.managed.policy, error.to_string());
                    self.inner = self.managed.dial(true)?;
                    self.managed.needs_probe = false;
                }
            }
        }
        self.observer_ready()
    }
}
impl Managed {
    pub(super) fn dial(&mut self, rotate: bool) -> Result<cxx::UniquePtr<sys::ffi::Client>, Error> {
        let count = self.endpoints.len();
        let mut candidates: Vec<_> = (0..count)
            .filter(|&i| self.health[i].available(Instant::now()))
            .collect();
        if candidates.is_empty() {
            return Err(Error::native("all endpoint circuits are open"));
        }
        let start = match self.policy.selection {
            EndpointSelection::Sticky => self.current,
            EndpointSelection::RoundRobin => (self.current + usize::from(rotate)) % count,
            EndpointSelection::Random => {
                self.random ^= self.random << 13;
                self.random ^= self.random >> 7;
                self.random ^= self.random << 17;
                (self.random as usize) % count
            }
            EndpointSelection::LatencyAware => self
                .health
                .iter()
                .filter(|h| h.available(Instant::now()))
                .min_by_key(|h| h.latency.unwrap_or(std::time::Duration::MAX))
                .map_or(self.current, |h| h.index),
        };
        candidates.sort_by_key(|&index| (index + count - start) % count);
        let mut last = Error::native("no endpoint connected");
        for pass in 0..self.policy.retries.max(1) {
            for &index in &candidates {
                if !self.health[index].available(Instant::now()) {
                    continue;
                }
                if pass > 0 && !self.policy.delay.is_zero() {
                    std::thread::sleep(self.policy.delay);
                }
                let started = Instant::now();
                match sys::new_native_client(&self.options[index]) {
                    Ok(connection) => {
                        self.health[index].success(started.elapsed());
                        self.transition(index);
                        return Ok(connection);
                    }
                    Err(message) => {
                        let fatal = message.starts_with("fatal:");
                        self.health[index].failure(self.policy, message.clone());
                        last = Error::native(message);
                        if fatal {
                            return Err(last);
                        }
                    }
                }
            }
        }
        Err(last)
    }
    fn transition(&mut self, index: usize) {
        if index == self.current {
            return;
        }
        let event = FailoverEvent {
            old_index: self.current,
            new_index: index,
            old_endpoint: std::borrow::Cow::Owned(endpoint_token(&self.endpoints[self.current])),
            new_endpoint: std::borrow::Cow::Owned(endpoint_token(&self.endpoints[index])),
        };
        crate::telemetry::warn_failover(&event);
        self.current = index;
        if let Some(callback) = &mut self.callback
            && std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(&event))).is_err()
        {
            self.callback_error = Some(Error::native("failover observer panicked"));
        }
    }
}
fn options(endpoint: &Endpoint, config: &ClientConfig<'_>) -> NativeOptions {
    let connect_timeout = config
        .timeout
        .unwrap_or_else(|| config.tls.connect_timeout());
    let read_timeout = config.timeout.unwrap_or_else(|| config.tls.read_timeout());
    let write_timeout = config.timeout.unwrap_or_else(|| config.tls.write_timeout());
    NativeOptions {
        host: String::from(endpoint.host()),
        port: endpoint.port(),
        user: String::from(config.user.as_ref()),
        password: String::from(config.password.as_ref()),
        database: String::from(config.database.as_ref()),
        secure: endpoint.scheme() == Scheme::Tcps,
        sni: String::from(config.tls.sni().as_ref()),
        ca_pem: config.tls.ca_pem().to_vec(),
        use_native_roots: config.tls.use_native_roots(),
        skip_verification: config.tls.skip_verification(),
        connect_timeout_ms: TlsSettings::timeout_ms(connect_timeout),
        recv_timeout_ms: TlsSettings::timeout_ms(read_timeout),
        send_timeout_ms: TlsSettings::timeout_ms(write_timeout),
        tcp_nodelay: config.tls.tcp_nodelay(),
        tcp_keepalive: config.tls.tcp_keepalive(),
        keepalive_idle_secs: config.tls.keepalive_idle().as_secs(),
        keepalive_interval_secs: config.tls.keepalive_interval().as_secs(),
        keepalive_count: config.tls.keepalive_count(),
        send_retries: 0,
        retry_timeout_secs: 0,
        ping_before_query: false,
        compression: match config.compression {
            CompressionMethod::None => 0,
            CompressionMethod::Lz4 => 1,
            CompressionMethod::Zstd => 2,
        },
        ..NativeOptions::default()
    }
}

fn endpoint_token(endpoint: &Endpoint) -> String {
    let scheme = if endpoint.scheme() == Scheme::Tcps {
        "tcps"
    } else {
        "tcp"
    };
    format!("{scheme}://{}:{}", endpoint.host(), endpoint.port())
}

/// Seed each client independently from the platform-backed standard hash state.
fn random_seed() -> u64 {
    RandomState::new()
        .hash_one("clickhouse endpoint selection")
        .max(1)
}
