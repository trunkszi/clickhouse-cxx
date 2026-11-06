//! Native session construction, failover, and cold-path health probes.
use std::{collections::hash_map::RandomState, hash::BuildHasher, time::Instant};

use clickhouse_sys::NativeOptions;

use super::NativeSession;
use crate::{
    ClientConfig, CompressionMethod, Endpoint, EndpointHealth, Error, FailoverEvent,
    FailoverPolicy, Scheme, TlsSettings,
    config::FailoverCallback,
    failover::{CircuitStateWord, DialOrder, EligibilityMask, EndpointSelector},
    sys,
};
/// Active endpoints and circuit health. Dial masks at most 1024 endpoints (`Error::Config` above
/// that).
pub(super) struct Managed {
    pub endpoints: Vec<Endpoint>,
    pub current: usize,
    options: Vec<NativeOptions>,
    health: Vec<EndpointHealth>,
    circuits: Vec<CircuitStateWord>,
    policy: FailoverPolicy,
    selector: Option<EndpointSelector>,
    callback: Option<FailoverCallback>,
    random: u64,
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
        let circuits = vec![CircuitStateWord::new(0); config.endpoints.len()];
        let mut managed = Managed {
            selector: EndpointSelector::new(config.endpoints.len()),
            endpoints: config.endpoints,
            current: 0,
            options,
            health,
            circuits,
            policy: config.policy,
            callback: config.callback,
            random: config.policy.random_seed.unwrap_or_else(random_seed),
            callback_error: None,
        };
        let inner = managed.dial(false)?;
        crate::telemetry::control_log!("Native connection ready");
        let server_info = super::read_server_info(&inner);
        Ok(Self {
            inner,
            server_info,
            managed,
            state: super::SessionStateWord::new(0)
                .patch(uarch::patch!(super::SessionStateWord { USABLE: true })),
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
        self.set_probe_needed(true);
        let inner = self.managed.dial(true)?;
        self.server_info = super::read_server_info(&inner);
        self.inner = inner;
        self.set_probe_needed(false);
        self.set_usable(true);
        self.cleanup_error = None;
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
            self.managed.circuits[index].begin_probe();
            let started = Instant::now();
            match sys::new_native_client(&self.managed.options[index]) {
                Ok(mut connection) => match connection.as_mut() {
                    Some(client) => match sys::ffi::ping(client) {
                        Ok(()) => self.managed.record_success(index, started.elapsed()),
                        Err(error) => self.managed.record_failure(index, error.to_string()),
                    },
                    None => self
                        .managed
                        .record_failure(index, "native client handle is null".to_owned()),
                },
                Err(error) => self.managed.record_failure(index, error),
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
    /// Account for a transport failure before selecting a recovery endpoint.
    pub(super) fn record_transport_failure(&mut self, error: &Error) {
        self.set_probe_needed(true);
        self.managed
            .record_failure(self.managed.current, error.to_string());
    }

    /// A complete successful application operation closes the failure streak.
    pub(crate) fn operation_succeeded(&mut self) {
        let index = self.managed.current;
        self.managed.record_success(
            index,
            self.managed.health[index].latency.unwrap_or_default(),
        );
        self.set_probe_needed(false);
    }

    pub(super) fn operation_error(&mut self, error: cxx::Exception) -> Error {
        self.set_probe_needed(true);
        self.managed
            .record_failure(self.managed.current, error.to_string());
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
        if self.probe_needed() || self.managed.policy.ping {
            let started = Instant::now();
            let pinged = match self.inner.as_mut() {
                Some(client) => sys::ffi::ping(client).map_err(|error| error.to_string()),
                None => Err("native client handle is null".to_owned()),
            };
            match pinged {
                Ok(()) => {
                    self.managed
                        .record_connected(self.managed.current, started.elapsed());
                    self.set_probe_needed(false);
                }
                Err(error) => {
                    self.managed.record_failure(self.managed.current, error);
                    let inner = self.managed.dial(true)?;
                    self.server_info = super::read_server_info(&inner);
                    self.inner = inner;
                    self.set_probe_needed(false);
                }
            }
        }
        self.observer_ready()
    }
}
impl Managed {
    fn record_success(&mut self, index: usize, elapsed: std::time::Duration) {
        self.circuits[index].record_success(&mut self.health[index], elapsed);
    }
    fn record_connected(&mut self, index: usize, elapsed: std::time::Duration) {
        self.circuits[index].record_connected(&mut self.health[index], elapsed);
    }
    fn record_failure(&mut self, index: usize, error: String) {
        self.circuits[index].record_failure(&mut self.health[index], self.policy, error);
    }
    pub(super) fn dial(&mut self, rotate: bool) -> Result<cxx::UniquePtr<sys::ffi::Client>, Error> {
        // One sample freezes the candidate set. A cooldown that elapses during
        // this dial does not add an endpoint; a failure that opens a circuit
        // is still skipped by the live check inside the walk.
        let now = Instant::now();
        let Some(mask) = EligibilityMask::snapshot_stored(&self.health, &self.circuits, now) else {
            return Err(Error::Config {
                reason: "endpoint count exceeds the failover eligibility mask".into(),
            });
        };
        if mask.is_empty() {
            return Err(Error::native("all endpoint circuits are open"));
        }
        let selector = self
            .selector
            .ok_or_else(|| Error::native("all endpoint circuits are open"))?;
        let start = selector.select_stored(
            &self.policy,
            (&self.health, &self.circuits),
            self.current,
            now,
            rotate,
            &mut self.random,
        );
        let mut last = Error::native("no endpoint connected");
        for pass in 0..self.policy.retries.max(1) {
            for index in DialOrder::with_selector(start, mask, selector) {
                if !self.circuits[index].available(&self.health[index], Instant::now()) {
                    continue;
                }
                if pass > 0 && !self.policy.delay.is_zero() {
                    clickhouse_sys::wait_retry(self.policy.delay)?;
                }
                if !self.circuits[index].try_acquire_probe(&self.health[index], Instant::now()) {
                    continue;
                }
                let started = Instant::now();
                match sys::new_native_client(&self.options[index]) {
                    Ok(connection) => {
                        self.record_connected(index, started.elapsed());
                        self.transition(index);
                        return Ok(connection);
                    }
                    Err(message) => {
                        let fatal = message.starts_with("fatal:");
                        self.record_failure(index, message.clone());
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::options;
    use crate::{ClientConfig, Endpoint};

    #[test]
    fn unit_submillisecond_timeout_options() {
        let endpoint = Endpoint::parse("tcp://127.0.0.1:9000").expect("valid endpoint");
        let config = ClientConfig::from_endpoints(vec![endpoint.clone()])
            .expect("valid configuration")
            .with_timeout(Duration::from_micros(500));
        let native = options(&endpoint, &config);
        assert_eq!(native.connect_timeout_ms, 1);
        assert_eq!(native.recv_timeout_ms, 1);
        assert_eq!(native.send_timeout_ms, 1);
    }
}
