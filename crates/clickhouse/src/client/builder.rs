//! Client construction and thread-local factories.
use crate::{
    Client, ClientConfig, Endpoint, Error, Result,
    threading::{ThreadLocalClient, WorkerBalance, WorkerClient},
};
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
