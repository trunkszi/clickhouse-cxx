//! Native client construction and thread-local factories.
use crate::{
    Client, ClientConfig, Endpoint, Error, Result,
    threading::{ThreadLocalClient, WorkerBalance, WorkerClient},
};
/// Builder shared by single clients, worker pools, and thread-local factories.
#[derive(Default)]
pub struct ClientBuilder {
    endpoints: Option<Vec<Endpoint>>,
    config: ClientConfig<'static>,
}

impl ClientBuilder {
    /// Set one target endpoint.
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoints = Some(vec![endpoint]);
        self
    }

    /// Set the ordered endpoint list used for initial striping and failover.
    pub fn endpoints(mut self, endpoints: Vec<Endpoint>) -> Self {
        self.endpoints = Some(endpoints);
        self
    }

    /// Replace the connection configuration.
    pub fn config(mut self, config: ClientConfig<'static>) -> Self {
        self.config = config;
        self
    }

    /// Set the number of local connection slots per calling thread.
    pub fn workers(mut self, workers: usize) -> Self {
        self.config.workers = workers;
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

    fn into_config(mut self) -> Result<ClientConfig<'static>> {
        if let Some(endpoints) = self.endpoints.take() {
            if endpoints.is_empty() {
                return Err(Error::Invalid("endpoint list is empty".into()));
            }
            self.config.endpoints = endpoints;
        }
        if self.config.endpoints.is_empty() {
            return Err(Error::Invalid("client endpoint is required".into()));
        }
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
