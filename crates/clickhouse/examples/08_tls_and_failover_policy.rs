//! Native TCPS certificate trust, endpoint selection, and observable recovery.
//!
//! Inspect configuration without opening a connection:
//!
//! ```text
//! cargo run -p clickhouse --example 08_tls_and_failover_policy
//! ```
//!
//! Query a running TCPS server with a trusted certificate:
//!
//! ```text
//! CLICKHOUSE_TCPS_URLS=tcps://127.0.0.1:9440 \
//! CLICKHOUSE_CA_PEM=/path/to/ca.pem CLICKHOUSE_TLS_SNI=localhost \
//!   cargo run -p clickhouse --example 08_tls_and_failover_policy
//! ```
//!
//! `CLICKHOUSE_TCPS_URLS` accepts comma-separated TCPS endpoints. Credentials
//! use `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE`.
//! Without `CLICKHOUSE_CA_PEM`, certificate verification uses platform roots.
//! Without `CLICKHOUSE_TLS_SNI`, the dialed host supplies the certificate name.
//! Verification is always enabled; an untrusted certificate fails the connection.
//!
//! Client, endpoint, and policy objects stay on one thread. The observer uses
//! `Rc<RefCell<_>>` and executes synchronously on the connection-owning thread.
//! This example issues only reads. Failed writes must be reconciled by the caller
//! before a deliberate retry because a server may already have committed them.

use std::{cell::RefCell, env, error::Error, rc::Rc, time::Duration};

use clickhouse::{
    Client, ClientConfig, Endpoint, EndpointSelection, FailoverPolicy, Scheme, TlsSettings,
};

const SELECTIONS: [EndpointSelection; 4] = [
    EndpointSelection::RoundRobin,
    EndpointSelection::Random,
    EndpointSelection::LatencyAware,
    EndpointSelection::Sticky,
];

type Transition = (usize, usize, String, String);

/// Construct each selection policy with an explicit retry and circuit budget.
fn policy(selection: EndpointSelection) -> FailoverPolicy {
    FailoverPolicy::new()
        .with_selection(selection)
        .with_send_retries(1)
        .with_retry_timeout(Duration::from_millis(100))
        .with_ping_before_query(true)
        .with_circuit(2, Duration::from_secs(10))
}

/// Read an optional UTF-8 environment value without hiding malformed values.
fn setting(name: &str) -> Result<Option<String>, env::VarError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Show defaults and construct all four policies without making network requests.
fn offline_configuration() -> Result<(), Box<dyn Error>> {
    let endpoints = Endpoint::parse_list("tcp://localhost,tcps://localhost")?;
    println!("Native TCP default port: {}", endpoints[0].port());
    println!("Native TCPS default port: {}", endpoints[1].port());
    for selection in SELECTIONS {
        let config = ClientConfig::parse("tcps://localhost")?
            .with_policy(policy(selection))
            .with_tls(TlsSettings::new().with_sni("localhost"));
        drop(config);
        println!("Configured {selection:?}; connection attempts are deferred until Client::new");
    }
    Ok(())
}

/// Exercise authenticated TCPS reads, probes, and explicit reconnection for every policy.
fn live_queries(urls: &str) -> Result<(), Box<dyn Error>> {
    let endpoints = Endpoint::parse_list(urls)?;
    if endpoints
        .iter()
        .any(|endpoint| endpoint.scheme() != Scheme::Tcps)
    {
        return Err("CLICKHOUSE_TCPS_URLS must contain only tcps:// endpoints".into());
    }
    let ca_path = setting("CLICKHOUSE_CA_PEM")?;
    let use_native_roots = ca_path.is_none();
    let ca = match ca_path {
        Some(path) => std::fs::read(path)?,
        None => Vec::new(),
    };
    if !use_native_roots && ca.is_empty() {
        return Err("CLICKHOUSE_CA_PEM must contain a nonempty PEM trust anchor".into());
    }
    let user = setting("CLICKHOUSE_USER")?.unwrap_or_else(|| String::from("default"));
    let password = setting("CLICKHOUSE_PASSWORD")?.unwrap_or_default();
    let database = setting("CLICKHOUSE_DATABASE")?.unwrap_or_else(|| String::from("default"));
    let sni = setting("CLICKHOUSE_TLS_SNI")?.unwrap_or_default();

    for selection in SELECTIONS {
        let transitions = Rc::new(RefCell::new(Vec::<Transition>::new()));
        let observed = Rc::clone(&transitions);
        let tls = TlsSettings::new()
            .with_ca_pem(ca.as_slice())
            .with_native_roots(use_native_roots)
            .with_sni(sni.as_str())
            .with_connect_timeout(Duration::from_secs(3))
            .with_read_timeout(Duration::from_secs(5))
            .with_write_timeout(Duration::from_secs(5));
        let config = ClientConfig::parse(urls)?
            .with_user(user.as_str())
            .with_password(password.as_str())
            .with_database(database.as_str())
            .with_tls(tls)
            .with_policy(policy(selection))
            .on_failover(move |event| {
                observed.borrow_mut().push((
                    event.old_index,
                    event.new_index,
                    String::from(event.old_endpoint.as_ref()),
                    String::from(event.new_endpoint.as_ref()),
                ));
            });
        let mut client = Client::new(config)?;
        if let Some(error) = client.take_callback_error() {
            return Err(error.into());
        }
        assert_eq!(client.query_u64("SELECT toUInt64(1)")?, 1);
        println!("{selection:?}: authenticated TCPS SELECT succeeded");

        let health = client.probe_health();
        for endpoint in &health {
            println!(
                "endpoint={} healthy={} failures={} circuit_open={} latency={:?} retry_at={:?} \
                 last_error={:?}",
                endpoint.index,
                endpoint.healthy,
                endpoint.failures,
                endpoint.circuit_open,
                endpoint.latency,
                endpoint.retry_at,
                endpoint.last_error,
            );
        }
        if !health.iter().any(|endpoint| endpoint.healthy) {
            return Err("no endpoint passed the authenticated health probe".into());
        }
        client.reset_connection_endpoint()?;
        if let Some(error) = client.take_callback_error() {
            return Err(error.into());
        }
        assert_eq!(client.query_u64("SELECT toUInt64(1)")?, 1);
        for (old, new, old_address, new_address) in transitions.borrow().iter() {
            println!("endpoint transition {old}->{new}: {old_address} -> {new_address}");
        }
        println!(
            "{selection:?}: explicit reconnect succeeded; dropping the client closes its session"
        );
    }
    Ok(())
}

/// Configure policies offline and query TCPS only when an endpoint list is supplied.
fn main() -> Result<(), Box<dyn Error>> {
    offline_configuration()?;
    match setting("CLICKHOUSE_TCPS_URLS")? {
        Some(urls) if !urls.is_empty() => live_queries(&urls)?,
        Some(_) => return Err("CLICKHOUSE_TCPS_URLS must not be empty".into()),
        None => {
            println!("Live TCPS queries were not requested; set CLICKHOUSE_TCPS_URLS to run them")
        }
    }
    Ok(())
}
