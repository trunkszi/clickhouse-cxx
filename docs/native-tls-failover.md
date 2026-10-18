# Native TLS and endpoint failover

`Client` uses the same block transport for `tcp://` and `tcps://` addresses.
A secure address selects a C++ socket factory that delegates synchronous IO to
Rust `rustls` streams. The client links `clickhouse-cpp` with OpenSSL disabled.
The server can use its own TLS implementation independently of the client.

```rust,no_run
use clickhouse::{Client, ClientConfig, EndpointSelection, FailoverPolicy, TlsSettings};
use std::time::Duration;

let ca = std::fs::read("clickhouse-ca.pem")?;
let policy = FailoverPolicy::new()
    .with_selection(EndpointSelection::RoundRobin)
    .with_send_retries(2)
    .with_retry_timeout(Duration::from_millis(200))
    .with_ping_before_query(true)
    .with_circuit(2, Duration::from_secs(10));
let config = ClientConfig::parse("tcps://localhost:9440,tcps://localhost:9441")?
    .with_policy(policy)
    .with_tls(TlsSettings::new().with_ca_pem(ca))
    .on_failover(|event| {
        eprintln!("endpoint transition: {} -> {}", event.old_endpoint, event.new_endpoint);
    });
let mut client = Client::new(config)?;
client.ping()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Thread confinement

Clients, endpoints, failover policies, and connected sessions are `!Send` and
`!Sync`. Construct and use them on one thread. Session IO uses checked
single-threaded interior borrowing without a mutex; reentrant access returns an
error. Failover observers accept single-threaded `FnMut` closures, including
closures that capture `Rc<RefCell<_>>`.

## Trust and socket options

Platform trust roots are enabled by default. `with_ca_pem` adds PEM trust anchors;
`with_native_roots(false)` selects only the supplied anchors. Invalid CA input
and certificate validation failures return errors. An SNI override applies the
configured certificate identity independently of the dialed host. Verification
skip is an explicit local-testing option; handshake signatures remain checked.
A truncated TLS stream is an error rather than a clean end of input.

`TlsSettings` controls connection, read, and write timeouts, TCP_NODELAY, and
keepalive. Zero read or write timeout means no socket deadline; zero connection
timeout selects the dialer's documented default.

## Selection, recovery, and observability

- `RoundRobin` rotates through eligible endpoints on reconnect.
- `Random` selects using a client-local pseudorandom sequence.
- `LatencyAware` prefers the smallest measured connection or probe latency.
- `Sticky` retains the current endpoint while it remains available.

Consecutive endpoint failures open its circuit for the configured cooldown.
`endpoint_health()` exposes the current health, failure count, latency, and last
error; `probe_health()` makes independent authenticated probes and closes circuits
that recover. Probes do not replace a healthy active connection.
`reset_connection_endpoint()` explicitly reconnects under the selection policy.

Connection attempts and preflight probes can fail over before a statement is
sent. Statements and inserts are not automatically replayed: a transport failure
can leave their server-side outcome uncertain. An operation error requires a
fresh probe before the next operation can proceed. Fatal trust, authentication,
and protocol failures do not silently select a less secure fallback.

Register a callback on `ClientConfig` to observe transitions during the initial
connection. Each event contains the old and new endpoint indices and addresses.
The optional `tracing` feature emits one structured warning for a successful
failover episode. Callback panics are contained; `take_callback_error()` exposes
and acknowledges the diagnostic before further operations are admitted.

This configuration and scheduler currently serve Native TCP and TCPS. HTTP
transport integration must use the same endpoint policy and recovery contract.

## Live verification

`cargo test -p clickhouse --features tracing --test tcps_failover_live` creates
an isolated `clickhouse/clickhouse-server:26.8.2.7` container with ephemeral host
ports. Its generated certificate mounts are readable by the container user.
The fixture removes its container and temporary certificates on completion or
Drop. Tests cover secure block inserts and selects, trust and SNI rejection,
explicit verification skip, endpoint selection, circuit state, recovery probes,
and structured transition telemetry.
