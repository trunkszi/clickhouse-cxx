//! E2E lifecycle and scenario checks for live Native TLS and endpoint failover.

use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use clickhouse::{
    ArrayColumn, Client, ClientConfig, EndpointSelection, FailoverPolicy, NativeBlock, TlsSettings,
    WireColumn,
};
#[path = "support/tcps_server.rs"]
mod tcps_server;
use tcps_server::{Server, issue_certs, server_report};

#[test]
fn live_clickhouse_transports_and_failover() {
    if let Err(err) = run_live() {
        panic!("{err}");
    }
}

fn run_live() -> Result<(), String> {
    let mut server = Server::start()?;
    let exercised = exercise(&server);
    if let Err(err) = exercised {
        let report = server_report(&server.name);
        return Err(format!("{err}\n{report}"));
    }
    server.cleanup()?;
    Ok(())
}

fn exercise(server: &Server) -> Result<(), String> {
    let ca = server.ca_pem.as_str();
    let ports = &server.ports;
    for (label, list) in [
        ("tcp", format!("tcp://127.0.0.1:{}", ports.tcp)),
        ("tcps", format!("tcps://127.0.0.1:{}", ports.tcps)),
    ] {
        let value = query_one(&list, tls_for(ca), false)?;
        if value != 1 {
            return Err(format!("{label} returned {value}"));
        }
        if query_one(&list, tls_for(ca), true)? != 1 {
            return Err(format!("{label} health probe failed"));
        }
    }
    for (label, port) in [("tcp", ports.tcp), ("tcps", ports.tcps)] {
        let list = format!("{label}://127.0.0.1:1,{label}://127.0.0.1:{port}");
        let events = query_failover(&list, tls_for(ca))?;
        if events != vec![(0usize, 1usize)] {
            return Err(format!("{label} failover events: {events:?}"));
        }
    }
    native_fallback(server)?;
    native_block_roundtrip(server)?;
    certificate_rejections(server)?;
    policy_matrix(server)?;
    policy_selection(server)?;
    observer_panic(server)?;
    #[cfg(feature = "tracing")]
    tracing_transition(server)?;
    Ok(())
}

fn native_fallback(server: &Server) -> Result<(), String> {
    for (secure, port) in [(false, server.ports.tcp), (true, server.ports.tcps)] {
        let options = clickhouse_sys::NativeOptions {
            host: "127.0.0.1".into(),
            port: 1,
            secure,
            sni: "localhost".into(),
            ca_pem: server.ca_pem.as_bytes().into(),
            use_native_roots: false,
            connect_timeout_ms: 2_000,
            recv_timeout_ms: 10_000,
            send_timeout_ms: 10_000,
            send_retries: 0,
            retry_timeout_secs: 0,
            extra_hosts: vec!["127.0.0.1".into()],
            extra_ports: vec![port],
            ..clickhouse_sys::NativeOptions::default()
        };
        let mut client = clickhouse_sys::new_native_client(&options)?;
        clickhouse_sys::ffi::ping(client.pin_mut()).map_err(|e| e.to_string())?;
        let actual = clickhouse_sys::ffi::current_port(&client).map_err(|e| e.to_string())?;
        if actual != port {
            return Err(format!(
                "native fallback chose port {actual}, expected {port}"
            ));
        }
        let blocks = clickhouse_sys::ffi::query_blocks(client.pin_mut(), "SELECT toUInt64(1)")
            .map_err(|e| e.to_string())?;
        let count = clickhouse_sys::ffi::block_list_len(&blocks);
        if count == 0 {
            return Err("native fallback lost its result block".into());
        }
    }
    Ok(())
}

fn native_block_roundtrip(server: &Server) -> Result<(), String> {
    let list = format!("tcps://127.0.0.1:{}", server.ports.tcps);
    let config = ClientConfig::parse(&list)
        .map_err(|e| e.to_string())?
        .with_tls(tls_for(&server.ca_pem));
    let mut client = Client::new(config).map_err(|e| e.to_string())?;
    client
        .execute(
            "CREATE TABLE tls_roundtrip (id UInt64, delta Int64, price Float64, symbol String, \
             tags Array(UInt32)) ENGINE=Memory",
        )
        .map_err(|e| e.to_string())?;
    let mut block = NativeBlock::new();
    block
        .push_u64("id", &[1, 2, 3])
        .map_err(|e| e.to_string())?;
    block
        .push_i64("delta", &[-1, 0, 1])
        .map_err(|e| e.to_string())?;
    block
        .push_f64("price", &[10.5, 20.25, 30.125])
        .map_err(|e| e.to_string())?;
    block
        .push_string("symbol", &["alpha".into(), "beta".into(), "gamma".into()])
        .map_err(|e| e.to_string())?;
    let mut tags = ArrayColumn::new("UInt32").map_err(|e| e.to_string())?;
    for values in [&[1_u32, 2][..], &[][..], &[3][..]] {
        let values = WireColumn::fixed(values).map_err(|e| e.to_string())?;
        tags.append_row(&values).map_err(|e| e.to_string())?;
    }
    block
        .push_wire_column("tags", &WireColumn::Array(Box::new(tags)))
        .map_err(|e| e.to_string())?;
    client
        .insert_block("tls_roundtrip", &mut block)
        .map_err(|e| e.to_string())?;
    let blocks = client
        .query_blocks("SELECT id,delta,price,symbol FROM tls_roundtrip ORDER BY id")
        .map_err(|e| e.to_string())?;
    let rows: usize = blocks.iter().map(NativeBlock::rows).sum();
    if rows != 3 {
        return Err(format!("TLS block roundtrip returned {rows} rows"));
    }
    let ids: Vec<u64> = blocks
        .iter()
        .filter(|b| b.rows() != 0)
        .flat_map(|b| {
            b.column_slice::<u64>("id")
                .expect("UInt64 projection")
                .iter()
                .copied()
        })
        .collect();
    if ids != [1, 2, 3] {
        return Err(format!("TLS block roundtrip changed IDs: {ids:?}"));
    }
    let matching = client
        .query_u64(
            "SELECT countIf((id=1 AND symbol='alpha' AND tags=[1,2]) OR (id=2 AND symbol='beta' \
             AND empty(tags)) OR (id=3 AND symbol='gamma' AND tags=[3])) FROM tls_roundtrip",
        )
        .map_err(|e| e.to_string())?;
    if matching != 3 {
        return Err(format!("TLS composite payload mismatch: {matching} rows"));
    }
    Ok(())
}

fn certificate_rejections(server: &Server) -> Result<(), String> {
    let list = format!("tcps://127.0.0.1:{}", server.ports.tcps);
    let wrong_ca = issue_certs()?;
    for tls in [
        tls_for(&wrong_ca.ca_pem),
        tls_for(&server.ca_pem).with_sni("invalid.example"),
    ] {
        if query_one(&list, tls.clone(), false).is_ok() {
            return Err("TLS accepted an untrusted certificate or wrong server name".into());
        }
        let downgrade = format!(
            "tcps://127.0.0.1:{},tcp://127.0.0.1:{}",
            server.ports.tcps, server.ports.tcp
        );
        let events = Rc::new(Cell::new(0));
        let recorded = Rc::clone(&events);
        let config = ClientConfig::parse(&downgrade)
            .map_err(|e| e.to_string())?
            .with_tls(tls)
            .with_policy(policy())
            .on_failover(move |_| {
                recorded.set(recorded.get() + 1);
            });
        if Client::new(config).is_ok() || events.get() != 0 {
            return Err("fatal certificate verification silently downgraded to plaintext".into());
        }
    }
    let insecure = tls_for("")
        .with_sni("invalid.example")
        .with_skip_verification(true);
    if query_one(&list, insecure, false)? != 1 {
        return Err("explicit insecure TLS query failed".into());
    }
    Ok(())
}

fn policy_matrix(server: &Server) -> Result<(), String> {
    let list = format!("tcps://127.0.0.1:1,tcps://127.0.0.1:{}", server.ports.tcps);
    for selection in [
        EndpointSelection::RoundRobin,
        EndpointSelection::Random,
        EndpointSelection::LatencyAware,
        EndpointSelection::Sticky,
    ] {
        let config = ClientConfig::parse(&list)
            .map_err(|e| e.to_string())?
            .with_tls(tls_for(&server.ca_pem))
            .with_policy(
                policy()
                    .with_selection(selection)
                    .with_random_seed(2)
                    .with_circuit(1, Duration::from_millis(10)),
            );
        let mut client = Client::new(config).map_err(|e| e.to_string())?;
        for _ in 0..3 {
            if client
                .query_u64("SELECT toUInt64(1)")
                .map_err(|e| e.to_string())?
                != 1
            {
                return Err(format!("{selection:?} returned an invalid scalar"));
            }
        }
        let health = client.probe_health();
        if health.len() != 2
            || health[0].healthy
            || !health[1].healthy
            || health[0].failures == 0
            || health[0].retry_at.is_none()
            || health[1].latency.is_none()
        {
            return Err(format!("{selection:?} omitted endpoint health"));
        }
        client
            .reset_connection_endpoint()
            .map_err(|e| e.to_string())?;
        if client
            .query_u64("SELECT toUInt64(1)")
            .map_err(|e| e.to_string())?
            != 1
        {
            return Err(format!("{selection:?} failed after explicit recovery"));
        }
    }
    Ok(())
}

fn observer_panic(server: &Server) -> Result<(), String> {
    let list = format!("tcps://127.0.0.1:1,tcps://127.0.0.1:{}", server.ports.tcps);
    let config = ClientConfig::parse(&list)
        .map_err(|e| e.to_string())?
        .with_tls(tls_for(&server.ca_pem))
        .with_policy(policy())
        .on_failover(|_| panic!("observer panic fixture"));
    let mut client = Client::new(config).map_err(|e| e.to_string())?;
    if client.query_u64("SELECT toUInt64(1)").is_ok() {
        return Err("observer panic did not block session reuse".into());
    }
    if client.take_callback_error().is_none() || client.take_callback_error().is_some() {
        return Err("observer panic diagnostic was not consumed exactly once".into());
    }
    if client
        .query_u64("SELECT toUInt64(1)")
        .map_err(|e| e.to_string())?
        != 1
    {
        return Err("session did not recover after consuming the observer diagnostic".into());
    }
    Ok(())
}

fn policy_selection(server: &Server) -> Result<(), String> {
    let list = format!(
        "tcps://127.0.0.1:{0},tcps://127.0.0.2:{0}",
        server.ports.tcps
    );
    for selection in [
        EndpointSelection::RoundRobin,
        EndpointSelection::Sticky,
        EndpointSelection::Random,
        EndpointSelection::LatencyAware,
    ] {
        let config = ClientConfig::parse(&list)
            .map_err(|e| e.to_string())?
            .with_tls(tls_for(&server.ca_pem))
            .with_policy(policy().with_selection(selection).with_random_seed(2));
        let mut client = Client::new(config).map_err(|e| e.to_string())?;
        let initial = client.current_endpoint().host().to_owned();
        let health = client.probe_health();
        if health.iter().any(|endpoint| !endpoint.healthy) {
            return Err(format!("{selection:?} could not probe both live endpoints"));
        }
        let fastest = health
            .iter()
            .min_by_key(|endpoint| endpoint.latency)
            .expect("two endpoints")
            .index;
        let mut seen = std::collections::BTreeSet::new();
        for turn in 0..8 {
            client
                .reset_connection_endpoint()
                .map_err(|e| e.to_string())?;
            let host = client.current_endpoint().host();
            match selection {
                EndpointSelection::Sticky => assert_eq!(host, initial),
                EndpointSelection::RoundRobin => assert_eq!(
                    host,
                    if turn % 2 == 0 {
                        "127.0.0.2"
                    } else {
                        "127.0.0.1"
                    }
                ),
                EndpointSelection::LatencyAware if turn == 0 => assert_eq!(
                    host,
                    if fastest == 0 {
                        "127.0.0.1"
                    } else {
                        "127.0.0.2"
                    }
                ),
                _ => {}
            }
            seen.insert(host.to_owned());
            assert_eq!(
                client
                    .query_u64("SELECT toUInt64(1)")
                    .map_err(|e| e.to_string())?,
                1
            );
        }
        if selection == EndpointSelection::Random && seen.len() != 2 {
            return Err("random selection never visited the second healthy endpoint".into());
        }
    }
    Ok(())
}

#[cfg(feature = "tracing")]
fn tracing_transition(server: &Server) -> Result<(), String> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tracing::Subscriber;
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt},
    };
    struct Warnings(Arc<AtomicUsize>);
    impl<S: Subscriber> Layer<S> for Warnings {
        fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
            if event.metadata().target() == "clickhouse::failover"
                && *event.metadata().level() == tracing::Level::WARN
            {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(Warnings(Arc::clone(&count)));
    let list = format!(
        "tcps://127.0.0.1:1,tcps://127.0.0.1:2,tcps://127.0.0.1:{}",
        server.ports.tcps
    );
    tracing::subscriber::with_default(subscriber, || {
        query_failover(&list, tls_for(&server.ca_pem))
    })?;
    let warnings = count.load(Ordering::Relaxed);
    if warnings != 1 {
        return Err(format!("one failover emitted {warnings} warnings"));
    }
    Ok(())
}

fn query_one(list: &str, tls: TlsSettings<'_>, ping: bool) -> Result<u64, String> {
    let config = ClientConfig::parse(list)
        .map_err(|err| err.to_string())?
        .with_tls(tls)
        .with_policy(policy().with_ping_before_query(ping));
    let mut client = Client::new(config).map_err(|err| err.to_string())?;
    client
        .query_u64("SELECT toUInt64(1)")
        .map_err(|err| err.to_string())
}

fn query_failover(list: &str, tls: TlsSettings<'_>) -> Result<Vec<(usize, usize)>, String> {
    let events = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&events);
    let config = ClientConfig::parse(list)
        .map_err(|err| err.to_string())?
        .with_tls(tls)
        .with_policy(policy())
        .on_failover(move |event| {
            recorded
                .borrow_mut()
                .push((event.old_index, event.new_index));
        });
    let mut client = Client::new(config).map_err(|err| err.to_string())?;
    let value = client
        .query_u64("SELECT toUInt64(1)")
        .map_err(|err| err.to_string())?;
    if value != 1 {
        return Err(format!("failover query returned {value}"));
    }
    let port = client.current_endpoint().port();
    if port <= 2 {
        return Err("failover stayed on the dead endpoint".to_owned());
    }
    Ok(events.borrow().clone())
}

fn policy() -> FailoverPolicy {
    FailoverPolicy::new()
        .with_retry_timeout(Duration::ZERO)
        .with_send_retries(1)
}

fn tls_for(ca_pem: &str) -> TlsSettings<'_> {
    TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(Cow::Borrowed(ca_pem.as_bytes()))
        .with_sni("localhost")
        .with_connect_timeout(Duration::from_secs(5))
        .with_read_timeout(Duration::from_secs(10))
        .with_write_timeout(Duration::from_secs(10))
}
