//! Unit coverage for local checkout, restoration, deadlines, and worker leases.
use super::*;

fn endpoint_config() -> ClientConfig<'static> {
    let scheme = if cfg!(feature = "http") {
        "http"
    } else {
        "tcp"
    };
    ClientConfig::parse(&format!("{scheme}://127.0.0.1:1")).expect("valid endpoint")
}

#[test]
fn positive_submillisecond_timeout_is_valid() {
    let config = endpoint_config().with_timeout(Duration::from_micros(500));
    let owned = OwnedConfig::capture(&config).expect("positive timeout");
    assert_eq!(owned.timeout, Some(Duration::from_micros(500)));
    assert!(OwnedConfig::capture(&endpoint_config().with_timeout(Duration::ZERO)).is_err());
}

#[test]
fn checkout_restores_after_unwind() {
    let entries = RefCell::new(HashMap::from([(1, Some(7))]));
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut first = Checkout::take(&entries, 1).expect("first checkout");
        *first.value.as_mut().expect("stored value") = 8;
        assert!(matches!(
            Checkout::take(&entries, 1),
            Err(Error::Config { .. })
        ));
        let mut second = Checkout::take(&entries, 2).expect("independent checkout");
        second.value = Some(9);
        panic!("closure panic");
    }));
    assert!(result.is_err());
    assert_eq!(entries.borrow().get(&1), Some(&Some(8)));
    assert_eq!(entries.borrow().get(&2), Some(&Some(9)));
}

#[cfg(feature = "http")]
#[test]
fn http_factories_allow_distinct_nesting() {
    let first = ThreadLocalClient::new(endpoint_config()).expect("first factory");
    let second = ThreadLocalClient::new(endpoint_config()).expect("second factory");
    first
        .with(|_| {
            second.with(|_| Ok(()))?;
            assert!(matches!(first.with(|_| Ok(())), Err(Error::Config { .. })));
            Ok(())
        })
        .expect("nested different handles");
    let id = first.id;
    drop(first);
    THREAD_CLIENTS.with(|entries| assert!(!entries.borrow().contains_key(&id)));
}

#[cfg(feature = "http")]
#[test]
fn http_workers_restore_after_panic() {
    let first = WorkerClient::new(endpoint_config()).expect("first worker");
    let second = WorkerClient::new(endpoint_config()).expect("second worker");
    let result = catch_unwind(AssertUnwindSafe(|| {
        first.with::<()>(|_| {
            second.with(|_| Ok(()))?;
            assert!(matches!(first.with(|_| Ok(())), Err(Error::Config { .. })));
            panic!("user callback panic");
        })
    }));
    assert!(result.is_err());
    first.with(|_| Ok(())).expect("restored worker");
    let shared = first.shared.clone();
    let id = first.id;
    drop(first);
    LOCAL_WORKERS.with(|entries| assert!(!entries.borrow().contains_key(&id)));
    assert!(!shared.leased[0].load(Ordering::Acquire));
}

#[cfg(feature = "tcp")]
#[test]
fn failed_initial_connection_releases_lease() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let address = listener.local_addr().expect("bound address");
    drop(listener);
    let config = ClientConfig::parse(&format!("tcp://{address}"))
        .expect("valid endpoint")
        .with_timeout(Duration::from_millis(1));
    let worker = WorkerClient::new(config).expect("worker handle");
    assert!(worker.with(|_| Ok(())).is_err());
    assert!(!worker.shared.leased[0].load(Ordering::Acquire));
    LOCAL_WORKERS.with(|entries| assert!(!entries.borrow().contains_key(&worker.id)));
}

#[cfg(feature = "http")]
#[test]
fn http_workers_stripe_single_endpoint_configs() {
    let config =
        ClientConfig::parse("http://127.0.0.1:8123,http://127.0.0.1:8124").expect("HTTP endpoints");
    let owned = OwnedConfig::capture(&config).expect("worker configuration");
    assert_eq!(owned.to_config(0).endpoints.len(), 1);
    assert_eq!(owned.to_config(0).endpoints[0].port(), 8123);
    assert_eq!(owned.to_config(1).endpoints[0].port(), 8124);
}

#[cfg(all(feature = "tcp", feature = "http"))]
#[test]
fn mixed_transport_factories_are_rejected() {
    let config = ClientConfig::parse("tcp://127.0.0.1:9000,http://127.0.0.1:8123")
        .expect("parse endpoint syntax");
    assert!(OwnedConfig::capture(&config).is_err());
}

/// Integration: worker configuration reaches a real synchronous HTTP peer.
#[cfg(feature = "http")]
#[test]
fn integration_http_worker_query() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
    let address = listener.local_addr().expect("listener address");
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("HTTP connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bounded read");
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            stream.read_exact(&mut byte).expect("request header");
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let headers = String::from_utf8(request).expect("HTTP text");
        assert!(headers.contains("X-ClickHouse-User: worker-user"));
        assert!(headers.contains("database=worker-db"));
        let body_len = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let mut body = vec![0; body_len];
        stream.read_exact(&mut body).expect("HTTP request body");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .expect("HTTP response");
    });
    let config = ClientConfig::parse(&format!("http://{address}"))
        .expect("HTTP configuration")
        .with_user("worker-user")
        .with_database("worker-db")
        .with_timeout(Duration::from_secs(5));
    let worker = WorkerClient::new(config).expect("worker factory");
    worker
        .with(|client| {
            client.set_http_version(crate::HttpVersionChoice::Http1)?;
            client.execute("SELECT 1")
        })
        .expect("HTTP worker execution");
    peer.join().expect("HTTP peer completed");
}

/// Lifecycle: factory handles may outlive their caches during TLS destruction.
#[cfg(feature = "http")]
#[test]
fn factories_survive_tls_cache_teardown() {
    thread_local! {
        static LOCAL: ThreadLocalClient = ThreadLocalClient::new(endpoint_config())
            .expect("thread-local factory");
        static WORKER: WorkerClient = WorkerClient::new(endpoint_config())
            .expect("worker factory");
    }
    let worker = std::thread::spawn(|| {
        // Initialize handles first, then caches; reverse destruction drops caches first.
        LOCAL.with(|_| {});
        WORKER.with(|_| {});
        LOCAL.with(|factory| factory.with(|_| Ok(())).expect("cached client"));
        WORKER.with(|factory| factory.with(|_| Ok(())).expect("cached worker"));
    });
    worker.join().expect("TLS teardown must not panic");
}
