//! Checkout restoration, deadlines, worker leases, and thread-local teardown.
use std::time::Duration;

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
fn isolated_leases_and_cached_routes_agree() {
    for workers in [1, 3, 7, 64] {
        let worker = WorkerClient::new(endpoint_config().with_workers(workers))
            .expect("worker configuration");
        for (index, slot) in worker.shared.leased.iter().enumerate() {
            assert_eq!(std::ptr::from_ref(slot) as usize % 128, 0);
            assert_eq!(worker.shared.lease(), Some(index));
        }
        assert_eq!(worker.shared.lease(), None);
        worker.shared.release(workers - 1);
        assert_eq!(worker.shared.lease(), Some(workers - 1));
        for index in [0, 1, 63, 1024, usize::MAX] {
            assert_eq!(
                worker.shared.config.worker_slot(index as u64),
                index % workers
            );
            assert_eq!(worker.shared.config.endpoint_slot(index), 0);
        }
        for index in 0..workers {
            worker.shared.release(index);
        }
    }
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
    assert!(
        WorkerLeaseWord::new(shared.leased[0].load(Ordering::Acquire))
            .matches(pattern!(WorkerLeaseWord { !LEASED }))
    );
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
    assert!(
        WorkerLeaseWord::new(worker.shared.leased[0].load(Ordering::Acquire))
            .matches(pattern!(WorkerLeaseWord { !LEASED }))
    );
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

#[test]
fn round_robin_advances_across_word_end() {
    let owner = WorkerClient::new(endpoint_config().with_workers(3)).unwrap();
    let slot = owner.shared.lease().unwrap();
    let mut local = LocalWorker::new(owner.shared.clone(), slot);
    local.next = usize::MAX;
    let first = local.round_robin(3);
    for offset in 1..=6 {
        assert_eq!(local.round_robin(3), (first + offset) % 3);
    }
}

#[test]
fn identity_exhaustion_fails_closed() {
    const CHILD: &str = "CLICKHOUSE_ID_EXHAUSTION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "threading::tests::identity_exhaustion_fails_closed",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    let _live = ThreadLocalClient::new(endpoint_config()).unwrap();
    NEXT_CLIENT_ID.store(u64::MAX, Ordering::Relaxed);
    assert!(matches!(
        ThreadLocalClient::new(endpoint_config()),
        Err(Error::Config { .. })
    ));
    assert!(matches!(
        WorkerClient::new(endpoint_config()),
        Err(Error::Config { .. })
    ));
    assert_eq!(NEXT_CLIENT_ID.load(Ordering::Relaxed), u64::MAX);
}
