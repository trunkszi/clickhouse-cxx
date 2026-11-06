use std::time::{Duration, Instant};

use super::{EndpointHealth, FailoverPolicy};

#[test]
fn unit_handshake_keeps_failure_streak() {
    let policy = FailoverPolicy::new().with_circuit(3, Duration::from_secs(60));
    let mut health = EndpointHealth::new(0);
    for expected in 1..=3 {
        health.failure(policy, String::from("stream socket disconnected"));
        health.connected(Duration::from_millis(1));
        assert_eq!(health.failures, expected);
    }
    assert!(health.circuit_open);
    assert!(!health.available(Instant::now()));
    health.success(Duration::from_millis(2));
    assert_eq!(health.failures, 0);
    assert!(health.available(Instant::now()));
    assert!(health.last_error.is_none());
}

#[test]
fn unit_half_open_requires_operation_success() {
    let policy = FailoverPolicy::new().with_circuit(1, Duration::ZERO);
    let mut health = EndpointHealth::new(0);
    health.failure(policy, String::from("transport failed"));
    assert!(health.available(Instant::now()));
    health.connected(Duration::from_millis(1));
    assert!(health.circuit_open);
    assert_eq!(health.failures, 1);
    health.failure(policy, String::from("stream failed after handshake"));
    assert_eq!(health.failures, 2);
    health.success(Duration::from_millis(2));
    assert!(!health.circuit_open);
}
