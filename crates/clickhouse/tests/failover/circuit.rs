use super::*;

#[test]
fn stored_probe_lifecycle_preserves_failures() {
    let policy = FailoverPolicy::new().with_circuit(1, Duration::ZERO);
    let mut health = EndpointHealth::new(0);
    let mut state = CircuitStateWord::new(0);
    state.record_failure(&mut health, policy, "application failed".into());
    assert!(state.matches(pattern!(CircuitStateWord { OPEN & !HEALTHY })));
    assert_eq!(state.get(CircuitStateWord::CONSECUTIVE_FAILURES), 1);
    let now = Instant::now();
    assert!(state.try_acquire_probe(&health, now));
    assert!(!state.try_acquire_probe(&health, now));
    #[cfg(feature = "tcp")]
    {
        state.record_connected(&mut health, Duration::from_millis(1));
        assert!(health.circuit_open);
        assert_eq!(health.failures, 1);
        assert!(state.try_acquire_probe(&health, now));
    }
    state.record_success(&mut health, Duration::from_millis(2));
    assert!(state.matches(pattern!(CircuitStateWord {
        HEALTHY & !OPEN & !HALF_OPEN_IN_FLIGHT
    })));
    assert_eq!(state.get(CircuitStateWord::CONSECUTIVE_FAILURES), 0);
    assert_eq!(health.failures, 0);
    assert!(health.last_error.is_none());
}

#[test]
fn saturated_streak_rejects_unknown_deadline() {
    let mut health = EndpointHealth::new(0);
    health.failures = u32::MAX;
    let mut state = CircuitStateWord::new(0).patch(patch!(CircuitStateWord {
        CONSECUTIVE_FAILURES: (u32::MAX as u64),
    }));
    state.record_failure(&mut health, FailoverPolicy::new(), "failed".into());
    health.retry_at = None;
    assert_eq!(state.get(CircuitStateWord::CONSECUTIVE_FAILURES), u32::MAX);
    assert_eq!(health.failures, u32::MAX);
    assert!(!state.try_acquire_probe(&health, Instant::now()));
}
