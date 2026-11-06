use std::time::{Duration, Instant};

use super::{
    CircuitPhase, DialOrder, EligibilityMask, EndpointHealth, EndpointSelection, FailoverPolicy,
};

fn row(index: usize, eligible: bool, latency: Option<Duration>) -> EndpointHealth {
    let now = Instant::now();
    EndpointHealth {
        index,
        healthy: eligible,
        failures: u32::from(!eligible),
        latency,
        circuit_open: !eligible,
        retry_at: (!eligible).then(|| now + Duration::from_secs(60)),
        last_error: None,
    }
}

#[test]
fn select_start_matches_dial_rules() {
    let now = Instant::now();
    let health = vec![
        row(0, true, Some(Duration::from_micros(40))),
        row(1, false, Some(Duration::from_micros(1))),
        row(2, true, Some(Duration::from_micros(10))),
        row(3, true, Some(Duration::from_micros(25))),
    ];
    let mut state = 0u64;
    let sticky = FailoverPolicy::new().with_selection(EndpointSelection::Sticky);
    assert_eq!(
        sticky.select_index(&health, 1, now, false, &mut state),
        Some(1)
    );
    assert_eq!(
        sticky.select_index(&health, 6, now, false, &mut state),
        Some(2)
    );
    assert_eq!(state, 0);

    let robin = FailoverPolicy::new().with_selection(EndpointSelection::RoundRobin);
    assert_eq!(
        robin.select_index(&health, 6, now, true, &mut state),
        Some(3)
    );
    assert_eq!(
        robin.select_index(&health, 6, now, false, &mut state),
        Some(2)
    );
    assert_eq!(
        robin.select_index(&health, 3, now, true, &mut state),
        Some(0)
    );

    let random = FailoverPolicy::new().with_selection(EndpointSelection::Random);
    let mut random_state = 0u64;
    assert_eq!(
        random.select_index(&health, 0, now, false, &mut random_state),
        Some(0)
    );
    assert_eq!(random_state, 0);
    random_state = 7;
    let once = random.select_index(&health, 0, now, false, &mut random_state);
    let mut manual = 7u64;
    manual ^= manual << 13;
    manual ^= manual >> 7;
    manual ^= manual << 17;
    assert_eq!(random_state, manual);
    assert_eq!(once, Some((manual as usize) % health.len()));
    let advanced = random_state;
    assert!(
        random
            .select_index(&health, 0, now, false, &mut random_state)
            .is_some()
    );
    assert_ne!(random_state, advanced);

    let mut skewed = health.clone();
    skewed[2].index = 3;
    let latency = FailoverPolicy::new().with_selection(EndpointSelection::LatencyAware);
    assert_eq!(
        latency.select_index(&skewed, 0, now, false, &mut state),
        Some(3)
    );
    skewed[2].index = 99;
    assert_eq!(
        latency.select_index(&skewed, 0, now, false, &mut state),
        Some(2)
    );
    let closed: Vec<_> = (0..4).map(|index| row(index, false, None)).collect();
    assert_eq!(
        latency.select_index(&closed, 6, now, false, &mut state),
        Some(2)
    );
    assert!(
        sticky
            .select_index(&[], 0, now, false, &mut state)
            .is_none()
    );
}

#[test]
fn snapshot_order_stays_frozen() {
    let now = Instant::now();
    let mut health = vec![
        row(0, true, None),
        row(1, false, None),
        row(2, true, None),
        row(3, true, None),
    ];
    let mask = EligibilityMask::snapshot(&health, now).expect("mask fits");
    let mut state = 0u64;
    let sticky_start = FailoverPolicy::new()
        .with_selection(EndpointSelection::Sticky)
        .select_index(&health, 1, now, false, &mut state)
        .expect("start");
    assert_eq!(sticky_start, 1);
    assert!(!mask.contains(sticky_start));
    let order: Vec<_> = DialOrder::new(sticky_start, mask).collect();
    assert_eq!(order, vec![2, 3, 0]);

    health[1].circuit_open = true;
    health[1].retry_at = Some(now);
    assert!(health[1].phase(now).is_eligible());
    assert!(!mask.contains(1));
    let frozen: Vec<_> = DialOrder::new(sticky_start, mask).collect();
    assert_eq!(frozen, vec![2, 3, 0]);

    health[2].circuit_open = true;
    health[2].retry_at = Some(now + Duration::from_secs(30));
    assert!(mask.contains(2));
    let live: Vec<_> = DialOrder::new(0, mask)
        .filter(|index| health[*index].phase(now).is_eligible())
        .collect();
    assert_eq!(live, vec![0, 3]);

    let wide = vec![EndpointHealth::new(0); super::ELIGIBILITY_LIMIT + 1];
    assert!(EligibilityMask::snapshot(&wide, now).is_none());
}

#[test]
fn open_without_deadline_stays_open_cooldown() {
    let now = Instant::now();
    let open = EndpointHealth {
        index: 0,
        healthy: false,
        failures: 3,
        latency: None,
        circuit_open: true,
        retry_at: None,
        last_error: None,
    };
    assert_eq!(open.phase(now), CircuitPhase::OpenCooldown);
    assert!(!open.phase(now).is_eligible());
    let mut expired = open.clone();
    expired.retry_at = Some(now);
    assert_eq!(expired.phase(now), CircuitPhase::HalfOpenProbe);
    let mut closed = open;
    closed.circuit_open = false;
    closed.healthy = true;
    closed.failures = 0;
    assert_eq!(closed.phase(now), CircuitPhase::ClosedHealthy);
    closed.healthy = false;
    assert_eq!(closed.phase(now), CircuitPhase::ClosedDegraded);
}
