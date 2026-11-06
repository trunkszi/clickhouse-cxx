use super::*;
#[test]
fn cached_ring_wraps_extreme_indices() {
    for count in [1, 3, 7, 64, 65, 1024] {
        let selector = EndpointSelector::new(count).unwrap();
        for index in [0, count - 1, count, usize::MAX - 1, usize::MAX] {
            assert_eq!(selector.fold(index), index % count);
        }
        let health: Vec<_> = (0..count).map(EndpointHealth::new).collect();
        let mut random = 0;
        assert_eq!(
            selector.select(
                &FailoverPolicy::new(),
                &health,
                usize::MAX,
                Instant::now(),
                true,
                &mut random
            ),
            0
        );
        let mask = EligibilityMask::snapshot(&health, Instant::now()).unwrap();
        let order: Vec<_> = DialOrder::with_selector(usize::MAX, mask, selector).collect();
        assert_eq!(order.len(), count);
        assert!(order.iter().all(|index| *index < count));
    }
    assert!(EndpointSelector::new(0).is_none());
}

#[test]
fn ring_visits_each_slot_near_word_end() {
    for count in [3, 7, 65, 1024] {
        let selector = EndpointSelector::new(count).unwrap();
        let health: Vec<_> = (0..count).map(EndpointHealth::new).collect();
        let mask = EligibilityMask::snapshot(&health, Instant::now()).unwrap();
        for start in [usize::MAX - 1, usize::MAX] {
            let expected: Vec<_> = (0..count)
                .map(|offset| (start % count + offset) % count)
                .collect();
            assert_eq!(
                DialOrder::with_selector(start, mask, selector).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(DialOrder::new(start, mask).collect::<Vec<_>>(), expected);
        }
    }
}

#[test]
fn bitmap_edges_preserve_ring_membership() {
    let mut health: Vec<_> = (0..1024).map(EndpointHealth::new).collect();
    let now = Instant::now();
    let full = EligibilityMask::snapshot(&health, now).unwrap();
    assert_eq!(DialOrder::new(1023, full).count(), 1024);
    for index in [0, 63, 64, 1023] {
        health[index].circuit_open = true;
        health[index].retry_at = None;
    }
    let mask = EligibilityMask::snapshot(&health, now).unwrap();
    for index in [0, 63, 64, 1023, usize::MAX] {
        assert!(!mask.contains(index));
    }
    let order: Vec<_> = DialOrder::new(usize::MAX, mask).collect();
    assert_eq!(order.len(), 1020);
    assert!(
        order
            .iter()
            .all(|&index| ![0, 63, 64, 1023].contains(&index))
    );
    let empty = EligibilityMask::snapshot(&[], now).unwrap();
    assert_eq!(DialOrder::new(usize::MAX, empty).next(), None);
}
