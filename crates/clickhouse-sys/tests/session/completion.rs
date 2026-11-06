//! Synthetic CQEs exercise deadline failures and both target/cancel arrival orders.
use super::*;
fn id(value: u64) -> OperationId {
    OperationId::from_raw((1 << 56) | value)
}
fn cqe(id: u64, result: i32) -> Completion {
    Completion {
        operation_id: self::id(id),
        result,
        flags: 0,
    }
}

#[test]
fn zero_completion_retires_without_timer() {
    let mut state = OperationState::new(id(1), None);
    assert!(state.result().is_none());
    assert!(!state.complete());
    state.record(cqe(1, 0)).unwrap();
    assert_eq!(state.result(), Some(0));
    assert!(state.complete());
    assert!(!state.needs_target_cancel());
    assert!(!state.needs_timer_cancel());
}

#[test]
fn minimum_completion_is_invalid() {
    let mut state = OperationState::new(id(1), Some(id(2)));
    let error = state.record(cqe(2, i32::MIN)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(state.result().is_none());
    assert!(!state.complete());

    state.request_target_cancel(id(3));
    let error = state.record(cqe(3, i32::MIN)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!state.complete());
}

#[test]
fn unit_failed_timer_returns_its_error_with_the_target_still_pending() {
    for errno in [libc::ENOMEM, libc::EINVAL, libc::EFAULT, libc::ECANCELED] {
        let mut state = OperationState::new(id(1), Some(id(2)));
        let error = state.record(cqe(2, -errno)).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(errno));
        assert!(state.result().is_none());
        assert!(!state.complete());
    }
}

#[test]
fn unit_successful_target_waits_for_timer_and_cancel_final_completions() {
    for cancel_first in [false, true] {
        let mut state = OperationState::new(id(1), Some(id(2)));
        state.record(cqe(1, 17)).unwrap();
        assert!(!state.complete());
        state.request_timer_cancel(id(3));
        let events = if cancel_first {
            [cqe(3, 0), cqe(2, -libc::ECANCELED)]
        } else {
            [cqe(2, -libc::ECANCELED), cqe(3, 0)]
        };
        state.record(events[0]).unwrap();
        assert!(!state.complete());
        state.record(events[1]).unwrap();
        assert!(state.complete());
        assert_eq!(state.result(), Some(17));
    }
}

#[test]
fn unit_expired_target_waits_for_its_final_cqe_in_both_cancel_orders() {
    for cancel_first in [false, true] {
        let mut state = OperationState::new(id(1), Some(id(2)));
        state.record(cqe(2, -libc::ETIME)).unwrap();
        assert!(state.expired());
        state.request_target_cancel(id(3));
        let events = if cancel_first {
            [cqe(3, 0), cqe(1, -libc::ECANCELED)]
        } else {
            [cqe(1, -libc::ECANCELED), cqe(3, 0)]
        };
        state.record(events[0]).unwrap();
        assert!(!state.complete());
        state.record(events[1]).unwrap();
        assert!(state.complete());
    }
}

#[test]
fn unit_fatal_target_cancel_keeps_the_target_unretired() {
    for errno in [libc::EINVAL, libc::ENOMEM, libc::EFAULT] {
        let mut state = OperationState::new(id(1), Some(id(2)));
        state.record(cqe(2, -libc::ETIME)).unwrap();
        state.request_target_cancel(id(3));
        let error = state.record(cqe(3, -errno)).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(errno));
        assert!(state.word.matches(pattern!(CompletionWord {
            TARGET_CANCEL_PENDING
        })));
        assert!(state.result().is_none());
        assert!(!state.complete());
    }
}

#[test]
fn unit_fatal_timer_cancel_keeps_the_timer_unretired() {
    for errno in [libc::EINVAL, libc::ENOMEM, libc::EFAULT] {
        let mut state = OperationState::new(id(1), Some(id(2)));
        state.record(cqe(1, 17)).unwrap();
        state.request_timer_cancel(id(3));
        let error = state.record(cqe(3, -errno)).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(errno));
        assert!(state.word.matches(pattern!(CompletionWord {
            TIMER_CANCEL_PENDING
        })));
        assert!(state.word.matches(pattern!(CompletionWord { !TIMER_DONE })));
        assert!(!state.complete());
    }
}

#[test]
fn unit_target_cancel_races_still_require_the_target_final_cqe() {
    for errno in [libc::ENOENT, libc::EALREADY] {
        for cancel_first in [false, true] {
            let mut state = OperationState::new(id(1), Some(id(2)));
            state.record(cqe(2, -libc::ETIME)).unwrap();
            state.request_target_cancel(id(3));
            let events = if cancel_first {
                [cqe(3, -errno), cqe(1, -libc::ECANCELED)]
            } else {
                [cqe(1, -libc::ECANCELED), cqe(3, -errno)]
            };
            state.record(events[0]).unwrap();
            assert!(!state.complete());
            state.record(events[1]).unwrap();
            assert!(state.complete());
        }
    }
}

#[test]
fn unit_timer_cancel_races_still_require_the_timer_final_cqe() {
    for errno in [libc::ENOENT, libc::EALREADY] {
        for cancel_first in [false, true] {
            let mut state = OperationState::new(id(1), Some(id(2)));
            state.record(cqe(1, 17)).unwrap();
            state.request_timer_cancel(id(3));
            let events = if cancel_first {
                [cqe(3, -errno), cqe(2, -libc::ETIME)]
            } else {
                [cqe(2, -libc::ETIME), cqe(3, -errno)]
            };
            state.record(events[0]).unwrap();
            assert!(!state.complete());
            state.record(events[1]).unwrap();
            assert!(state.complete());
        }
    }
}
