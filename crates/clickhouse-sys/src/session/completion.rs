//! Completion ordering and deadline failures for one Native operation.
use std::io;

use liburing::{Completion, OperationId};

pub(super) struct OperationState {
    target: OperationId,
    deadline: Option<OperationId>,
    pub result: Option<i32>,
    pub timer_done: bool,
    pub expired: bool,
    pub cancel_target: Option<OperationId>,
    pub cancel_timer: Option<OperationId>,
    target_cancel_done: bool,
    timer_cancel_done: bool,
}
impl OperationState {
    pub fn new(target: OperationId, deadline: Option<OperationId>) -> Self {
        Self {
            target,
            deadline,
            result: None,
            timer_done: deadline.is_none(),
            expired: false,
            cancel_target: None,
            cancel_timer: None,
            target_cancel_done: false,
            timer_cancel_done: false,
        }
    }

    pub fn record(&mut self, completion: Completion) -> io::Result<()> {
        if completion.operation_id == self.target {
            self.result = Some(completion.result);
        } else if Some(completion.operation_id) == self.deadline {
            self.timer_done = true;
            match completion.result {
                value if value == -libc::ETIME => self.expired = true,
                value if value == -libc::ECANCELED && self.cancel_timer.is_some() => {}
                value if value < 0 => return Err(io::Error::from_raw_os_error(-value)),
                value => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid native timer completion: {value}"),
                    ));
                }
            }
        } else if Some(completion.operation_id) == self.cancel_target {
            check_cancel(completion.result)?;
            self.target_cancel_done = true;
        } else if Some(completion.operation_id) == self.cancel_timer {
            check_cancel(completion.result)?;
            self.timer_cancel_done = true;
        }
        Ok(())
    }

    pub fn complete(&self) -> bool {
        self.result.is_some()
            && self.timer_done
            && (self.cancel_target.is_none() || self.target_cancel_done)
            && (self.cancel_timer.is_none() || self.timer_cancel_done)
    }
}

fn check_cancel(result: i32) -> io::Result<()> {
    match result {
        // These races still require the target's final CQE before operand reuse.
        0 => Ok(()),
        value if value == -libc::ENOENT || value == -libc::EALREADY => Ok(()),
        value if value < 0 && value != i32::MIN => Err(io::Error::from_raw_os_error(-value)),
        value => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid native cancellation completion: {value}"),
        )),
    }
}

#[cfg(test)]
mod tests {
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
    fn unit_failed_timer_returns_its_error_with_the_target_still_pending() {
        for errno in [libc::ENOMEM, libc::EINVAL, libc::EFAULT, libc::ECANCELED] {
            let mut state = OperationState::new(id(1), Some(id(2)));
            let error = state.record(cqe(2, -errno)).unwrap_err();
            assert_eq!(error.raw_os_error(), Some(errno));
            assert!(state.result.is_none());
            assert!(!state.complete());
        }
    }

    #[test]
    fn unit_successful_target_waits_for_timer_and_cancel_final_completions() {
        for cancel_first in [false, true] {
            let mut state = OperationState::new(id(1), Some(id(2)));
            state.record(cqe(1, 17)).unwrap();
            assert!(!state.complete());
            state.cancel_timer = Some(id(3));
            let events = if cancel_first {
                [cqe(3, 0), cqe(2, -libc::ECANCELED)]
            } else {
                [cqe(2, -libc::ECANCELED), cqe(3, 0)]
            };
            state.record(events[0]).unwrap();
            assert!(!state.complete());
            state.record(events[1]).unwrap();
            assert!(state.complete());
            assert_eq!(state.result, Some(17));
        }
    }

    #[test]
    fn unit_expired_target_waits_for_its_final_cqe_in_both_cancel_orders() {
        for cancel_first in [false, true] {
            let mut state = OperationState::new(id(1), Some(id(2)));
            state.record(cqe(2, -libc::ETIME)).unwrap();
            assert!(state.expired);
            state.cancel_target = Some(id(3));
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
            state.cancel_target = Some(id(3));
            let error = state.record(cqe(3, -errno)).unwrap_err();
            assert_eq!(error.raw_os_error(), Some(errno));
            assert!(!state.target_cancel_done);
            assert!(state.result.is_none());
            assert!(!state.complete());
        }
    }

    #[test]
    fn unit_fatal_timer_cancel_keeps_the_timer_unretired() {
        for errno in [libc::EINVAL, libc::ENOMEM, libc::EFAULT] {
            let mut state = OperationState::new(id(1), Some(id(2)));
            state.record(cqe(1, 17)).unwrap();
            state.cancel_timer = Some(id(3));
            let error = state.record(cqe(3, -errno)).unwrap_err();
            assert_eq!(error.raw_os_error(), Some(errno));
            assert!(!state.timer_cancel_done);
            assert!(!state.timer_done);
            assert!(!state.complete());
        }
    }

    #[test]
    fn unit_target_cancel_races_still_require_the_target_final_cqe() {
        for errno in [libc::ENOENT, libc::EALREADY] {
            for cancel_first in [false, true] {
                let mut state = OperationState::new(id(1), Some(id(2)));
                state.record(cqe(2, -libc::ETIME)).unwrap();
                state.cancel_target = Some(id(3));
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
                state.cancel_timer = Some(id(3));
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
}
