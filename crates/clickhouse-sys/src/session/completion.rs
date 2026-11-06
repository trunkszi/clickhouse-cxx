//! Completion ordering and deadline failures for one Native operation.
use std::io;

use liburing::{Completion, OperationId};
use uarch::prelude::*;

use super::error::errno;

bitword! {
    struct CompletionWord(u64) {
        [0] TARGET_DONE,
        [1] TIMER_DONE,
        [2] EXPIRED,
        [3] TARGET_CANCEL_PENDING,
        [4] TIMER_CANCEL_PENDING,
    }
}
assert_layout!(
    CompletionWord,
    size = 8,
    align = std::mem::align_of::<u64>()
);

pub(super) struct OperationState {
    target: OperationId,
    deadline: Option<OperationId>,
    result: i32,
    word: CompletionWord,
    cancel_target: Option<OperationId>,
    cancel_timer: Option<OperationId>,
}
impl OperationState {
    pub fn new(target: OperationId, deadline: Option<OperationId>) -> Self {
        Self {
            target,
            deadline,
            result: 0,
            word: if deadline.is_none() {
                CompletionWord::new(0).patch(patch!(CompletionWord { TIMER_DONE: true }))
            } else {
                CompletionWord::new(0)
            },
            cancel_target: None,
            cancel_timer: None,
        }
    }

    #[hot_inline]
    pub fn record(&mut self, completion: Completion) -> io::Result<()> {
        if completion.operation_id == self.target {
            self.result = completion.result;
            self.word = self
                .word
                .patch(patch!(CompletionWord { TARGET_DONE: true }));
        } else if Some(completion.operation_id) == self.deadline {
            self.word = self.word.patch(patch!(CompletionWord { TIMER_DONE: true }));
            match completion.result {
                value if value == -libc::ETIME => {
                    self.word = self.word.patch(patch!(CompletionWord { EXPIRED: true }));
                }
                value if value == -libc::ECANCELED && self.cancel_timer.is_some() => {}
                value if value < 0 && value != i32::MIN => return Err(errno(-value)),
                value => {
                    return Err(invalid_completion("timer", value));
                }
            }
        } else if Some(completion.operation_id) == self.cancel_target {
            check_cancel(completion.result)?;
            self.word = self.word.patch(patch!(CompletionWord {
                TARGET_CANCEL_PENDING: false
            }));
        } else if Some(completion.operation_id) == self.cancel_timer {
            check_cancel(completion.result)?;
            self.word = self.word.patch(patch!(CompletionWord {
                TIMER_CANCEL_PENDING: false
            }));
        }
        Ok(())
    }

    #[hot_inline]
    pub fn result(&self) -> Option<i32> {
        if self.word.matches(pattern!(CompletionWord { TARGET_DONE })) {
            Some(self.result)
        } else {
            None
        }
    }

    #[hot_inline]
    pub fn expired(&self) -> bool {
        self.word.matches(pattern!(CompletionWord { EXPIRED }))
    }

    #[hot_inline]
    pub fn needs_target_cancel(&self) -> bool {
        ilp_all!(
            self.word
                .matches(pattern!(CompletionWord { !TARGET_DONE & EXPIRED })),
            self.cancel_target.is_none()
        )
    }

    #[hot_inline]
    pub fn needs_timer_cancel(&self) -> bool {
        ilp_all!(
            self.word
                .matches(pattern!(CompletionWord { TARGET_DONE & !TIMER_DONE })),
            self.cancel_timer.is_none()
        )
    }

    #[hot_inline]
    pub fn request_target_cancel(&mut self, id: OperationId) {
        self.cancel_target = Some(id);
        self.word = self.word.patch(patch!(CompletionWord {
            TARGET_CANCEL_PENDING: true
        }));
    }

    #[hot_inline]
    pub fn request_timer_cancel(&mut self, id: OperationId) {
        self.cancel_timer = Some(id);
        self.word = self.word.patch(patch!(CompletionWord {
            TIMER_CANCEL_PENDING: true
        }));
    }

    #[hot_inline]
    pub fn complete(&self) -> bool {
        self.word.matches(pattern!(CompletionWord {
            TARGET_DONE & TIMER_DONE & !TARGET_CANCEL_PENDING & !TIMER_CANCEL_PENDING
        }))
    }
}

#[cold_sink]
fn invalid_completion(kind: &str, value: i32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid native {kind} completion: {value}"),
    )
}
#[hot_inline]
fn check_cancel(result: i32) -> io::Result<()> {
    match result {
        // These races still require the target's final CQE before operand reuse.
        0 => Ok(()),
        value if value == -libc::ENOENT || value == -libc::EALREADY => Ok(()),
        value if value < 0 && value != i32::MIN => Err(errno(-value)),
        value => Err(invalid_completion("cancellation", value)),
    }
}

#[cfg(test)]
#[path = "../../tests/session/completion.rs"]
mod tests;
