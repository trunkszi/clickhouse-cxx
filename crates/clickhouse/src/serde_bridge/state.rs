//! Scalar state carried by each serde traversal.

use uarch::prelude::*;

bitword! {
    pub(super) struct SerdeState(u64) {
        [0] pub(super) CAPTURE,
        [1] pub(super) IN_ROW,
        [2] pub(super) KEY_PENDING,
    }
}

impl SerdeState {
    #[hot_inline]
    pub(super) fn for_row(first: bool) -> Self {
        if first {
            Self::new(0).patch(patch!(SerdeState { CAPTURE: true }))
        } else {
            Self::new(0)
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<SerdeState>() == std::mem::size_of::<u64>());
    assert!(std::mem::align_of::<SerdeState>() == std::mem::align_of::<u64>());
};
