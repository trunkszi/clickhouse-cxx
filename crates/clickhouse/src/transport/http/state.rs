//! Stored protocol and reconfiguration state for the owning HTTP thread.

use uarch::prelude::*;

use crate::HttpVersionChoice;

bitword! {
    pub(super) struct TransportState(u64) {
        [0] pub(super) RESET_REQUIRED,
        [1] NEGOTIATED,
        [2..=3] REQUESTED: u8,
        [4..=5] OBSERVED: u8,
    }
}

impl TransportState {
    #[hot_inline]
    pub(super) fn for_version(version: HttpVersionChoice) -> Self {
        Self::new(0).with_version(version)
    }

    #[hot_inline]
    pub(super) fn with_version(self, version: HttpVersionChoice) -> Self {
        self.patch(Self::REQUESTED.patch(encode(version)))
    }

    #[hot_inline]
    pub(super) fn version(self) -> HttpVersionChoice {
        decode(self.get(Self::REQUESTED))
    }

    #[hot_inline]
    pub(super) fn negotiated(self) -> Option<HttpVersionChoice> {
        if self.matches(pattern!(TransportState { NEGOTIATED })) {
            Some(decode(self.get(Self::OBSERVED)))
        } else {
            None
        }
    }

    #[hot_inline]
    pub(super) fn observe(self, version: Option<HttpVersionChoice>) -> Self {
        match version {
            Some(version) => self
                .patch(Self::OBSERVED.patch(encode(version)))
                .patch(patch!(TransportState { NEGOTIATED: true })),
            None => self.patch(patch!(TransportState { NEGOTIATED: false })),
        }
    }
}

#[hot_inline]
fn encode(version: HttpVersionChoice) -> u8 {
    match version {
        HttpVersionChoice::Http2PriorKnowledge => 0,
        HttpVersionChoice::Http2Tls => 1,
        HttpVersionChoice::Http1 => 2,
        HttpVersionChoice::Negotiate => 3,
    }
}

#[hot_inline]
fn decode(code: u8) -> HttpVersionChoice {
    match code {
        0 => HttpVersionChoice::Http2PriorKnowledge,
        1 => HttpVersionChoice::Http2Tls,
        2 => HttpVersionChoice::Http1,
        _ => HttpVersionChoice::Negotiate,
    }
}

#[hot_inline]
pub(super) fn valid_credentials(value: &str) -> bool {
    reduce_four_way!(
        value.as_bytes(),
        true,
        true,
        |valid, byte| valid & ilp_all!(*byte >= 32, *byte != 127),
        |left, right| left & right,
    )
}

const _: () = {
    assert!(std::mem::size_of::<TransportState>() == std::mem::size_of::<u64>());
    assert!(std::mem::align_of::<TransportState>() == std::mem::align_of::<u64>());
};
