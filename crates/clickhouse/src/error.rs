//! Error surface for the synchronous client.

use std::borrow::Cow;

/// Errors returned by endpoint parsing, the native bridge, or block
/// projections.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The endpoint text could not be parsed.
    #[error("invalid endpoint `{endpoint}`: {message}")]
    Endpoint {
        /// Original endpoint text.
        endpoint: Cow<'static, str>,
        /// Why parsing failed.
        message: Cow<'static, str>,
    },
    /// The endpoint uses a transport that Ticket 01 does not implement.
    #[error("unsupported transport `{scheme}://`; Ticket 01 supports tcp:// only")]
    UnsupportedScheme {
        /// Scheme without the `://` separator.
        scheme: Cow<'static, str>,
    },
    /// `clickhouse-cpp` rejected or failed the operation.
    #[error("native transport failure: {message}")]
    Native {
        /// Diagnostic text from the bridge.
        message: Cow<'static, str>,
    },
    /// A block column was missing, mismatched, or misaligned.
    #[error("block failure: {message}")]
    Block {
        /// Diagnostic text for the projection failure.
        message: Cow<'static, str>,
    },
}

impl Error {
    pub(crate) fn endpoint(
        endpoint: Cow<'static, str>,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self::Endpoint {
            endpoint,
            message: message.into(),
        }
    }

    pub(crate) fn native(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Native {
            message: message.into(),
        }
    }

    pub(crate) fn block(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Block {
            message: message.into(),
        }
    }
}
