//! Capability-aware access to authenticated handshake metadata.
use super::{Backend, Client};
#[cfg(feature = "http")]
use crate::Error;
use crate::{Result, ServerInfo};

impl Client {
    /// Borrow actual Native handshake information without panicking on HTTP.
    /// # Errors
    /// HTTP endpoints do not have a Native handshake and return Unsupported.
    pub fn try_server_info(&self) -> Result<&ServerInfo> {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.try_server_info(),
            #[cfg(feature = "http")]
            Backend::Http(_) => Err(Error::unsupported(
                "Native server information requires a Native endpoint",
            )),
        }
    }
}
