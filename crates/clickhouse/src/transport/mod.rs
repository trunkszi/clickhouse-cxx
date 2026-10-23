//! Synchronous HTTP transport and negotiated protocol observations.

mod http;

pub(crate) use http::HttpTransport;
pub use http::{HttpPreferences, HttpRequest};
