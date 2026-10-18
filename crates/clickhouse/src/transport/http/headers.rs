//! Shared response metadata for buffered and borrowed HTTP readers.

use crate::{Error, HttpVersionChoice};

/// Limit buffered wire and decoded bodies independently.
pub(super) const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Metadata resets at every status line, including interim responses.
#[derive(Default)]
pub(super) struct ResponseHeaders {
    pub(super) status: u32,
    pub(super) encoding: String,
    pub(super) exception: Option<String>,
    pub(super) negotiated: Option<HttpVersionChoice>,
}

impl ResponseHeaders {
    /// Parse one curl-delivered header line without duplicating transport rules.
    pub(super) fn observe(&mut self, line: &[u8], requested: HttpVersionChoice) {
        let text = String::from_utf8_lossy(line);
        if line.starts_with(b"HTTP/") {
            *self = Self::default();
            self.status = text
                .split_ascii_whitespace()
                .nth(1)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            self.negotiated = if line.starts_with(b"HTTP/2 ") {
                Some(if requested == HttpVersionChoice::Http2PriorKnowledge {
                    requested
                } else {
                    HttpVersionChoice::Http2Tls
                })
            } else if line.starts_with(b"HTTP/1.") {
                Some(HttpVersionChoice::Http1)
            } else {
                None
            };
        } else if let Some((name, value)) = text.trim().split_once(':') {
            if name.eq_ignore_ascii_case("content-encoding") {
                self.encoding = value.trim().to_owned();
            } else if name.eq_ignore_ascii_case("x-clickhouse-exception-code") {
                self.exception = Some(value.trim().to_owned());
            }
        }
    }

    /// Reject exception-bearing successful statuses as server failures.
    pub(super) fn check(&self, diagnostic: &[u8]) -> Result<(), Error> {
        if !(200..300).contains(&self.status) || self.exception.is_some() {
            let message = super::lossy_text(diagnostic);
            return Err(Error::Http {
                status: self.status,
                message: match &self.exception {
                    Some(code) => format!("ClickHouse exception {code}: {message}"),
                    None => message,
                },
            });
        }
        Ok(())
    }
}
