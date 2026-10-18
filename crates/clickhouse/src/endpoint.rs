//! Endpoint parsing for the Native TCP transport.

use std::borrow::Cow;

use crate::error::Error;

/// Transport scheme supported by Ticket 01.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    /// ClickHouse Native TCP.
    Tcp,
}

/// A parsed `tcp://host[:port]` endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    scheme: Scheme,
    host: Cow<'static, str>,
    port: u16,
}

impl Endpoint {
    /// Parse `tcp://host[:port]`; the port defaults to `9000`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Endpoint`] for empty text, a missing host, a
    /// non-numeric or out-of-range port, or trailing path data. Returns
    /// [`Error::UnsupportedScheme`] for any non-`tcp` scheme.
    pub fn parse(text: impl Into<Cow<'static, str>>) -> Result<Self, Error> {
        let text = text.into();
        let parsed = parse_parts(&text);
        match parsed {
            Ok((start, end, port)) => {
                let host = match text {
                    Cow::Borrowed(value) => Cow::Borrowed(&value[start..end]),
                    Cow::Owned(value) => Cow::Owned(value[start..end].to_string()),
                };
                Ok(Self {
                    scheme: Scheme::Tcp,
                    host,
                    port,
                })
            }
            Err(ParseFailure::Scheme(scheme)) => Err(Error::UnsupportedScheme {
                scheme: Cow::Owned(scheme),
            }),
            Err(ParseFailure::Message(message)) => Err(Error::endpoint(text, message)),
        }
    }

    /// Transport scheme.
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Host name or literal address.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// TCP port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

enum ParseFailure {
    Scheme(String),
    Message(&'static str),
}

fn parse_parts(text: &str) -> Result<(usize, usize, u16), ParseFailure> {
    let Some(rest) = text.strip_prefix("tcp://") else {
        if let Some((scheme, _)) = text.split_once("://") {
            return Err(ParseFailure::Scheme(scheme.to_string()));
        }
        return Err(ParseFailure::Message("expected tcp://host:port"));
    };
    if rest.is_empty() {
        return Err(ParseFailure::Message("missing host"));
    }
    let offset = text.len() - rest.len();
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() => {
            if !port.chars().all(|ch| ch.is_ascii_digit()) {
                return Err(ParseFailure::Message("port is not numeric"));
            }
            let port = port
                .parse::<u16>()
                .map_err(|_| ParseFailure::Message("port is out of range"))?;
            (host, port)
        }
        _ => (rest, 9000),
    };
    if host.is_empty() {
        return Err(ParseFailure::Message("missing host"));
    }
    if host.contains('/') {
        return Err(ParseFailure::Message("endpoint must not contain a path"));
    }
    Ok((offset, offset + host.len(), port))
}
