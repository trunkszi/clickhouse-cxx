//! Endpoint parsing for the Native TCP transport.

use std::borrow::Cow;

use crate::error::Error;

/// Native transport security mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    /// ClickHouse Native TCP.
    Tcp,
    /// Native TCP encrypted with rustls.
    Tcps,
}

/// A parsed Native TCP or TCPS host and port bound to its creating thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    scheme: Scheme,
    host: Cow<'static, str>,
    port: u16,
    thread_bound: std::marker::PhantomData<*const ()>,
}

impl Endpoint {
    /// Parse `tcp://host[:port]` or `tcps://host[:port]`, defaulting to 9000 or 9440.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Endpoint`] for empty text, a missing host, a
    /// non-numeric or out-of-range port, or trailing path data. Returns
    /// [`Error::UnsupportedScheme`] for schemes outside Native TCP and TCPS.
    pub fn parse(text: impl Into<Cow<'static, str>>) -> Result<Self, Error> {
        let text = text.into();
        let scheme = if text.starts_with("tcps://") {
            Scheme::Tcps
        } else {
            Scheme::Tcp
        };
        let parsed = parse_parts(&text);
        match parsed {
            Ok((start, end, port)) => {
                let host = match text {
                    Cow::Borrowed(value) => Cow::Borrowed(&value[start..end]),
                    Cow::Owned(value) => Cow::Owned(value[start..end].to_string()),
                };
                Ok(Self {
                    scheme,
                    host,
                    port,
                    thread_bound: std::marker::PhantomData,
                })
            }
            Err(ParseFailure::Scheme(scheme)) => Err(Error::UnsupportedScheme {
                scheme: Cow::Owned(scheme),
            }),
            Err(ParseFailure::Message(message)) => Err(Error::endpoint(text, message)),
        }
    }

    /// Parse a comma-separated list into owned endpoint addresses.
    ///
    /// # Errors
    /// Returns an endpoint error for malformed or empty lists.
    pub fn parse_list(text: &str) -> Result<Vec<Self>, Error> {
        let endpoints: Result<Vec<_>, _> = text
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| Self::parse(String::from(s)))
            .collect();
        let endpoints = endpoints?;
        if endpoints.is_empty() {
            return Err(Error::Config {
                reason: "endpoint list is empty".into(),
            });
        }
        Ok(endpoints)
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
    let Some(rest) = text
        .strip_prefix("tcp://")
        .or_else(|| text.strip_prefix("tcps://"))
    else {
        if let Some((scheme, _)) = text.split_once("://") {
            return Err(ParseFailure::Scheme(scheme.to_string()));
        }
        return Err(ParseFailure::Message(
            "expected tcp://host:port or tcps://host:port",
        ));
    };
    if rest.is_empty() {
        return Err(ParseFailure::Message("missing host"));
    }
    let offset = text.len() - rest.len();
    if rest.contains(['/', '?', '#', '@']) || rest.chars().any(char::is_whitespace) {
        return Err(ParseFailure::Message(
            "endpoint must contain only a host and port",
        ));
    }
    let default_port = if text.starts_with("tcps://") {
        9440
    } else {
        9000
    };
    let (host, start, port_text) = if let Some(body) = rest.strip_prefix('[') {
        let end = body.find(']').ok_or(ParseFailure::Message(
            "IPv6 host is missing closing bracket",
        ))?;
        let tail = &body[end + 1..];
        let port = if tail.is_empty() {
            None
        } else {
            Some(
                tail.strip_prefix(':')
                    .ok_or(ParseFailure::Message("unexpected IPv6 suffix"))?,
            )
        };
        (&body[..end], offset + 1, port)
    } else {
        match rest.split_once(':') {
            Some((host, port)) => {
                if port.contains(':') {
                    return Err(ParseFailure::Message("IPv6 host requires brackets"));
                }
                (host, offset, Some(port))
            }
            None => (rest, offset, None),
        }
    };
    if host.is_empty() {
        return Err(ParseFailure::Message("missing host"));
    }
    let port = match port_text {
        None => default_port,
        Some(value) => {
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ParseFailure::Message("port is not numeric"));
            }
            let port = value
                .parse::<u16>()
                .map_err(|_| ParseFailure::Message("port is out of range"))?;
            if port == 0 {
                return Err(ParseFailure::Message("port must be nonzero"));
            }
            port
        }
    };
    Ok((start, start + host.len(), port))
}
