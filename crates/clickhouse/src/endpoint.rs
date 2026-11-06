//! Endpoint parsing for synchronous Native TCP and HTTP transports.

use std::borrow::Cow;

use uarch::prelude::*;

use crate::error::Error;

/// Wire protocol selected by a server endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    /// ClickHouse Native TCP.
    Tcp,
    /// Native TCP encrypted with rustls.
    Tcps,
    /// Cleartext HTTP.
    Http,
    /// HTTP protected by TLS.
    Https,
}

/// A parsed server endpoint with optional HTTP credentials and database.
#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint {
    scheme: Scheme,
    host: Cow<'static, str>,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    thread_bound: std::marker::PhantomData<*const ()>,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Endpoint")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("database", &self.database)
            .finish()
    }
}

impl Endpoint {
    /// Rebuild a validated endpoint for a calling-thread client factory.
    pub(crate) fn from_parts(scheme: Scheme, host: String, port: u16) -> Self {
        Self {
            scheme,
            host: Cow::Owned(host),
            port,
            user: None,
            password: None,
            database: None,
            thread_bound: std::marker::PhantomData,
        }
    }

    /// Parse `tcp://`, `tcps://`, `http://`, or `https://` server addresses.
    ///
    /// Ports default to 9000, 9440, 8123, and 8443 respectively.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Endpoint`] for empty text, a missing host, a
    /// non-numeric or out-of-range port, or a Native TCP path. Returns
    /// [`Error::UnsupportedScheme`] for any other scheme.
    pub fn parse(text: impl Into<Cow<'static, str>>) -> Result<Self, Error> {
        let text = text.into();
        let (address, user, password, database) = split_http_url(&text)?;
        let parsed = parse_parts(address.as_ref());
        match parsed {
            Ok((scheme, start, end, port)) => {
                let host = Cow::Owned(address[start..end].to_owned());
                Ok(Self {
                    scheme,
                    host,
                    port,
                    user,
                    password,
                    database,
                    thread_bound: std::marker::PhantomData,
                })
            }
            Err(ParseFailure::Scheme(scheme)) => Err(Error::UnsupportedScheme {
                scheme: Cow::Owned(scheme),
            }),
            Err(ParseFailure::Message(message)) => {
                let diagnostic = if user.is_some() {
                    Cow::Owned(address.into_owned())
                } else {
                    text
                };
                Err(Error::endpoint(diagnostic, message))
            }
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
            return Err(empty_endpoint_list());
        }
        Ok(endpoints)
    }

    /// Transport scheme.
    #[hot_inline]
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Host name or literal address.
    #[hot_inline]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// TCP port.
    #[hot_inline]
    pub fn port(&self) -> u16 {
        self.port
    }
    /// User supplied in an HTTP URL.
    #[hot_inline]
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Password supplied in an HTTP URL.
    #[hot_inline]
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Database supplied in an HTTP URL path.
    #[hot_inline]
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }
}

enum ParseFailure {
    Scheme(String),
    Message(&'static str),
}

fn parse_parts(text: &str) -> Result<(Scheme, usize, usize, u16), ParseFailure> {
    let (name, rest) = text
        .split_once("://")
        .ok_or_else(|| parse_failure("expected scheme://host:port"))?;
    let (scheme, default_port) = match name {
        "tcp" => (Scheme::Tcp, 9000),
        "tcps" => (Scheme::Tcps, 9440),
        "http" => (Scheme::Http, 8123),
        "https" => (Scheme::Https, 8443),
        _ => return Err(unknown_scheme(name)),
    };
    if rest.is_empty() {
        return Err(parse_failure("missing host"));
    }
    if ilp_any!(
        rest.contains(['/', '?', '#', '@']),
        rest.chars().any(char::is_whitespace)
    ) {
        return Err(parse_failure(
            "endpoint must contain only a host and optional port",
        ));
    }
    let offset = text.len() - rest.len();
    let (host, port, host_offset) = if let Some(bracketed) = rest.strip_prefix('[') {
        let (host, suffix) = bracketed
            .split_once(']')
            .ok_or_else(|| parse_failure("unterminated IPv6 address"))?;
        host.parse::<std::net::Ipv6Addr>()
            .map_err(|_| parse_failure("invalid IPv6 address"))?;
        let port = if suffix.is_empty() {
            default_port
        } else {
            parse_port(
                suffix
                    .strip_prefix(':')
                    .ok_or_else(|| parse_failure("invalid IPv6 port suffix"))?,
            )?
        };
        (host, port, offset + 1)
    } else {
        match rest.rsplit_once(':') {
            Some((host, port)) => {
                if host.contains(':') {
                    return Err(parse_failure("IPv6 addresses require brackets"));
                }
                (host, parse_port(port)?, offset)
            }
            None => (rest, default_port, offset),
        }
    };
    if host.is_empty() {
        return Err(parse_failure("missing host"));
    }
    Ok((scheme, host_offset, host_offset + host.len(), port))
}

fn parse_port(text: &str) -> Result<u16, ParseFailure> {
    if !ilp_all!(
        !text.is_empty(),
        text.bytes().all(|byte| byte.is_ascii_digit())
    ) {
        return Err(parse_failure("port is not numeric"));
    }
    let port = text
        .parse::<u16>()
        .map_err(|_| parse_failure("port is out of range"))?;
    if port == 0 {
        return Err(parse_failure("port must be nonzero"));
    }
    Ok(port)
}

type HttpUrlParts<'a> = (Cow<'a, str>, Option<String>, Option<String>, Option<String>);

fn split_http_url(text: &str) -> Result<HttpUrlParts<'_>, Error> {
    if !ilp_any!(text.starts_with("http://"), text.starts_with("https://")) {
        return Ok((Cow::Borrowed(text), None, None, None));
    }
    let (scheme, rest) = text
        .split_once("://")
        .ok_or_else(|| Error::protocol("missing URL scheme"))?;
    let (authority, database) = match rest.split_once('/') {
        Some((authority, database))
            if ilp_all!(!database.is_empty(), !database.contains(['/', '?', '#'])) =>
        {
            (authority, Some(database.to_owned()))
        }
        Some((authority, "")) => (authority, None),
        Some(_) => return Err(Error::protocol("invalid HTTP database path")),
        None => (rest, None),
    };
    let (host, user, password) = match authority.rsplit_once('@') {
        Some((info, host)) => {
            let (user, password) = match info.split_once(':') {
                Some((user, password)) => (user, Some(password.to_owned())),
                None => (info, None),
            };
            if user.is_empty() {
                return Err(Error::protocol("empty HTTP user"));
            }
            (host, Some(user.to_owned()), password)
        }
        None => (authority, None, None),
    };
    Ok((
        Cow::Owned(format!("{scheme}://{host}")),
        user,
        password,
        database,
    ))
}

#[cold_sink]
fn parse_failure(message: &'static str) -> ParseFailure {
    ParseFailure::Message(message)
}

#[cold_sink]
fn unknown_scheme(scheme: &str) -> ParseFailure {
    ParseFailure::Scheme(scheme.to_owned())
}

#[cold_sink]
fn empty_endpoint_list() -> Error {
    Error::Config {
        reason: "endpoint list is empty".into(),
    }
}
