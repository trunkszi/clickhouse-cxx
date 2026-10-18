//! Synchronous HTTP transport on `curl`'s event loop.
//!
//! Cleartext `http://` calls request HTTP/2 through `CURL_HTTP_VERSION_2` and
//! accept the HTTP/1.1 response ClickHouse 26.8 returns. That server rejects
//! an HTTP/2 prior-knowledge preface. `https://` calls request HTTP/2 via TLS.
//! The multi handle keeps the connection, with multiplexing, `TCP_NODELAY`,
//! and `CURLOPT_PIPEWAIT` enabled. Bodies may be LZ4 frames or zstd frames.

use std::{
    borrow::Cow,
    cell::RefCell,
    io::{Read, Write},
    marker::PhantomData,
    net::{IpAddr, ToSocketAddrs},
    rc::Rc,
    time::Duration,
};

use curl::{
    EventLoop,
    easy::{Easy, HttpVersion, List},
};

use crate::{
    endpoint::Scheme,
    error::Error,
    query::{Compression, HttpVersionChoice, Query},
};

mod headers;
mod streaming;
use headers::{MAX_RESPONSE_BYTES, ResponseHeaders};

/// HTTP options the transport applies to every call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpPreferences {
    /// HTTP version requested from curl.
    pub version: HttpVersionChoice,
    /// `TCP_NODELAY` is set on the transfer.
    pub tcp_nodelay: bool,
    /// The curl multi cache may reuse the connection.
    pub keep_alive: bool,
    /// HTTP/2 multiplexing is enabled on the multi handle.
    pub multiplex: bool,
}

/// Owns the event loop and callbacks on the constructing thread.
///
/// The persistent marker prevents Send/Sync even between transfers, when
/// request-local callback buffers no longer exist. No event-loop handle or
/// borrowed response view may be moved to another thread.
pub(crate) struct HttpTransport {
    scheme: Scheme,
    host: String,
    port: u16,
    user: String,
    password: String,
    version: HttpVersionChoice,
    timeout: Duration,
    loop_: EventLoop,
    negotiated: Option<HttpVersionChoice>,
    owner_thread: PhantomData<Rc<()>>,
}

impl HttpTransport {
    /// Opens a curl event loop aimed at `host`. No TCP connection yet.
    ///
    /// # Errors
    ///
    /// Returns an error when curl rejects the event loop or multiplexing option.
    pub(crate) fn open(
        scheme: Scheme,
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        version: HttpVersionChoice,
    ) -> Result<Self, Error> {
        let backend = curl::Version::get();
        if !backend.feature_http2() {
            return Err(Error::protocol("libcurl was built without HTTP/2 support"));
        }
        if !backend
            .ssl_version()
            .is_some_and(|name| name.to_ascii_lowercase().contains("rustls"))
        {
            return Err(Error::protocol("libcurl must use the rustls TLS backend"));
        }
        if user.contains(['\r', '\n', '\0']) || password.contains(['\r', '\n', '\0']) {
            return Err(Error::protocol(
                "HTTP credentials contain prohibited control characters",
            ));
        }
        let loop_ = EventLoop::new().map_err(Error::curl)?;
        loop_.pipelining(false, true).map_err(Error::curl)?;
        Ok(Self {
            scheme,
            host: host.to_owned(),
            port,
            user: user.to_owned(),
            password: password.to_owned(),
            version,
            timeout: Duration::from_secs(60),
            loop_,
            negotiated: None,
            owner_thread: PhantomData,
        })
    }

    /// Set a positive deadline without rounding sub-millisecond values to zero.
    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = if timeout.is_zero() {
            timeout
        } else {
            Duration::from_millis(timeout.as_millis().max(1).min(u128::from(u64::MAX)) as u64)
        };
    }

    /// Return the configured connection-reuse and protocol preferences.
    pub(crate) fn preferences(&self) -> HttpPreferences {
        HttpPreferences {
            version: self.version,
            tcp_nodelay: true,
            keep_alive: true,
            multiplex: true,
        }
    }

    /// Change the HTTP version requested on subsequent transfers.
    pub(crate) fn set_version(&mut self, version: HttpVersionChoice) -> Result<(), Error> {
        self.version = version;
        Ok(())
    }

    /// Return the protocol observed in the most recent response status line.
    pub(crate) fn negotiated_version(&self) -> Option<HttpVersionChoice> {
        self.negotiated
    }

    /// Posts `sql` and returns the response body.
    ///
    /// `database` is sent as a query parameter. Compression wraps a non-empty
    /// body and asks the server to compress the response the same way.
    ///
    /// # Errors
    ///
    /// Returns an error when curl fails, the status is outside 200..299, or the
    /// compressed body cannot be decoded.
    pub(crate) fn execute(
        &mut self,
        database: &str,
        query: &Query<'_>,
        sql: &str,
        body: &[u8],
        compression: Compression,
        dedup: Option<&str>,
    ) -> Result<Vec<u8>, Error> {
        let compression = query.compression_override().unwrap_or(compression);
        let (payload, encoding) = compress_body(compression, body)?;
        let url = self.url(database, query, sql, dedup, compression)?;
        let headers = self.headers(encoding, compression);
        let (encoded, metadata) = self.transfer(&url, &headers, payload.as_ref())?;
        let decoded = decompress_body(metadata.encoding.clone(), &encoded)?;
        metadata.check(&decoded)?;
        Ok(decoded)
    }

    fn url(
        &self,
        database: &str,
        query: &Query<'_>,
        sql: &str,
        dedup: Option<&str>,
        compression: Compression,
    ) -> Result<String, Error> {
        if query.has_event_callbacks() || query.tracing_context.is_some() {
            return Err(Error::unsupported(
                "Native event callbacks and tracing are unavailable on HTTP",
            ));
        }
        if !query.params.is_empty() {
            return Err(Error::unsupported(
                "Native typed query parameters are unavailable on HTTP",
            ));
        }
        if query.settings.values().any(|field| field.flags != 0) {
            return Err(Error::unsupported(
                "Native setting flags are unavailable on HTTP",
            ));
        }
        let mut params = Vec::new();
        if !query.query_id.is_empty() {
            params.push(("query_id".to_owned(), query.query_id.as_ref().to_owned()));
        }
        params.push(("query".to_owned(), sql.to_owned()));
        params.push(("database".to_owned(), database.to_owned()));
        let mut saw_dedup = false;
        for (key, field) in query.settings() {
            let value = &field.value;
            if key.as_ref() == "insert_deduplication_token" {
                saw_dedup = true;
            }
            params.push((key.as_ref().to_owned(), value.as_ref().to_owned()));
        }
        if !saw_dedup && let Some(token) = query.deduplication_token_value().or(dedup) {
            params.push(("insert_deduplication_token".to_owned(), token.to_owned()));
        }
        if compression != Compression::None {
            params.push(("enable_http_compression".to_owned(), "1".to_owned()));
        }
        Ok(build_url(self.scheme, &self.host, self.port, &params))
    }

    fn headers(&self, encoding: Option<&str>, compression: Compression) -> Vec<String> {
        let mut headers = vec![
            format!("X-ClickHouse-User: {}", self.user),
            format!("X-ClickHouse-Key: {}", self.password),
            "Expect:".to_owned(),
        ];
        if let Some(encoding) = encoding {
            headers.push(format!("Content-Encoding: {encoding}"));
        }
        match compression {
            Compression::None => {}
            Compression::Lz4 => headers.push("Accept-Encoding: lz4".to_owned()),
            Compression::Zstd => headers.push("Accept-Encoding: zstd".to_owned()),
        }
        headers
    }

    fn configured_easy(&self, url: &str, headers: &[String], body: &[u8]) -> Result<Easy, Error> {
        let mut easy = Easy::new();
        easy.url(url).map_err(Error::curl)?;
        resolve_host(&mut easy, &self.host, self.port)?;
        easy.http_version(curl_version(self.version))
            .map_err(Error::curl)?;
        easy.tcp_nodelay(true).map_err(Error::curl)?;
        easy.pipewait(true).map_err(Error::curl)?;
        easy.forbid_reuse(false).map_err(Error::curl)?;
        easy.fresh_connect(false).map_err(Error::curl)?;
        easy.post(true).map_err(Error::curl)?;
        easy.post_fields_copy(body).map_err(Error::curl)?;
        easy.timeout(self.timeout).map_err(Error::curl)?;
        easy.connect_timeout(self.timeout.min(Duration::from_secs(10)))
            .map_err(Error::curl)?;
        let mut list = List::new();
        for header in headers {
            list.append(header).map_err(Error::curl)?;
        }
        easy.http_headers(list).map_err(Error::curl)?;

        Ok(easy)
    }

    fn transfer(
        &mut self,
        url: &str,
        headers: &[String],
        body: &[u8],
    ) -> Result<(Vec<u8>, ResponseHeaders), Error> {
        let mut easy = self.configured_easy(url, headers, body)?;

        self.negotiated = None;
        let payload = Rc::new(RefCell::new(Vec::new()));
        let metadata = Rc::new(RefCell::new(ResponseHeaders::default()));
        let status = Rc::new(RefCell::new(0_u32));
        let failure = Rc::new(RefCell::new(None));
        let overflow = Rc::new(std::cell::Cell::new(false));
        {
            let payload = Rc::clone(&payload);
            let overflow = Rc::clone(&overflow);
            easy.write_function(move |data| {
                let mut payload = payload.borrow_mut();
                if data.len() > MAX_RESPONSE_BYTES.saturating_sub(payload.len()) {
                    overflow.set(true);
                    return Ok(0);
                }
                payload.extend_from_slice(data);
                Ok(data.len())
            })
            .map_err(Error::curl)?;
        }
        {
            let metadata = Rc::clone(&metadata);
            let requested = self.version;
            easy.header_function(move |line| {
                metadata.borrow_mut().observe(line, requested);
                true
            })
            .map_err(Error::curl)?;
        }
        let status_done = Rc::clone(&status);
        let failure_done = Rc::clone(&failure);
        self.loop_
            .add(easy, move |easy, result| {
                match easy.response_code() {
                    Ok(code) => *status_done.borrow_mut() = code,
                    Err(error) => {
                        *failure_done.borrow_mut() = Some(format!("response status: {error}"))
                    }
                }
                if let Err(error) = result {
                    *failure_done.borrow_mut() = Some(format!("{error}"));
                }
            })
            .map_err(|error| Error::curl(error.error))?;
        self.loop_.run().map_err(Error::curl)?;
        if overflow.get() {
            return Err(Error::protocol("HTTP response exceeds 256 MiB limit"));
        }
        if let Some(error) = failure.borrow_mut().take() {
            return Err(Error::Curl(error));
        }
        let mut metadata = std::mem::take(&mut *metadata.borrow_mut());
        metadata.status = *status.borrow();
        self.negotiated = metadata.negotiated;
        Ok((std::mem::take(&mut *payload.borrow_mut()), metadata))
    }
}

fn curl_version(choice: HttpVersionChoice) -> HttpVersion {
    match choice {
        HttpVersionChoice::Http2PriorKnowledge => HttpVersion::V2PriorKnowledge,
        HttpVersionChoice::Http2Tls => HttpVersion::V2TLS,
        HttpVersionChoice::Http1 => HttpVersion::V11,
        HttpVersionChoice::Negotiate => HttpVersion::V2,
    }
}

fn resolve_host(easy: &mut Easy, host: &str, port: u16) -> Result<(), Error> {
    if host.parse::<IpAddr>().is_ok() {
        return Ok(());
    }
    let mut addrs = Vec::new();
    for address in (host, port).to_socket_addrs()? {
        let ip = address.ip();
        if !addrs.contains(&ip) {
            addrs.push(ip);
        }
    }
    if addrs.is_empty() {
        return Err(Error::protocol(format!("no addresses for {host}")));
    }
    easy.resolve_host(host, port, &addrs).map_err(Error::curl)
}

fn build_url(scheme: Scheme, host: &str, port: u16, params: &[(String, String)]) -> String {
    let scheme = match scheme {
        Scheme::Https => "https",
        _ => "http",
    };
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    let mut url = format!("{scheme}://{host}:{port}/?");
    for (index, (key, value)) in params.iter().enumerate() {
        if index > 0 {
            url.push('&');
        }
        url.push_str(&percent_encode(key));
        url.push('=');
        url.push_str(&percent_encode(value));
    }
    url
}

fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn compress_body(
    compression: Compression,
    body: &[u8],
) -> Result<(Cow<'_, [u8]>, Option<&'static str>), Error> {
    if body.is_empty() {
        return Ok((Cow::Borrowed(body), None));
    }
    match compression {
        Compression::None => Ok((Cow::Borrowed(body), None)),
        Compression::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(body)?;
            let compressed = encoder
                .finish()
                .map_err(|error| Error::protocol(format!("lz4 frame: {error}")))?;
            Ok((Cow::Owned(compressed), Some("lz4")))
        }
        Compression::Zstd => {
            let compressed = zstd::stream::encode_all(body, 0)?;
            Ok((Cow::Owned(compressed), Some("zstd")))
        }
    }
}

fn decompress_body(encoding: String, body: &[u8]) -> Result<Vec<u8>, Error> {
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(Error::protocol("HTTP response exceeds 256 MiB limit"));
    }
    if body.is_empty() || encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(body.to_vec());
    }
    if encoding.eq_ignore_ascii_case("lz4") {
        return read_bounded(lz4_flex::frame::FrameDecoder::new(body), MAX_RESPONSE_BYTES);
    }
    if encoding.eq_ignore_ascii_case("zstd") {
        return read_bounded(zstd::stream::read::Decoder::new(body)?, MAX_RESPONSE_BYTES);
    }
    Err(Error::protocol(format!(
        "unsupported HTTP content encoding: {encoding}"
    )))
}

/// Stop decompression before a small compressed frame can expand without bounds.
fn read_bounded(reader: impl Read, limit: usize) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    reader.take((limit + 1) as u64).read_to_end(&mut out)?;
    if out.len() > limit {
        return Err(Error::protocol(
            "decoded HTTP response exceeds 256 MiB limit",
        ));
    }
    Ok(out)
}

fn lossy_text(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut message = text.trim().to_owned();
    if message.len() > 4_096 {
        let mut end = 4_096;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

#[cfg(test)]
mod unit {
    use super::{compress_body, decompress_body, lossy_text};
    use crate::query::Compression;

    #[test]
    fn compressed_bodies_round_trip() {
        let body = b"clickhouse column values";
        for compression in [Compression::Lz4, Compression::Zstd] {
            let (encoded, name) = compress_body(compression, body).unwrap();
            let decoded = decompress_body(name.unwrap().to_owned(), &encoded).unwrap();
            assert_eq!(decoded, body);
        }
        assert!(decompress_body("gzip".to_owned(), body).is_err());
        assert_eq!(decompress_body("identity".to_owned(), body).unwrap(), body);
    }

    #[test]
    fn error_truncation_preserves_utf8() {
        let body = "中".repeat(2_000);
        let diagnostic = lossy_text(body.as_bytes());
        assert!(diagnostic.len() <= 4_096);
        assert!(!diagnostic.is_empty());
        assert!(body.starts_with(&diagnostic));
    }

    /// Bounded decoding rejects expansion beyond its limit, including exact EOF edges.
    #[test]
    fn decoded_body_limit_rejects_expansion() {
        let body = vec![b'x'; 4096];
        let encoded = zstd::stream::encode_all(body.as_slice(), 0).unwrap();
        let decoder = zstd::stream::read::Decoder::new(encoded.as_slice()).unwrap();
        assert!(super::read_bounded(decoder, 4095).is_err());
        let decoder = zstd::stream::read::Decoder::new(encoded.as_slice()).unwrap();
        assert_eq!(super::read_bounded(decoder, 4096).unwrap(), body);
    }

    /// A positive fractional-millisecond timeout cannot become an unlimited deadline.
    #[test]
    fn positive_submillisecond_deadline_rounds_up() {
        let mut transport = super::HttpTransport::open(
            crate::endpoint::Scheme::Http,
            "127.0.0.1",
            1,
            "default",
            "",
            crate::HttpVersionChoice::Http1,
        )
        .unwrap();
        transport.set_timeout(std::time::Duration::from_micros(500));
        assert_eq!(transport.timeout, std::time::Duration::from_millis(1));
    }

    #[test]
    fn interim_headers_reset_exception_and_encoding() {
        let mut headers = super::ResponseHeaders::default();
        headers.observe(
            b"HTTP/1.1 100 Continue\r\n",
            crate::HttpVersionChoice::Http1,
        );
        headers.observe(
            b"X-ClickHouse-Exception-Code: 42\r\n",
            crate::HttpVersionChoice::Http1,
        );
        headers.observe(
            b"Content-Encoding: zstd\r\n",
            crate::HttpVersionChoice::Http1,
        );
        headers.observe(b"HTTP/1.1 200 OK\r\n", crate::HttpVersionChoice::Http1);
        assert!(headers.check(b"healthy").is_ok());
        assert!(headers.encoding.is_empty());
    }
}
