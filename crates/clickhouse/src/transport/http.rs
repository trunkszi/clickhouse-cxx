//! Synchronous HTTP requests driven by a caller-owned Nexora proactor.

use std::{cell::Cell, marker::PhantomData, net::IpAddr, rc::Rc, time::Duration};

use nexora::{
    AlpnProtocol, ClientConfig, HttpClient, Method, Proactor, Request, Response, TlsClientConfig,
    Uri, Version,
};

use crate::{
    endpoint::Scheme,
    error::Error,
    query::{Compression, HttpVersionChoice, Query},
};

mod body;
mod pending;
use body::{MAX_RESPONSE_BYTES, ResponseHeaders, compress_body};
pub use pending::HttpRequest;

/// HTTP options the transport applies to every call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpPreferences {
    /// Requested HTTP protocol selection.
    pub version: HttpVersionChoice,
    /// `TCP_NODELAY` is enabled by the client.
    pub tcp_nodelay: bool,
    /// Idle connections may be reused.
    pub keep_alive: bool,
    /// HTTP/2 connections support multiplexing.
    pub multiplex: bool,
}

/// Owns all request and completion state on the constructing thread.
pub(crate) struct HttpTransport {
    scheme: Scheme,
    host: String,
    port: u16,
    user: String,
    password: String,
    version: HttpVersionChoice,
    timeout: Duration,
    // Drop the client before the ring that owns its registered operations.
    client: HttpClient,
    proactor: Proactor,
    reconfigure: bool,
    negotiated: Option<HttpVersionChoice>,
    observed: Rc<Cell<Option<HttpVersionChoice>>>,
    request_owner: Rc<()>,
    owner_thread: PhantomData<Rc<()>>,
}

impl HttpTransport {
    /// Creates a thread-local driver without connecting to the endpoint.
    ///
    /// # Errors
    /// Returns an invalid credential, protocol, resolver, or proactor error.
    pub(crate) fn open(
        scheme: Scheme,
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        version: HttpVersionChoice,
    ) -> Result<Self, Error> {
        validate_version(scheme, version)?;
        if user
            .bytes()
            .chain(password.bytes())
            .any(|byte| byte < 32 || byte == 127)
        {
            return Err(Error::protocol(
                "HTTP credentials contain prohibited control characters",
            ));
        }
        let timeout = Duration::from_secs(60);
        let (client, proactor) = driver(version, timeout)?;
        Ok(Self {
            scheme,
            host: host.to_owned(),
            port,
            user: user.to_owned(),
            password: password.to_owned(),
            version,
            timeout,
            client,
            proactor,
            reconfigure: false,
            negotiated: None,
            observed: Rc::new(Cell::new(None)),
            request_owner: Rc::new(()),
            owner_thread: PhantomData,
        })
    }

    /// Applies deadlines to subsequent requests, preserving positive fractions.
    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = if timeout.is_zero() {
            timeout
        } else {
            Duration::from_millis(timeout.as_millis().max(1).min(u128::from(u64::MAX)) as u64)
        };
        self.reconfigure = true;
    }

    pub(crate) fn preferences(&self) -> HttpPreferences {
        HttpPreferences {
            version: self.version,
            tcp_nodelay: true,
            keep_alive: true,
            multiplex: true,
        }
    }

    pub(crate) fn set_version(&mut self, version: HttpVersionChoice) -> Result<(), Error> {
        validate_version(self.scheme, version)?;
        if self.version != version {
            if self.client.active_request_count() != 0 {
                return Err(Error::Config {
                    reason: "HTTP protocol cannot change while requests are pending".into(),
                });
            }
            self.version = version;
            self.reconfigure = true;
        }
        Ok(())
    }

    pub(crate) fn negotiated_version(&self) -> Option<HttpVersionChoice> {
        self.negotiated
    }

    /// Posts SQL and validates the decoded server response.
    ///
    /// # Errors
    /// Returns transport, HTTP status, server exception, or decoding failures.
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
        let response = self.transfer(&url, headers, payload.as_ref())?;
        let metadata = ResponseHeaders::from_response(&response, self.version);
        metadata.decode_body(response.body.as_ref())
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

    fn headers(&self, encoding: Option<&str>, compression: Compression) -> nexora::Headers {
        let mut headers = nexora::Headers::new();
        headers.insert("X-ClickHouse-User", &self.user);
        headers.insert("X-ClickHouse-Key", &self.password);
        if let Some(encoding) = encoding {
            headers.insert("Content-Encoding", encoding);
        }
        match compression {
            Compression::None => {}
            Compression::Lz4 => headers.insert("Accept-Encoding", "lz4"),
            Compression::Zstd => headers.insert("Accept-Encoding", "zstd"),
        }
        headers
    }

    fn transfer(
        &mut self,
        url: &str,
        headers: nexora::Headers,
        body: &[u8],
    ) -> Result<Response, Error> {
        if self.reconfigure {
            self.reset()?;
        }
        let mut request = self.start_request(url, headers, body)?;
        loop {
            let completed = request.take_response();
            if let Some(response) = completed {
                let response = response?;
                let metadata = ResponseHeaders::from_response(&response, self.version);
                self.negotiated = metadata.negotiated;
                if self.version == HttpVersionChoice::Http2Tls && response.version != Version::Http2
                {
                    return Err(Error::protocol(
                        "HTTP/2 over TLS was required but not negotiated",
                    ));
                }
                return Ok(response);
            }
            if let Err(error) = self.poll() {
                self.cancel(&request)?;
                self.reconfigure = true;
                return Err(error);
            }
        }
    }
}

impl Drop for HttpTransport {
    fn drop(&mut self) {
        self.client.shutdown(&mut self.proactor);
    }
}

fn validate_version(scheme: Scheme, version: HttpVersionChoice) -> Result<(), Error> {
    if version == HttpVersionChoice::Http2Tls && scheme != Scheme::Https {
        return Err(Error::protocol(
            "HTTP/2 over TLS requires an https:// endpoint",
        ));
    }
    Ok(())
}

fn tls_alpn(version: HttpVersionChoice) -> &'static [AlpnProtocol] {
    match version {
        HttpVersionChoice::Http1 => &[AlpnProtocol::Http11],
        HttpVersionChoice::Http2Tls => &[AlpnProtocol::Http2],
        _ => &[AlpnProtocol::Http2, AlpnProtocol::Http11],
    }
}

fn client_config(version: HttpVersionChoice, timeout: Duration) -> ClientConfig {
    let timeout = if timeout.is_zero() {
        Duration::from_secs(i64::MAX as u64)
    } else {
        timeout
    };
    ClientConfig {
        prefer_http2: version != HttpVersionChoice::Http1,
        http2_prior_knowledge: version == HttpVersionChoice::Http2PriorKnowledge,
        tls_config: Some(TlsClientConfig::default().with_alpn(tls_alpn(version))),
        request_timeout: timeout,
        connect_timeout: timeout.min(Duration::from_secs(10)),
        max_body_size: MAX_RESPONSE_BYTES,
        max_header_list_size: 64 * 1024,
        follow_redirects: false,
        ..ClientConfig::default()
    }
}

fn driver(version: HttpVersionChoice, timeout: Duration) -> Result<(HttpClient, Proactor), Error> {
    let mut proactor = Proactor::builder().build().map_err(Error::http_transport)?;
    let client = HttpClient::new(&mut proactor, client_config(version, timeout))?;
    Ok((client, proactor))
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

#[cfg(test)]
mod tests {
    use super::*;

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

    fn configured_transport(
        scheme: Scheme,
        host: &str,
        port: u16,
        version: HttpVersionChoice,
        config: ClientConfig,
    ) -> HttpTransport {
        let timeout = config.request_timeout;
        let mut proactor = Proactor::builder().build().unwrap();
        let mut client = HttpClient::new(&mut proactor, config).unwrap();
        if host == "localhost" {
            client.resolve_host(host, port, vec![std::net::Ipv4Addr::LOCALHOST.into()]);
        }
        HttpTransport {
            scheme,
            host: host.to_owned(),
            port,
            user: "alice".to_owned(),
            password: "secret".to_owned(),
            version,
            timeout,
            client,
            proactor,
            reconfigure: false,
            negotiated: None,
            observed: Rc::new(std::cell::Cell::new(None)),
            request_owner: Rc::new(()),
            owner_thread: PhantomData,
        }
    }

    mod integration_dns_scenarios {
        //! Integration deadline and NXDOMAIN scenarios against a controlled DNS peer.
        use std::{
            net::UdpSocket,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            thread,
            time::Instant,
        };

        use super::*;

        fn failed_lookup(reply: bool) {
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            socket.set_nonblocking(true).unwrap();
            let address = socket.local_addr().unwrap();
            let done = Arc::new(AtomicBool::new(false));
            let stop = Arc::clone(&done);
            let server = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut queries = 0;
                loop {
                    if stop.load(Ordering::Acquire) {
                        return queries;
                    }
                    assert!(Instant::now() < deadline, "DNS fixture deadline exceeded");
                    let mut packet = [0_u8; 512];
                    match socket.recv_from(&mut packet) {
                        Ok((size, peer)) => {
                            queries += 1;
                            assert!(size >= 17);
                            if reply {
                                let mut end = 12;
                                while packet[end] != 0 {
                                    end += 1 + usize::from(packet[end]);
                                }
                                end += 5;
                                assert!(end <= size);
                                packet[2..12]
                                    .copy_from_slice(&[0x81, 0x83, 0, 1, 0, 0, 0, 0, 0, 0]);
                                socket.send_to(&packet[..end], peer).unwrap();
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::yield_now()
                        }
                        Err(error) => panic!("DNS peer: {error}"),
                    }
                }
            });
            let version = HttpVersionChoice::Http1;
            let mut config = client_config(version, Duration::from_millis(150));
            config.dns_servers_csv = Some(address.to_string());
            let mut transport = super::configured_transport(
                Scheme::Http,
                "missing.unit.test",
                8123,
                version,
                config,
            );
            let start = Instant::now();
            let error = transport
                .execute(
                    "default",
                    &Query::new("SELECT 1"),
                    "SELECT 1",
                    &[],
                    Compression::None,
                    None,
                )
                .unwrap_err();
            assert!(start.elapsed() < Duration::from_secs(2));
            match error {
                Error::HttpTransport(message) if reply => {
                    assert!(message.contains("DNS resolution failed"), "{message}");
                    assert!(message.contains("missing.unit.test"), "{message}");
                }
                Error::HttpTransport(message) => {
                    assert!(message.contains("timed out"), "{message}")
                }
                error => panic!("incorrect error category: {error}"),
            }
            done.store(true, Ordering::Release);
            assert!(server.join().unwrap() >= 1);
        }

        #[test]
        fn nxdomain_maps_to_http_transport_error() {
            failed_lookup(true);
        }

        #[test]
        fn stalled_dns_consumes_the_request_deadline() {
            failed_lookup(false);
        }
    }

    mod integration_tls_scenarios {
        //! Integration scenarios for the private transport trust seam over real TLS sockets.
        //! Test-only roots trust the owned localhost peer; production uses public roots.
        use std::{
            io::{Read, Write},
            net::{TcpListener, TcpStream},
            thread,
        };

        use nexora::{
            AlpnProtocol, BytesMut, TlsClientConfig, TlsServerConfig,
            proto::{Http2Connection, Http2Event, TlsSession},
        };

        use super::*;

        fn flush(session: &mut TlsSession, stream: &mut TcpStream) {
            let mut ciphertext = BytesMut::new();
            session.take_ciphertext(&mut ciphertext).unwrap();
            stream.write_all(ciphertext.as_ref()).unwrap();
        }

        fn tls_peer(listener: TcpListener, config: TlsServerConfig) -> (usize, bool) {
            listener.set_nonblocking(true).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(std::time::Instant::now() < deadline);
                        thread::yield_now();
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut session = TlsSession::new_server(&config).unwrap();
            let mut http2 = Http2Connection::server_default();
            let mut plaintext = BytesMut::new();
            let mut responded = false;
            loop {
                flush(&mut session, &mut stream);
                if responded {
                    return (1, false);
                }
                let mut buffer = [0_u8; 4096];
                let count = stream.read(&mut buffer).unwrap();
                if count == 0 {
                    assert!(plaintext.is_empty());
                    return (0, false);
                }
                if let Err(error) = session.feed_ciphertext(&buffer[..count]) {
                    let reason = error.to_string();
                    assert!(plaintext.is_empty());
                    return (
                        0,
                        reason.contains("application protocol")
                            || reason.contains("doesn't support any known protocol"),
                    );
                }
                session.read_plaintext(&mut plaintext).unwrap();
                if session.is_handshaking() {
                    continue;
                }
                if session.alpn_protocol() == Some(AlpnProtocol::Http2) {
                    http2.feed_bytes(&mut plaintext).unwrap();
                    while let Some(event) = http2.poll_event() {
                        if let Http2Event::RequestReceived { stream_id, request } = event {
                            assert_eq!(request.method, Method::Post);
                            http2
                                .submit_response(stream_id, &Response::ok("ok"))
                                .unwrap();
                            responded = true;
                        }
                    }
                    let mut output = BytesMut::new();
                    http2.take_tx_bytes(&mut output);
                    if !output.is_empty() {
                        assert_eq!(
                            session.write_plaintext(output.as_ref()).unwrap(),
                            output.len()
                        );
                    }
                } else if plaintext
                    .as_ref()
                    .windows(4)
                    .any(|bytes| bytes == b"\r\n\r\n")
                {
                    assert!(plaintext.as_ref().starts_with(b"POST "));
                    session
                        .write_plaintext(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .unwrap();
                    responded = true;
                }
                if responded {
                    session.send_close_notify();
                }
            }
        }

        fn transport(port: u16, roots: Vec<u8>, version: HttpVersionChoice) -> HttpTransport {
            let mut config = client_config(version, Duration::from_secs(5));
            config.tls_config =
                Some(TlsClientConfig::with_root_certificates(&[roots], tls_alpn(version)).unwrap());
            super::configured_transport(Scheme::Https, "localhost", port, version, config)
        }

        #[test]
        fn tls_protocol_selection_enforces_explicit_requirement_and_fallback() {
            for (version, alpn, expected) in [
                (
                    HttpVersionChoice::Http1,
                    vec![AlpnProtocol::Http2, AlpnProtocol::Http11],
                    Some(HttpVersionChoice::Http1),
                ),
                (
                    HttpVersionChoice::Http2Tls,
                    vec![AlpnProtocol::Http2, AlpnProtocol::Http11],
                    Some(HttpVersionChoice::Http2Tls),
                ),
                (
                    HttpVersionChoice::Http2Tls,
                    vec![AlpnProtocol::Http11],
                    None,
                ),
                (HttpVersionChoice::Http2Tls, vec![], None),
                (
                    HttpVersionChoice::Negotiate,
                    vec![AlpnProtocol::Http11],
                    Some(HttpVersionChoice::Http1),
                ),
                (
                    HttpVersionChoice::Negotiate,
                    vec![AlpnProtocol::Http2, AlpnProtocol::Http11],
                    Some(HttpVersionChoice::Http2Tls),
                ),
            ] {
                let (config, certificate) =
                    TlsServerConfig::self_signed_for_localhost(&alpn).unwrap();
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                let server = thread::spawn(move || tls_peer(listener, config));
                let mut transport = transport(port, certificate, version);
                let result = transport.execute(
                    "default",
                    &Query::new("SELECT 1"),
                    "SELECT 1",
                    &[],
                    Compression::None,
                    None,
                );
                let negotiated = transport.negotiated_version();
                drop(transport);
                let outcome = server.join().unwrap();
                if let Some(expected) = expected {
                    assert_eq!(result.unwrap(), b"ok");
                    assert_eq!(negotiated, Some(expected));
                    assert_eq!(outcome, (1, false));
                } else {
                    assert_eq!(
                        outcome.0, 0,
                        "a rejected protocol executed POST: {result:?}"
                    );
                    assert!(matches!(result, Err(Error::HttpTransport(_))));
                }
            }
        }

        /// The public client offers only h2; an HTTP/1.1 peer rejects before SQL arrives.
        #[test]
        fn explicit_http2_tls_rejects_before_post_on_public_client() {
            let (config, _) =
                TlsServerConfig::self_signed_for_localhost(&[AlpnProtocol::Http11]).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = thread::spawn(move || tls_peer(listener, config));
            let endpoint = crate::Endpoint::parse(format!("https://127.0.0.1:{port}")).unwrap();
            let mut client =
                crate::Client::connect(endpoint, "alice", "secret", "default").unwrap();
            client
                .set_http_version(HttpVersionChoice::Http2Tls)
                .unwrap();
            let result = client.insert_formatted(
                &Query::new("INSERT INTO t FORMAT RowBinary"),
                b"must not execute",
            );
            assert!(matches!(result, Err(Error::HttpTransport(_))));
            assert_eq!(server.join().unwrap(), (0, true));
        }

        /// A reset synchronous stream cannot terminate an unrelated pending async stream.
        #[test]
        fn reset_stream_preserves_an_async_sibling_on_the_same_tls_connection() {
            use std::sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            };
            let (config, certificate) =
                TlsServerConfig::self_signed_for_localhost(&[AlpnProtocol::Http2]).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let release = Arc::new(AtomicBool::new(false));
            let gate = Arc::clone(&release);
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut tls = TlsSession::new_server(&config).unwrap();
                let mut connection = Http2Connection::server_default();
                let mut plain = BytesMut::new();
                let mut held = None;
                let mut reset = false;
                loop {
                    flush(&mut tls, &mut socket);
                    if reset && let Some(stream_id) = held {
                        let deadline = std::time::Instant::now() + Duration::from_secs(5);
                        while !gate.load(Ordering::Acquire) {
                            assert!(std::time::Instant::now() < deadline);
                            thread::yield_now();
                        }
                        connection
                            .submit_response(stream_id, &Response::ok("sibling survives"))
                            .unwrap();
                        let mut output = BytesMut::new();
                        connection.take_tx_bytes(&mut output);
                        tls.write_plaintext(output.as_ref()).unwrap();
                        tls.send_close_notify();
                        flush(&mut tls, &mut socket);
                        return;
                    }
                    let mut input = [0; 4096];
                    let size = socket.read(&mut input).unwrap();
                    assert!(size > 0);
                    tls.feed_ciphertext(&input[..size]).unwrap();
                    tls.read_plaintext(&mut plain).unwrap();
                    if tls.is_handshaking() {
                        continue;
                    }
                    connection.feed_bytes(&mut plain).unwrap();
                    while let Some(event) = connection.poll_event() {
                        if let Http2Event::RequestReceived { stream_id, request } = event {
                            if request.uri.path_and_query().contains("reset_value") {
                                connection.cancel_stream(stream_id).unwrap();
                                reset = true;
                            } else {
                                held = Some(stream_id);
                            }
                        }
                    }
                    let mut output = BytesMut::new();
                    connection.take_tx_bytes(&mut output);
                    if !output.is_empty() {
                        tls.write_plaintext(output.as_ref()).unwrap();
                    }
                }
            });
            let mut transport = transport(port, certificate, HttpVersionChoice::Http2Tls);
            let mut pending = transport
                .submit_raw(
                    "default",
                    &Query::new("SELECT keep_value"),
                    &[],
                    Compression::None,
                    None,
                )
                .unwrap();
            let result = transport.execute(
                "default",
                &Query::new("SELECT reset_value"),
                "SELECT reset_value",
                &[],
                Compression::None,
                None,
            );
            let sibling_pending = !pending.is_complete();
            release.store(true, Ordering::Release);
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !pending.is_complete() {
                assert!(std::time::Instant::now() < deadline);
                transport.poll().unwrap();
            }
            assert!(matches!(result, Err(Error::HttpTransport(_))));
            assert!(sibling_pending, "one stream error terminated its sibling");
            assert_eq!(pending.try_take().unwrap().unwrap(), b"sibling survives");
            assert!(pending.try_take().is_none());
            server.join().unwrap();
        }
    }
}
