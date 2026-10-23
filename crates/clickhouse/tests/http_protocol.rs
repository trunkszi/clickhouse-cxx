#![cfg(feature = "http")]
//! Integration lifecycle and scenario tests through the public HTTP client.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

use clickhouse::{Client, ClientConfig, Compression, Endpoint, Error, HttpVersionChoice, Query};
use nexora::{
    BytesMut, Response,
    proto::{Http2Connection, Http2Event},
};

fn client(port: u16) -> Client {
    Client::connect(
        Endpoint::parse(format!("http://127.0.0.1:{port}")).unwrap(),
        "alice",
        "secret",
        "default",
    )
    .unwrap()
}

fn accept(listener: &TcpListener) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return stream;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "HTTP peer did not connect");
                thread::yield_now();
            }
            Err(error) => panic!("accept failed: {error}"),
        }
    }
}

fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let end = loop {
        let mut buffer = [0_u8; 4096];
        let count = stream.read(&mut buffer).unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec())
        .unwrap()
        .to_ascii_lowercase();
    let length = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .unwrap()
        .trim()
        .parse::<usize>()
        .unwrap();
    while bytes.len() - end < length {
        let mut buffer = [0_u8; 4096];
        let count = stream.read(&mut buffer).unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    (headers, bytes[end..].to_vec())
}

#[test]
fn http2_prior_knowledge_preserves_headers_and_connection_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let mut socket = accept(&listener);
        let mut connection = Http2Connection::server_default();
        let mut incoming = BytesMut::new();
        let mut outgoing = BytesMut::new();
        let mut requests = 0;
        loop {
            connection.take_tx_bytes(&mut outgoing);
            socket.write_all(outgoing.as_ref()).unwrap();
            outgoing.clear();
            if requests == 2 {
                break;
            }
            let mut buffer = [0_u8; 4096];
            let count = socket.read(&mut buffer).unwrap();
            assert!(count > 0);
            incoming.extend_from_slice(&buffer[..count]);
            connection.feed_bytes(&mut incoming).unwrap();
            while let Some(event) = connection.poll_event() {
                if let Http2Event::RequestReceived { stream_id, request } = event {
                    assert_eq!(request.method, nexora::Method::Post);
                    assert_eq!(request.headers.get("content-length"), Some("0"));
                    assert_eq!(request.headers.get("x-clickhouse-user"), Some("alice"));
                    assert_eq!(request.headers.get("x-clickhouse-key"), Some("secret"));
                    assert!(request.uri.path_and_query().contains("database=default"));
                    connection
                        .submit_response(stream_id, &Response::ok("result"))
                        .unwrap();
                    requests += 1;
                }
            }
        }
    });
    let mut client = client(port);
    client
        .set_http_version(HttpVersionChoice::Http2PriorKnowledge)
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            client.fetch_bytes(&Query::new("SELECT payload")).unwrap(),
            b"result"
        );
        assert_eq!(
            client.negotiated_http_version(),
            Some(HttpVersionChoice::Http2PriorKnowledge)
        );
    }
    server.join().unwrap();
}

#[test]
fn explicit_tls_protocol_rejects_cleartext_without_changing_selection() {
    let mut client = client(1);
    assert!(matches!(
        client.set_http_version(HttpVersionChoice::Http2Tls),
        Err(Error::Protocol(_))
    ));
    assert_eq!(
        client.http_preferences().unwrap().version,
        HttpVersionChoice::Negotiate
    );
}

#[test]
fn compressed_requests_and_borrowed_decoded_responses_round_trip() {
    for compression in [Compression::Lz4, Compression::Zstd] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let payload = vec![b'z'; 48_000];
        let expected = payload.clone();
        let server = thread::spawn(move || {
            for index in 0..2 {
                let mut stream = accept(&listener);
                let (headers, body) = read_request(&mut stream);
                let (name, encoded) = match compression {
                    Compression::Lz4 => {
                        if index == 0 {
                            let mut decoded = Vec::new();
                            lz4_flex::frame::FrameDecoder::new(body.as_slice())
                                .read_to_end(&mut decoded)
                                .unwrap();
                            assert_eq!(decoded, b"input rows");
                        }
                        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                        encoder.write_all(&payload).unwrap();
                        ("lz4", encoder.finish().unwrap())
                    }
                    Compression::Zstd => {
                        if index == 0 {
                            assert_eq!(
                                zstd::stream::decode_all(body.as_slice()).unwrap(),
                                b"input rows"
                            );
                        }
                        (
                            "zstd",
                            zstd::stream::encode_all(payload.as_slice(), 0).unwrap(),
                        )
                    }
                    _ => unreachable!(),
                };
                assert!(headers.contains(&format!("accept-encoding: {name}")));
                if index == 0 {
                    assert!(headers.contains(&format!("content-encoding: {name}")));
                }
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nCoNtEnT-EnCoDiNg: {name}\r\nContent-Length: \
                     {}\r\nConnection: close\r\n\r\n",
                    encoded.len()
                )
                .unwrap();
                stream.write_all(&encoded).unwrap();
            }
        });
        let mut client = client(port);
        client.set_compression(compression).unwrap();
        client
            .insert_formatted(&Query::new("INSERT INTO t FORMAT RowBinary"), b"input rows")
            .unwrap();
        let mut decoded = Vec::new();
        let count = client
            .query_raw_with(&Query::new("SELECT payload"), |chunk| {
                decoded.extend_from_slice(chunk);
                Ok(())
            })
            .unwrap();
        assert_eq!(count, expected.len());
        assert_eq!(decoded, expected);
        server.join().unwrap();
    }
}

#[test]
fn response_deadline_surfaces_transport_error_and_client_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wait = std::sync::Arc::clone(&finished);
    let server = thread::spawn(move || {
        let mut first = accept(&listener);
        read_request(&mut first);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !wait.load(std::sync::atomic::Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        drop(first);
        let mut second = accept(&listener);
        read_request(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
    });
    let config = ClientConfig::parse(&format!("http://127.0.0.1:{port}"))
        .unwrap()
        .with_timeout(Duration::from_millis(150));
    let mut client = Client::new(config).unwrap();
    let start = Instant::now();
    assert!(matches!(
        client.fetch_bytes(&Query::new("SELECT slow")),
        Err(Error::HttpTransport(_))
    ));
    assert!(start.elapsed() < Duration::from_secs(5));
    finished.store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(
        client.fetch_bytes(&Query::new("SELECT healthy")).unwrap(),
        b"ok"
    );
    server.join().unwrap();
}

/// Redirects retain their status instead of replaying credentials to another origin.
#[test]
fn redirect_status_is_returned_without_following_location() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let mut stream = accept(&listener);
        let (headers, _) = read_request(&mut stream);
        assert!(headers.contains("x-clickhouse-key: secret"));
        stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/leak\r\nContent-Length: 6\r\nConnection: close\r\n\r\nclosed").unwrap();
    });
    let error = client(port)
        .fetch_bytes(&Query::new("SELECT 1"))
        .unwrap_err();
    assert!(matches!(error, Error::Http { status: 302, message } if message == "closed"));
    server.join().unwrap();
}

/// Failed diagnostic decoding cannot replace the original status or exception category.
#[test]
fn malformed_error_bodies_preserve_http_diagnostics() {
    for (status, exception) in [(500, false), (200, true)] {
        for encoding in ["gzip", "lz4", "zstd"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = thread::spawn(move || {
                for _ in 0..2 {
                    let mut stream = accept(&listener);
                    read_request(&mut stream);
                    let exception = if exception {
                        "X-ClickHouse-Exception-Code: 241\r\n"
                    } else {
                        ""
                    };
                    let body = b"invalid encoded diagnostic";
                    write!(
                        stream,
                        "HTTP/1.1 {status} Error\r\n{exception}Content-Encoding: \
                         {encoding}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                    stream.write_all(body).unwrap();
                }
            });
            let mut client = client(port);
            let buffered = client.fetch_bytes(&Query::new("SELECT bad")).unwrap_err();
            let mut delivered = false;
            let callback = client
                .query_raw_with(&Query::new("SELECT bad"), |_| {
                    delivered = true;
                    Ok(())
                })
                .unwrap_err();
            assert!(!delivered);
            for error in [buffered, callback] {
                match error {
                    Error::Http {
                        status: actual,
                        message,
                    } => {
                        assert_eq!(actual, status);
                        assert!(
                            message.contains("response body decoding failed"),
                            "{message}"
                        );
                        if exception {
                            assert!(message.contains("241"), "{message}");
                        }
                    }
                    error => panic!("lost HTTP status: {error}"),
                }
            }
            server.join().unwrap();
        }
    }
}

/// Public hostname requests retain their HTTP error category within the configured budget.
#[test]
fn public_hostname_failure_uses_transport_error_and_deadline() {
    let config = ClientConfig::parse("http://missing-http-transport.invalid:8123")
        .unwrap()
        .with_timeout(Duration::from_millis(150));
    let mut client = Client::new(config).unwrap();
    let start = Instant::now();
    let error = client.fetch_bytes(&Query::new("SELECT 1")).unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(2));
    match error {
        Error::HttpTransport(message) => {
            assert!(
                message.contains("DNS") || message.contains("timed out"),
                "{message}"
            );
        }
        error => panic!("hostname resolution escaped HTTP error mapping: {error}"),
    }
}

/// Integration: worker configuration reaches a real synchronous HTTP peer.
#[cfg(feature = "http")]
#[test]
fn integration_http_worker_query() {
    use std::{
        io::{Read, Write},
        time::Duration,
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
    let address = listener.local_addr().expect("listener address");
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("HTTP connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bounded read");
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            stream.read_exact(&mut byte).expect("request header");
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let headers = String::from_utf8(request).expect("HTTP text");
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("x-clickhouse-user: worker-user")
        );
        assert!(headers.contains("database=worker-db"));
        let body_len = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let mut body = vec![0; body_len];
        stream.read_exact(&mut body).expect("HTTP request body");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .expect("HTTP response");
    });
    let config = ClientConfig::parse(&format!("http://{address}"))
        .expect("HTTP configuration")
        .with_user("worker-user")
        .with_database("worker-db")
        .with_timeout(Duration::from_secs(5));
    let worker = Client::connect_workers(config).expect("worker factory");
    worker
        .with(|client| {
            client.set_http_version(clickhouse::HttpVersionChoice::Http1)?;
            client.execute("SELECT 1")
        })
        .expect("HTTP worker execution");
    peer.join().expect("HTTP peer completed");
}
