#![cfg(feature = "http")]
//! Integration scenario verifying wire headers and Native bodies against a controlled peer.

use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

use clickhouse::{
    Block, Client, Endpoint, Error, HTTP_NATIVE_REVISION, HttpVersionChoice, Query, encode_block,
};

#[test]
fn http_insert_sends_native_body_headers_and_dedup_token() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        // The client pools connections but this peer closes after each response,
        // so INSERT and the following SELECT each open their own TCP connection.
        handle_insert(&listener);
        handle_query(&listener);
    });

    let endpoint = Endpoint::parse(format!("http://127.0.0.1:{port}")).unwrap();
    let mut client = Client::connect(endpoint, "ada", "secret", "app").unwrap();
    client.set_http_version(HttpVersionChoice::Http1).unwrap();
    client.set_insert_deduplication_token("batch-1").unwrap();
    clickhouse::row::RowClient::insert_block(&mut client, "events", &sample_block()).unwrap();
    let blocks =
        clickhouse::row::RowClient::query_blocks(&mut client, "SELECT id FROM events").unwrap();
    assert_eq!(blocks[0].column_slice_by_name::<u64>("id").unwrap(), &[4]);
    server.join().unwrap();
}

fn handle_insert(listener: &TcpListener) {
    let mut stream = accept_one(listener);
    let (header, body) = read_http(&mut stream);
    assert!(
        header
            .to_ascii_lowercase()
            .contains("x-clickhouse-user: ada")
    );
    assert!(
        header
            .to_ascii_lowercase()
            .contains("x-clickhouse-key: secret")
    );
    assert!(header.contains("insert_deduplication_token=batch-1"));
    assert!(header.contains("FORMAT%20Native"));
    let mut expected = Vec::new();
    encode_block(&sample_block(), HTTP_NATIVE_REVISION, &mut expected).unwrap();
    assert_eq!(body, expected);
    write_response(&mut stream, b"");
}

fn handle_query(listener: &TcpListener) {
    let mut stream = accept_one(listener);
    let (header, _body) = read_http(&mut stream);
    assert!(header.contains("FORMAT%20Native"));
    assert!(header.contains("SELECT%20id%20FROM%20events"));
    write_response(&mut stream, &sample_response());
}

fn accept_one(listener: &TcpListener) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let (stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(err)
                if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::Interrupted =>
            {
                if Instant::now() >= deadline {
                    panic!("timed out waiting for HTTP connection");
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("accept failed: {err}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
}

fn write_response(stream: &mut TcpStream, body: &[u8]) {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    stream.flush().unwrap();
}

fn sample_block() -> Block {
    let mut block = Block::new();
    block.append_fixed_slice("id", "UInt64", &[4_u64]).unwrap();
    block
}

fn sample_response() -> Vec<u8> {
    let mut block = Block::new();
    block.append_fixed_slice("id", "UInt64", &[4_u64]).unwrap();
    let mut bytes = Vec::new();
    encode_block(&block, HTTP_NATIVE_REVISION, &mut bytes).unwrap();
    bytes
}

fn read_http(stream: &mut impl Read) -> (String, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 1024];
    let header_end = loop {
        let n = stream.read(&mut tmp).unwrap();
        assert!(n > 0, "eof before headers");
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let header = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let length = content_length(&header);
    while buf.len() < header_end + length {
        let n = stream.read(&mut tmp).unwrap();
        assert!(n > 0, "eof before body");
        buf.extend_from_slice(&tmp[..n]);
    }
    let body = buf[header_end..header_end + length].to_vec();
    (header, body)
}

fn content_length(header: &str) -> usize {
    header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}

/// Credentials cannot inject additional HTTP request headers.
#[test]
fn auth_header_injection_is_rejected() {
    let endpoint = Endpoint::parse("http://127.0.0.1:1").unwrap();
    assert!(Client::connect(endpoint.clone(), "bad\r\nX-Injected: yes", "", "default").is_err());
    assert!(Client::connect(endpoint, "default", "bad\0secret", "default").is_err());
}

/// A chunked wire body is collected before borrowed decoded chunks are delivered.
#[test]
fn response_callbacks_deliver_collected_chunks() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let expected = vec![b'x'; 40_000];
    let sent = expected.clone();
    let server = thread::spawn(move || {
        let mut stream = accept_one(&listener);
        read_http(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        for chunk in sent.chunks(10_000) {
            write!(stream, "{:X}\r\n", chunk.len()).unwrap();
            stream.write_all(chunk).unwrap();
            stream.write_all(b"\r\n").unwrap();
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
        stream.flush().unwrap();
    });
    let mut client = mock_client(port);
    let mut data = Vec::new();
    let mut callbacks = 0;
    let bytes = client
        .query_raw_with(&Query::new("SELECT payload"), |chunk| {
            callbacks += 1;
            data.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap();
    assert_eq!(bytes, expected.len());
    assert_eq!(data, expected);
    assert!(callbacks >= 3);
    server.join().unwrap();
}

/// A caller failure propagates unchanged and the next request remains usable.
#[test]
fn callback_failure_is_not_a_partial_success() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        for body in [b"blocked".as_slice(), b"healthy".as_slice()] {
            let mut stream = accept_one(&listener);
            read_http(&mut stream);
            write_response(&mut stream, body);
        }
    });
    let mut client = mock_client(port);
    let error = client
        .query_raw_with(&Query::new("SELECT payload"), |_| {
            Err(Error::Config {
                reason: "caller rejected body".into(),
            })
        })
        .unwrap_err();
    assert!(matches!(error, Error::Config { reason } if reason == "caller rejected body"));
    assert_eq!(
        client.fetch_bytes(&Query::new("SELECT payload")).unwrap(),
        b"healthy"
    );
    server.join().unwrap();
}

/// Owned sinks reuse caller capacity; bounded slices reject overflow without claiming success.
#[test]
fn caller_owned_sinks_keep_bounds_and_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        for _ in 0..4 {
            let mut stream = accept_one(&listener);
            read_http(&mut stream);
            write_response(&mut stream, b"result");
        }
    });
    let mut client = mock_client(port);
    let query = Query::new("SELECT payload");
    let mut owned = Vec::with_capacity(128);
    owned.extend_from_slice(b"stale");
    let pointer = owned.as_ptr();
    let capacity = owned.capacity();
    assert_eq!(client.query_raw_into(&query, &mut owned).unwrap(), 6);
    assert_eq!(owned, b"result");
    assert_eq!(owned.as_ptr(), pointer);
    assert_eq!(owned.capacity(), capacity);
    let mut exact = [0_u8; 6];
    assert_eq!(client.query_raw_into_slice(&query, &mut exact).unwrap(), 6);
    assert_eq!(&exact, b"result");
    let mut larger = [0x7f_u8; 8];
    assert_eq!(client.query_raw_into_slice(&query, &mut larger).unwrap(), 6);
    assert_eq!(&larger[..6], b"result");
    assert_eq!(&larger[6..], &[0x7f, 0x7f]);
    let mut insufficient = [0_u8; 5];
    assert!(matches!(
        client.query_raw_into_slice(&query, &mut insufficient),
        Err(Error::Config { .. })
    ));
    server.join().unwrap();
}

/// The transport stays on the test thread; only the controlled peer runs independently.
fn mock_client(port: u16) -> Client {
    let endpoint = Endpoint::parse(format!("http://127.0.0.1:{port}")).unwrap();
    let mut client = Client::connect(endpoint, "default", "", "default").unwrap();
    client.set_http_version(HttpVersionChoice::Http1).unwrap();
    client
}

/// Unwinding revokes the borrowed receiver before the same client is used again.
#[test]
fn callback_panic_revokes_scoped_receiver() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        for body in [b"panic".as_slice(), b"healthy".as_slice()] {
            let mut stream = accept_one(&listener);
            read_http(&mut stream);
            write_response(&mut stream, body);
        }
    });
    let mut client = mock_client(port);
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let callback_state = std::rc::Rc::clone(&calls);
        client.query_raw_with(&Query::new("SELECT payload"), move |_| {
            callback_state.set(callback_state.get() + 1);
            panic!("caller callback panic");
        })
    }));
    assert!(panic.is_err());
    assert_eq!(calls.get(), 1);
    assert_eq!(
        client.fetch_bytes(&Query::new("SELECT payload")).unwrap(),
        b"healthy"
    );
    assert_eq!(
        calls.get(),
        1,
        "the revoked receiver must not be called again"
    );
    server.join().unwrap();
}

/// Exception headers reject even HTTP 200 before invoking a borrowed receiver.
#[test]
fn successful_status_with_exception_is_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let mut stream = accept_one(&listener);
            read_http(&mut stream);
            stream.write_all(b"HTTP/1.1 200 OK\r\nx-cLiCkHoUsE-ExCePtIoN-cOdE: 241\r\nContent-Length: 13\r\nConnection: close\r\n\r\nmemory denied").unwrap();
            stream.flush().unwrap();
        }
    });
    let mut client = mock_client(port);
    let query = Query::new("SELECT payload");
    let buffered = client.fetch_bytes(&query).unwrap_err();
    assert!(
        matches!(buffered, Error::Http { status: 200, message } if message.contains("241") && message.contains("memory denied"))
    );
    let mut called = false;
    let streamed = client
        .query_raw_with(&query, |_| {
            called = true;
            Ok(())
        })
        .unwrap_err();
    assert!(!called);
    assert!(
        matches!(streamed, Error::Http { status: 200, message } if message.contains("241") && message.contains("memory denied"))
    );
    server.join().unwrap();
}

/// A semicolon and misleading quoted FORMAT text do not suppress binary output.
#[test]
fn native_format_is_appended_after_sql_tokens() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let mut stream = accept_one(&listener);
        let (header, _) = read_http(&mut stream);
        assert!(header.contains("FORMAT%20Native"));
        assert!(!header.contains("%3B%0AFORMAT"));
        write_response(&mut stream, &sample_response());
    });
    let mut client = mock_client(port);
    let blocks = clickhouse::row::RowClient::query_blocks(
        &mut client,
        "SELECT ' format ' AS x; -- trailing",
    )
    .unwrap();
    assert_eq!(blocks[0].row_count(), 1);
    assert!(clickhouse::row::RowClient::query_blocks(&mut client, "SELECT 1 FORMAT JSON").is_err());
    server.join().unwrap();
}

/// Staged Native inserts identify columns exactly like direct shared-block inserts.
#[test]
fn staged_insert_preserves_explicit_columns() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let mut stream = accept_one(&listener);
        let (direct_header, direct_body) = read_http(&mut stream);
        write_response(&mut stream, b"");
        let mut stream = accept_one(&listener);
        let (staged_header, staged_body) = read_http(&mut stream);
        write_response(&mut stream, b"");
        let direct_target = direct_header.lines().next().unwrap();
        let staged_target = staged_header.lines().next().unwrap();
        assert_eq!(staged_target, direct_target);
        assert!(staged_target.contains("%28%60id%60%29%20FORMAT%20Native"));
        let mut expected = direct_body.clone();
        expected.extend_from_slice(&direct_body);
        assert_eq!(staged_body, expected);
    });
    let mut client = mock_client(port);
    client.insert_block("events", &sample_block()).unwrap();
    let mut staged = client.http_inserter("events").unwrap();
    staged.write(&sample_block()).unwrap();
    staged.write(&sample_block()).unwrap();
    staged.end().unwrap();
    server.join().unwrap();
}
