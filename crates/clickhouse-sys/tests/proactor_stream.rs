//! Native ring integration, DMA retirement, hostname, and lifecycle coverage.
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use clickhouse_sys::{ConnectParams, TlsSession, connect};

fn options(host: &str, port: u16) -> ConnectParams<'_> {
    ConnectParams {
        host,
        port,
        secure: false,
        sni: "",
        ca_pem: b"",
        use_native_roots: false,
        skip_verification: false,
        connect_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_millis(150),
        write_timeout: Duration::from_secs(2),
        tcp_nodelay: true,
        tcp_keepalive: false,
        keepalive_idle: Duration::from_secs(30),
        keepalive_interval: Duration::from_secs(5),
        keepalive_count: 3,
    }
}
fn read_exact(session: &TlsSession, bytes: &mut [u8]) {
    let mut offset = 0;
    while offset < bytes.len() {
        let count = session.read(&mut bytes[offset..]).unwrap();
        assert!(count > 0);
        offset += count;
    }
}

#[test]
fn native_dma_chunks_round_trip_beyond_one_buffer() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload: Vec<_> = (0..(1024 * 1024 + 17))
        .map(|index| (index % 251) as u8)
        .collect();
    let expected = payload.clone();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = vec![0; expected.len()];
        stream.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, expected);
        stream.write_all(&bytes).unwrap();
    });
    let session = connect(&options("127.0.0.1", port)).unwrap();
    session.write_all(&payload).unwrap();
    let mut received = vec![0; payload.len()];
    read_exact(&session, &mut received);
    assert_eq!(received, payload);
    peer.join().unwrap();
}

#[test]
fn receive_timeout_retires_dma_before_the_next_receive() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let release = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&release);
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !signal.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        stream.write_all(b"fresh after cancellation").unwrap();
    });
    let session = connect(&options("127.0.0.1", port)).unwrap();
    let mut untouched = [0xA5; 32];
    let start = Instant::now();
    assert_eq!(
        session.read(&mut untouched).unwrap_err().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(untouched, [0xA5; 32]);
    release.store(true, Ordering::Release);
    let mut actual = [0; 24];
    read_exact(&session, &mut actual);
    assert_eq!(&actual, b"fresh after cancellation");
    peer.join().unwrap();
}

#[test]
fn native_hostname_uses_the_registered_resolver() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"resolved").unwrap();
    });
    let session = connect(&options("localhost", port)).unwrap();
    let mut bytes = [0; 8];
    read_exact(&session, &mut bytes);
    assert_eq!(&bytes, b"resolved");
    peer.join().unwrap();
}

#[test]
fn native_dns_failure_obeys_the_shared_dial_budget() {
    let mut options = options("missing-native-proactor.invalid", 8123);
    options.connect_timeout = Duration::from_millis(150);
    let start = Instant::now();
    let error = connect(&options).unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(
        matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::TimedOut
        ),
        "{error}"
    );
}
