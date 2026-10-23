#![cfg(feature = "http")]
//! Public nonblocking HTTP admission, progress, cancellation, and ownership.
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

use clickhouse::{Client, ClientConfig, Error, HttpVersionChoice, Query};

fn client(port: u16) -> Client {
    let mut client = Client::new(
        ClientConfig::parse(&format!("http://127.0.0.1:{port}"))
            .unwrap()
            .with_timeout(Duration::from_secs(5)),
    )
    .unwrap();
    client.set_http_version(HttpVersionChoice::Http1).unwrap();
    client.reset_http().unwrap();
    client
}
fn read_head(socket: &mut impl Read) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).unwrap(), 1);
        head.push(byte[0]);
    }
}
fn until(mut step: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !step() {
        assert!(Instant::now() < deadline, "progress deadline exceeded");
        thread::yield_now();
    }
}

#[test]
fn submit_and_poll_return_while_the_peer_withholds_its_response() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let arrived = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&arrived);
    let gate = Arc::clone(&release);
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        read_head(&mut socket);
        seen.store(true, Ordering::Release);
        until(|| gate.load(Ordering::Acquire));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nresult")
            .unwrap();
    });
    let mut client = client(port);
    let started = Instant::now();
    let mut request = client
        .submit_http(&Query::new("SELECT value FORMAT RawBLOB"), &[])
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(request.try_take().is_none());
    until(|| {
        client.poll_http().unwrap();
        arrived.load(Ordering::Acquire)
    });
    for _ in 0..100 {
        client.poll_http().unwrap();
        assert!(!request.is_complete());
    }
    release.store(true, Ordering::Release);
    until(|| {
        client.poll_http().unwrap();
        request.is_complete()
    });
    assert_eq!(request.try_take().unwrap().unwrap(), b"result");
    assert!(request.try_take().is_none());
    assert!(request.is_complete());
    assert!(!client.cancel_http(&request).unwrap());
    server.join().unwrap();
}

#[test]
fn explicit_cancel_is_exactly_once_and_checks_client_ownership() {
    let mut client = client(1);
    let mut other = self::client(1);
    let mut request = client.submit_http(&Query::new("SELECT slow"), &[]).unwrap();
    assert!(matches!(
        other.cancel_http(&request),
        Err(Error::Config { .. })
    ));
    assert!(matches!(
        client.set_http_version(HttpVersionChoice::Http2PriorKnowledge),
        Err(Error::Config { .. })
    ));
    assert!(client.cancel_http(&request).unwrap());
    assert!(!client.cancel_http(&request).unwrap());
    assert!(request.is_complete());
    assert!(matches!(
        request.try_take().unwrap(),
        Err(Error::HttpTransport(_))
    ));
    assert!(request.try_take().is_none());
}

#[test]
fn dropping_the_client_terminates_an_outstanding_request_handle() {
    let mut client = client(1);
    let mut request = client
        .submit_http(&Query::new("SELECT pending"), &[])
        .unwrap();
    drop(client);
    assert!(request.is_complete());
    assert!(matches!(
        request.try_take().unwrap(),
        Err(Error::HttpTransport(_))
    ));
    assert!(request.try_take().is_none());
}

/// Completed handles cannot cancel a reused identifier after a driver configuration change.
#[test]
fn completed_handle_cannot_cancel_a_new_driver_request() {
    let mut client = client(1);
    let previous = client.submit_http(&Query::new("SELECT old"), &[]).unwrap();
    assert!(client.cancel_http(&previous).unwrap());
    client
        .set_http_version(HttpVersionChoice::Http2PriorKnowledge)
        .unwrap();
    assert!(matches!(
        client.submit_http(&Query::new("SELECT new"), &[]),
        Err(Error::Config { .. })
    ));
    client.reset_http().unwrap();
    let current = client.submit_http(&Query::new("SELECT new"), &[]).unwrap();
    assert!(!client.cancel_http(&previous).unwrap());
    assert!(!current.is_complete());
    assert!(client.cancel_http(&current).unwrap());
}

/// Nonblocking admission never implicitly drains an established keep-alive driver.
#[test]
fn keepalive_reconfiguration_requires_an_explicit_synchronous_reset() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let probe_idle = Arc::new(AtomicBool::new(false));
    let idle = Arc::new(AtomicBool::new(false));
    let allow_reset = Arc::new(AtomicBool::new(false));
    let retired = Arc::new(AtomicBool::new(false));
    let probe = Arc::clone(&probe_idle);
    let observed_idle = Arc::clone(&idle);
    let gate = Arc::clone(&allow_reset);
    let closed = Arc::clone(&retired);
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        first
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        read_head(&mut first);
        first
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nold")
            .unwrap();
        first.set_nonblocking(true).unwrap();
        // Probe after configuration and rejected admission, before reset is allowed.
        until(|| probe.load(Ordering::Acquire));
        match first.read(&mut [0; 1]) {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            other => panic!("keep-alive retired before explicit reset: {other:?}"),
        }
        observed_idle.store(true, Ordering::Release);
        until(|| gate.load(Ordering::Acquire));
        until(|| match first.read(&mut [0; 1]) {
            Ok(0) => true,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
            other => panic!("unexpected retirement read: {other:?}"),
        });
        closed.store(true, Ordering::Release);
        let (mut next, _) = listener.accept().unwrap();
        next.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        read_head(&mut next);
        next.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nfresh")
            .unwrap();
    });
    let mut client = client(port);
    let mut previous = client.submit_http(&Query::new("SELECT old"), &[]).unwrap();
    until(|| {
        client.poll_http().unwrap();
        previous.is_complete()
    });
    assert_eq!(previous.try_take().unwrap().unwrap(), b"old");
    client
        .set_http_version(HttpVersionChoice::Negotiate)
        .unwrap();
    let start = Instant::now();
    assert!(matches!(
        client.submit_http(&Query::new("SELECT fresh"), &[]),
        Err(Error::Config { .. })
    ));
    assert!(start.elapsed() < Duration::from_secs(1));
    probe_idle.store(true, Ordering::Release);
    until(|| idle.load(Ordering::Acquire));
    assert!(!retired.load(Ordering::Acquire));
    allow_reset.store(true, Ordering::Release);
    client.reset_http().unwrap();
    let mut next = client
        .submit_http(&Query::new("SELECT fresh"), &[])
        .unwrap();
    until(|| {
        client.poll_http().unwrap();
        next.is_complete()
    });
    assert_eq!(next.try_take().unwrap().unwrap(), b"fresh");
    assert!(!client.cancel_http(&previous).unwrap());
    server.join().unwrap();
}

#[test]
fn explicit_reset_cancels_pending_handles_once() {
    let mut client = client(1);
    let mut request = client
        .submit_http(&Query::new("SELECT pending"), &[])
        .unwrap();
    client.reset_http().unwrap();
    assert!(request.is_complete());
    assert!(matches!(
        request.try_take().unwrap(),
        Err(Error::HttpTransport(_))
    ));
    assert!(request.try_take().is_none());
    let next = client.submit_http(&Query::new("SELECT next"), &[]).unwrap();
    assert!(!client.cancel_http(&request).unwrap());
    assert!(client.cancel_http(&next).unwrap());
}
