//! Integration scenarios for rustls and plain TCP, including the C++ socket factory.

use std::{
    io::{ErrorKind, Read, Write},
    net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use clickhouse_sys::{ConnectParams, EchoParams, TlsSession, connect, factory_echo};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

const PAYLOAD: &[u8] = b"clickhouse-cxx-echo";

struct Issued {
    ca_pem: String,
    cert_pem: String,
    key_pem: String,
}

struct Peer {
    port: u16,
    thread: Option<JoinHandle<()>>,
}

#[test]
fn plain_echo_reports_nodelay_and_keepalive() {
    let peer = Peer::spawn(PAYLOAD.len(), None);
    let mut params = params("127.0.0.1", peer.port);
    params.tcp_nodelay = true;
    params.tcp_keepalive = true;
    let session = connect(&params).expect("plain connect");
    assert!(session.tcp_nodelay());
    assert!(session.tcp_keepalive());
    exchange(&session, PAYLOAD);
}

#[test]
fn custom_ca_matches_sni_and_echoes() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let mut params = params("127.0.0.1", peer.port);
    params.secure = true;
    params.sni = "localhost";
    params.ca_pem = issued.ca_pem.as_bytes();
    let session = connect(&params).expect("tls connect");
    exchange(&session, PAYLOAD);
}

#[test]
fn native_roots_reject_the_test_ca() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let mut params = params("127.0.0.1", peer.port);
    params.secure = true;
    params.sni = "localhost";
    params.use_native_roots = true;
    let err = connect(&params).expect_err("native roots reject the test CA");
    assert!(!err.to_string().is_empty());
}

#[test]
fn skip_verification_accepts_the_test_ca() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let mut params = params("127.0.0.1", peer.port);
    params.secure = true;
    params.sni = "localhost";
    params.skip_verification = true;
    let session = connect(&params).expect("skip verification");
    exchange(&session, PAYLOAD);
}

#[test]
fn sni_mismatch_is_rejected() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let mut params = params("127.0.0.1", peer.port);
    params.secure = true;
    params.sni = "mismatch.invalid";
    params.ca_pem = issued.ca_pem.as_bytes();
    assert!(connect(&params).is_err());
}

#[test]
fn connect_timeout_finishes_quickly() {
    let mut params = params("192.0.2.1", 81);
    params.connect_timeout = Duration::from_millis(200);
    let started = Instant::now();
    let err = connect(&params).expect_err("blackhole connect");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "connect took {:?}: {err}",
        started.elapsed()
    );
}

#[test]
fn factory_echoes_plain_tcp() {
    let peer = Peer::spawn(PAYLOAD.len(), None);
    let echoed = factory_echo(&echo_params(peer.port, false, b"")).expect("factory echo");
    assert_eq!(echoed, PAYLOAD);
}

#[test]
fn factory_echoes_tls() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let echoed = factory_echo(&echo_params(peer.port, true, issued.ca_pem.as_bytes()))
        .expect("factory tls echo");
    assert_eq!(echoed, PAYLOAD);
}

/// A peer closing without TLS close_notify cannot masquerade as a clean EOF.
#[test]
fn truncated_tls_is_an_error() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let mut params = params("127.0.0.1", peer.port);
    params.secure = true;
    params.sni = "localhost";
    params.ca_pem = issued.ca_pem.as_bytes();
    let session = connect(&params).expect("tls connect");
    exchange(&session, PAYLOAD);
    let error = session
        .read(&mut [0; 1])
        .expect_err("truncated TLS rejected");
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
}

/// Invalid custom CA bytes are rejected even when other trust anchors exist.
#[test]
fn malformed_custom_ca_is_rejected() {
    let issued = issue_certs();
    let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
    let mut params = params("127.0.0.1", peer.port);
    params.secure = true;
    params.sni = "localhost";
    params.use_native_roots = true;
    params.ca_pem = b"not a certificate";
    let error = connect(&params).expect_err("invalid custom CA rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("custom CA PEM"));
}

/// Configuration and certificate failures remain fatal across the C++ bridge.
#[test]
fn factory_trust_errors_are_fatal() {
    let issued = issue_certs();
    let other = issue_certs();
    for invalid_ca in [b"not a certificate".as_slice(), other.ca_pem.as_bytes()] {
        let peer = Peer::spawn(PAYLOAD.len(), Some(server_config(&issued)));
        let error = factory_echo(&echo_params(peer.port, true, invalid_ca))
            .expect_err("invalid trust must fail closed");
        assert!(error.starts_with("fatal:"), "{error}");
    }
}

fn exchange(session: &TlsSession, payload: &[u8]) {
    session.write_all(payload).expect("write");
    session.flush().expect("flush");
    let mut got = vec![0u8; payload.len()];
    read_exact(session, &mut got).expect("read");
    assert_eq!(got, payload);
}

fn read_exact(session: &TlsSession, buf: &mut [u8]) -> std::io::Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let Some(slot) = buf.get_mut(filled..) else {
            return Err(std::io::Error::other("echo read cursor is out of range"));
        };
        let read = session.read(slot)?;
        if read == 0 {
            return Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "echo peer closed",
            ));
        }
        filled = filled.saturating_add(read);
    }
    Ok(())
}

fn params<'a>(host: &'a str, port: u16) -> ConnectParams<'a> {
    ConnectParams {
        host,
        port,
        secure: false,
        sni: "",
        ca_pem: b"",
        use_native_roots: false,
        skip_verification: false,
        connect_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(2),
        tcp_nodelay: true,
        tcp_keepalive: false,
        keepalive_idle: Duration::from_secs(30),
        keepalive_interval: Duration::from_secs(5),
        keepalive_count: 3,
    }
}

fn echo_params(port: u16, secure: bool, ca_pem: &[u8]) -> EchoParams {
    EchoParams {
        host: "127.0.0.1".to_owned(),
        port,
        secure,
        sni: if secure {
            "localhost".to_owned()
        } else {
            String::new()
        },
        ca_pem: ca_pem.to_vec(),
        use_native_roots: false,
        skip_verification: false,
        connect_timeout_ms: 2_000,
        recv_timeout_ms: 2_000,
        send_timeout_ms: 2_000,
        tcp_nodelay: true,
        tcp_keepalive: false,
        keepalive_idle_secs: 60,
        keepalive_interval_secs: 5,
        keepalive_count: 3,
        payload: PAYLOAD.to_vec(),
    }
}

fn issue_certs() -> Issued {
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "clickhouse-test-ca");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");

    let server_key = KeyPair::generate().expect("server key");
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_owned()]).expect("server params");
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server cert");
    Issued {
        ca_pem: ca_cert.pem(),
        cert_pem: server_cert.pem(),
        key_pem: server_key.serialize_pem(),
    }
}

fn server_config(issued: &Issued) -> Arc<ServerConfig> {
    let certs = rustls_pemfile::certs(&mut issued.cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse cert");
    let key = rustls_pemfile::private_key(&mut issued.key_pem.as_bytes())
        .expect("parse key")
        .expect("key pem");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .expect("tls versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("server config");
    Arc::new(config)
}

impl Peer {
    fn spawn(expected: usize, tls: Option<Arc<ServerConfig>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind echo");
        let port = listener.local_addr().expect("echo port").port();
        listener.set_nonblocking(true).expect("nonblocking accept");
        let thread = thread::spawn(move || serve(listener, expected, tls));
        Self {
            port,
            thread: Some(thread),
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        let Some(handle) = self.thread.take() else {
            return;
        };
        let joined = handle.join();
        if !thread::panicking() {
            joined.expect("echo thread panicked");
        }
    }
}

fn serve(listener: TcpListener, expected: usize, tls: Option<Arc<ServerConfig>>) {
    let Some(stream) = accept_for(listener, Duration::from_secs(5)) else {
        return;
    };
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    if stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .is_err()
    {
        return;
    }
    if stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .is_err()
    {
        return;
    }
    if let Some(config) = tls {
        let Ok(connection) = ServerConnection::new(config) else {
            return;
        };
        let mut tls_stream = StreamOwned::new(connection, stream);
        let _copied = copy_echo(&mut tls_stream, expected);
    } else {
        let mut plain = stream;
        let _copied = copy_echo(&mut plain, expected);
    }
}

fn accept_for(listener: TcpListener, budget: Duration) -> Option<TcpStream> {
    let started = Instant::now();
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Some(stream),
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                if started.elapsed() > budget {
                    return None;
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return None,
        }
    }
}

fn copy_echo(io: &mut (impl Read + Write), expected: usize) -> std::io::Result<()> {
    let mut buf = vec![0u8; expected];
    io.read_exact(&mut buf)?;
    io.write_all(&buf)?;
    io.flush()?;
    Ok(())
}
