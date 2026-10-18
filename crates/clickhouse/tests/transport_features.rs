//! Integration scenarios for explicit transport availability and configuration boundaries.
use clickhouse::{Client, ClientConfig, Error};

/// Managed Native configuration must not silently discard HTTP transport settings.
#[test]
fn managed_configuration_rejects_http_endpoints() {
    for scheme in ["http", "https"] {
        let config = ClientConfig::parse(&format!("{scheme}://127.0.0.1:1")).unwrap();
        let error = match Client::new(config) {
            Ok(_) => panic!("managed configuration accepted an unsupported HTTP endpoint"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::Unsupported(_)));
    }
}

/// A Native-only build rejects HTTP and HTTPS before opening a network connection.
#[cfg(not(feature = "http"))]
#[test]
fn disabled_http_transport_fails_before_io() {
    assert_disabled_transport("http", "http");
    assert_disabled_transport("https", "http");
}

/// An HTTP-only build rejects TCP and TCPS before opening a network connection.
#[cfg(not(feature = "tcp"))]
#[test]
fn disabled_native_transport_fails_before_io() {
    assert_disabled_transport("tcp", "tcp");
    assert_disabled_transport("tcps", "tcp");
}

/// A local listening socket proves the rejected request did not dial its destination.
#[cfg(any(not(feature = "tcp"), not(feature = "http")))]
fn assert_disabled_transport(scheme: &str, feature: &str) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = clickhouse::Endpoint::parse(format!("{scheme}://127.0.0.1:{port}")).unwrap();
    let error = match Client::connect(endpoint, "default", "", "default") {
        Ok(_) => panic!("disabled {scheme} transport was accepted"),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::Unsupported(ref reason) if reason.contains("feature") && reason.contains(feature))
    );
    let error = listener
        .accept()
        .expect_err("disabled transport must not open a socket");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
}
