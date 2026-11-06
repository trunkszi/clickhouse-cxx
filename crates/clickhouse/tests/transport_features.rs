//! Integration scenarios for explicit transport availability and configuration boundaries.
use clickhouse::{Client, ClientConfig, Error};

/// Enabled HTTP configuration accepts positive submillisecond deadlines without dialing.
#[cfg(feature = "http")]
#[test]
fn configured_http_preserves_capabilities() {
    for scheme in ["http", "https"] {
        let config = ClientConfig::parse(&format!("{scheme}://127.0.0.1:1"))
            .unwrap()
            .with_timeout(std::time::Duration::from_micros(500));
        let client = Client::new(config).expect("enabled HTTP configuration must be accepted");
        assert!(matches!(
            client.try_server_info(),
            Err(Error::Unsupported(_))
        ));
    }
}

/// Disabled HTTP configuration fails before opening a connection.
#[cfg(not(feature = "http"))]
#[test]
fn disabled_http_configuration_is_rejected() {
    let config = ClientConfig::parse("http://127.0.0.1:1").unwrap();
    assert!(matches!(Client::new(config), Err(Error::Unsupported(_))));
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

/// Custom Native trust/socket options fail closed before any HTTP connection.
#[cfg(feature = "http")]
#[test]
fn http_rejects_native_tls_options_before_io() {
    use std::time::Duration;

    use clickhouse::TlsSettings;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("https://{}", listener.local_addr().unwrap());
    let settings = [
        TlsSettings::new().with_ca_pem(b"custom CA".as_slice()),
        TlsSettings::new().with_skip_verification(true),
        TlsSettings::new().with_native_roots(false),
        TlsSettings::new().with_sni("custom.example"),
        TlsSettings::new().with_connect_timeout(Duration::from_secs(3)),
        TlsSettings::new().with_read_timeout(Duration::from_secs(3)),
        TlsSettings::new().with_write_timeout(Duration::from_secs(3)),
        TlsSettings::new().with_tcp_nodelay(false),
        TlsSettings::new().with_keepalive(Duration::from_secs(30), Duration::from_secs(2), 2),
    ];
    for tls in settings {
        let config = ClientConfig::parse(&endpoint).unwrap().with_tls(tls);
        assert!(matches!(Client::new(config), Err(Error::Unsupported(_))));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

/// HTTP cannot silently accept Native retry or health-probe policies.
#[cfg(feature = "http")]
#[test]
fn http_rejects_custom_native_failover() {
    let policies = [
        clickhouse::FailoverPolicy::new().with_send_retries(3),
        clickhouse::FailoverPolicy::new().with_ping_before_query(true),
        clickhouse::FailoverPolicy::new().with_circuit(1, std::time::Duration::from_secs(2)),
    ];
    for policy in policies {
        let config = ClientConfig::parse("http://127.0.0.1:1")
            .unwrap()
            .with_policy(policy);
        assert!(matches!(Client::new(config), Err(Error::Unsupported(_))));
    }
}
