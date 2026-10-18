//! cxx boundary between clickhouse-cpp sockets and [`crate::session`].

use std::time::Duration;

use crate::session::{ConnectParams, TlsSession, connect};

#[cxx::bridge]
mod ffi {
    struct TlsConnect {
        host: String,
        port: u16,
        secure: bool,
        sni: String,
        ca_pem: Vec<u8>,
        use_native_roots: bool,
        skip_verification: bool,
        connect_timeout_ms: u64,
        recv_timeout_ms: u64,
        send_timeout_ms: u64,
        tcp_nodelay: bool,
        tcp_keepalive: bool,
        keepalive_idle_secs: u64,
        keepalive_interval_secs: u64,
        keepalive_count: u32,
    }

    struct NativeOpen {
        host: String,
        port: u16,
        user: String,
        password: String,
        database: String,
        secure: bool,
        sni: String,
        ca_pem: Vec<u8>,
        use_native_roots: bool,
        skip_verification: bool,
        connect_timeout_ms: u64,
        recv_timeout_ms: u64,
        send_timeout_ms: u64,
        tcp_nodelay: bool,
        tcp_keepalive: bool,
        keepalive_idle_secs: u64,
        keepalive_interval_secs: u64,
        keepalive_count: u32,
        send_retries: u32,
        retry_timeout_secs: u64,
        ping_before_query: bool,
        extra_hosts: Vec<String>,
        extra_ports: Vec<u16>,
        compression: u8,
    }

    struct EchoOptions {
        host: String,
        port: u16,
        secure: bool,
        sni: String,
        ca_pem: Vec<u8>,
        use_native_roots: bool,
        skip_verification: bool,
        connect_timeout_ms: u64,
        recv_timeout_ms: u64,
        send_timeout_ms: u64,
        tcp_nodelay: bool,
        tcp_keepalive: bool,
        keepalive_idle_secs: u64,
        keepalive_interval_secs: u64,
        keepalive_count: u32,
        payload: Vec<u8>,
    }

    extern "Rust" {
        type TlsSession;

        fn tls_connect(request: &TlsConnect) -> Result<Box<TlsSession>>;

        fn tls_read(session: &TlsSession, buf: &mut [u8]) -> Result<usize>;

        fn tls_write(session: &TlsSession, buf: &[u8]) -> Result<usize>;

        fn tls_flush(session: &TlsSession) -> Result<()>;
    }

    // SAFETY: Opaque handles remain owned by C++, and all slices are borrowed only for each call.
    unsafe extern "C++" {
        include!("native_client.h");
        include!("rustls_socket.h");

        #[namespace = "chcxx"]
        type Client = crate::ffi::Client;

        #[namespace = "chcxx"]
        fn new_native_client(opts: &NativeOpen) -> Result<UniquePtr<Client>>;

        fn factory_echo(opts: &EchoOptions) -> Result<Vec<u8>>;
    }
}

type BridgeError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
struct BridgeIoError(std::io::Error);

impl std::fmt::Display for BridgeIoError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::io::ErrorKind;
        let prefix = match self.0.kind() {
            ErrorKind::InvalidInput | ErrorKind::InvalidData | ErrorKind::PermissionDenied => {
                "fatal"
            }
            _ => "retriable",
        };
        write!(formatter, "{prefix}: {}", self.0)
    }
}

impl std::error::Error for BridgeIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

fn bridge_io(err: std::io::Error) -> BridgeError {
    Box::new(BridgeIoError(err))
}

fn tls_connect(request: &ffi::TlsConnect) -> Result<Box<TlsSession>, BridgeError> {
    let params = ConnectParams {
        host: &request.host,
        port: request.port,
        secure: request.secure,
        sni: &request.sni,
        ca_pem: &request.ca_pem,
        use_native_roots: request.use_native_roots,
        skip_verification: request.skip_verification,
        connect_timeout: Duration::from_millis(request.connect_timeout_ms),
        read_timeout: Duration::from_millis(request.recv_timeout_ms),
        write_timeout: Duration::from_millis(request.send_timeout_ms),
        tcp_nodelay: request.tcp_nodelay,
        tcp_keepalive: request.tcp_keepalive,
        keepalive_idle: Duration::from_secs(request.keepalive_idle_secs),
        keepalive_interval: Duration::from_secs(request.keepalive_interval_secs),
        keepalive_count: request.keepalive_count,
    };
    connect(&params).map(Box::new).map_err(bridge_io)
}

fn tls_read(session: &TlsSession, buf: &mut [u8]) -> Result<usize, BridgeError> {
    session.read(buf).map_err(bridge_io)
}

fn tls_write(session: &TlsSession, buf: &[u8]) -> Result<usize, BridgeError> {
    session.write(buf).map_err(bridge_io)
}

fn tls_flush(session: &TlsSession) -> Result<(), BridgeError> {
    session.flush().map_err(bridge_io)
}

/// Options for one clickhouse-cpp client using the rustls socket factory.
pub struct NativeOptions {
    /// Primary host passed to `SetHost`.
    pub host: String,
    /// Primary port passed to `SetPort`.
    pub port: u16,
    /// ClickHouse user.
    pub user: String,
    /// ClickHouse password.
    pub password: String,
    /// Default database.
    pub database: String,
    /// Use rustls when true. Plain TCP when false.
    pub secure: bool,
    /// TLS server name. Empty uses the dialed host.
    pub sni: String,
    /// Extra PEM trust anchors.
    pub ca_pem: Vec<u8>,
    /// Trust the platform root store.
    pub use_native_roots: bool,
    /// Skip certificate verification for local self-signed tests.
    pub skip_verification: bool,
    /// Connect timeout in milliseconds. Zero uses 5 seconds inside rustls dial.
    pub connect_timeout_ms: u64,
    /// Receive timeout in milliseconds. Zero means no socket read deadline.
    pub recv_timeout_ms: u64,
    /// Send timeout in milliseconds. Zero means no socket write deadline.
    pub send_timeout_ms: u64,
    /// `TCP_NODELAY`.
    pub tcp_nodelay: bool,
    /// TCP keepalive.
    pub tcp_keepalive: bool,
    /// Keepalive idle seconds.
    pub keepalive_idle_secs: u64,
    /// Keepalive interval seconds.
    pub keepalive_interval_secs: u64,
    /// Keepalive probe count.
    pub keepalive_count: u32,
    /// clickhouse-cpp `send_retries`. Zero still walks each endpoint once on connect.
    pub send_retries: u32,
    /// clickhouse-cpp retry sleep in whole seconds.
    pub retry_timeout_secs: u64,
    /// Ask clickhouse-cpp to ping before each query.
    pub ping_before_query: bool,
    /// Extra endpoint hosts appended after the primary host.
    pub extra_hosts: Vec<String>,
    /// Extra endpoint ports, paired with `extra_hosts`.
    pub extra_ports: Vec<u16>,
    /// Block compression: 0 none, 1 LZ4, 2 ZSTD.
    pub compression: u8,
}

impl Default for NativeOptions {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 9000,
            user: "default".to_owned(),
            password: String::new(),
            database: "default".to_owned(),
            secure: false,
            sni: String::new(),
            ca_pem: Vec::new(),
            use_native_roots: true,
            skip_verification: false,
            connect_timeout_ms: 5_000,
            recv_timeout_ms: 0,
            send_timeout_ms: 0,
            tcp_nodelay: true,
            tcp_keepalive: false,
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 5,
            keepalive_count: 3,
            send_retries: 0,
            retry_timeout_secs: 0,
            ping_before_query: false,
            extra_hosts: Vec::new(),
            extra_ports: Vec::new(),
            compression: 0,
        }
    }
}

/// Echo `payload` through `RustlsSocketFactory` to a listening peer.
///
/// # Errors
///
/// Returns the C++ exception text when connect, write, or read fails.
pub fn factory_echo(opts: &EchoParams) -> Result<Vec<u8>, String> {
    ffi::factory_echo(&opts.to_ffi()).map_err(|err| err.to_string())
}

/// Byte payload and socket options for [`factory_echo`].
pub struct EchoParams {
    /// Dial host.
    pub host: String,
    /// Dial port.
    pub port: u16,
    /// Use rustls when true.
    pub secure: bool,
    /// TLS server name.
    pub sni: String,
    /// PEM trust anchors.
    pub ca_pem: Vec<u8>,
    /// Trust the platform root store.
    pub use_native_roots: bool,
    /// Skip certificate verification.
    pub skip_verification: bool,
    /// Connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Read timeout in milliseconds.
    pub recv_timeout_ms: u64,
    /// Write timeout in milliseconds.
    pub send_timeout_ms: u64,
    /// `TCP_NODELAY`.
    pub tcp_nodelay: bool,
    /// TCP keepalive.
    pub tcp_keepalive: bool,
    /// Keepalive idle seconds.
    pub keepalive_idle_secs: u64,
    /// Keepalive interval seconds.
    pub keepalive_interval_secs: u64,
    /// Keepalive probe count.
    pub keepalive_count: u32,
    /// Bytes written and expected back.
    pub payload: Vec<u8>,
}

impl EchoParams {
    fn to_ffi(&self) -> ffi::EchoOptions {
        ffi::EchoOptions {
            host: self.host.clone(),
            port: self.port,
            secure: self.secure,
            sni: self.sni.clone(),
            ca_pem: self.ca_pem.clone(),
            use_native_roots: self.use_native_roots,
            skip_verification: self.skip_verification,
            connect_timeout_ms: self.connect_timeout_ms,
            recv_timeout_ms: self.recv_timeout_ms,
            send_timeout_ms: self.send_timeout_ms,
            tcp_nodelay: self.tcp_nodelay,
            tcp_keepalive: self.tcp_keepalive,
            keepalive_idle_secs: self.keepalive_idle_secs,
            keepalive_interval_secs: self.keepalive_interval_secs,
            keepalive_count: self.keepalive_count,
            payload: self.payload.clone(),
        }
    }
}

impl NativeOptions {
    fn to_ffi(&self) -> ffi::NativeOpen {
        ffi::NativeOpen {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            secure: self.secure,
            sni: self.sni.clone(),
            ca_pem: self.ca_pem.clone(),
            use_native_roots: self.use_native_roots,
            skip_verification: self.skip_verification,
            connect_timeout_ms: self.connect_timeout_ms,
            recv_timeout_ms: self.recv_timeout_ms,
            send_timeout_ms: self.send_timeout_ms,
            tcp_nodelay: self.tcp_nodelay,
            tcp_keepalive: self.tcp_keepalive,
            keepalive_idle_secs: self.keepalive_idle_secs,
            keepalive_interval_secs: self.keepalive_interval_secs,
            keepalive_count: self.keepalive_count,
            send_retries: self.send_retries,
            retry_timeout_secs: self.retry_timeout_secs,
            ping_before_query: self.ping_before_query,
            extra_hosts: self.extra_hosts.clone(),
            extra_ports: self.extra_ports.clone(),
            compression: self.compression,
        }
    }
}

/// Open a canonical block-capable client using the rustls socket factory.
///
/// # Errors
/// Returns the classified connection error on a failed connection or handshake.
pub fn new_native_client(
    opts: &NativeOptions,
) -> Result<cxx::UniquePtr<crate::ffi::Client>, String> {
    ffi::new_native_client(&opts.to_ffi()).map_err(|err| err.to_string())
}
