//! Completion-driven rustls and plain sessions shared by the C++ socket bridge.

use std::{
    cell::{RefCell, RefMut},
    io::{Error, ErrorKind, Read, Write},
    marker::PhantomData,
    sync::{Arc, OnceLock},
    time::Duration,
};

mod completion;
mod driver;
mod resolve;
mod stream;
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, Error as TlsError, RootCertStore,
    SignatureScheme, StreamOwned,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use stream::ProactorStream;

/// Parameters for one completion-driven TCP connection, optionally wrapped in rustls.
pub struct ConnectParams<'a> {
    /// Dial target. This is not necessarily the TLS server name.
    pub host: &'a str,
    /// TCP port.
    pub port: u16,
    /// Wrap the socket in `rustls` when true.
    pub secure: bool,
    /// DNS name sent as SNI. Empty uses `host` when `host` is a DNS name.
    pub sni: &'a str,
    /// PEM certificates added to the trust store.
    pub ca_pem: &'a [u8],
    /// Trust the platform root store from `rustls-native-certs`.
    pub use_native_roots: bool,
    /// Accept any certificate while still checking handshake signatures.
    pub skip_verification: bool,
    /// TCP connect budget. Zero selects a 5 second budget.
    pub connect_timeout: Duration,
    /// Socket read budget after the handshake. Zero waits without a deadline.
    pub read_timeout: Duration,
    /// Socket write budget after the handshake. Zero waits without a deadline.
    pub write_timeout: Duration,
    /// Set `TCP_NODELAY`.
    pub tcp_nodelay: bool,
    /// Enable TCP keepalive.
    pub tcp_keepalive: bool,
    /// Idle time before the first keepalive probe.
    pub keepalive_idle: Duration,
    /// Interval between keepalive probes.
    pub keepalive_interval: Duration,
    /// Probe count before the kernel drops the connection.
    pub keepalive_count: u32,
}

enum Wire {
    Plain(ProactorStream),
    // Boxed so a plain socket does not reserve the TLS stream (~1KiB).
    Tls(Box<StreamOwned<ClientConnection, ProactorStream>>),
}

/// A thread-confined socket with checked, nonblocking access to its I/O state.
///
/// TLS sessions hold `StreamOwned<ClientConnection, ProactorStream>`. Input and output
/// callbacks borrow this state synchronously; overlapping access returns an error.
/// The session cannot be moved or shared across threads.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<clickhouse_sys::TlsSession>();
/// ```
///
/// ```compile_fail
/// fn require_sync<T: Sync>() {}
/// require_sync::<clickhouse_sys::TlsSession>();
/// ```
pub struct TlsSession {
    io: RefCell<Wire>,
    _thread_affinity: PhantomData<*const ()>,
    nodelay: bool,
    keepalive: bool,
}

impl std::fmt::Debug for TlsSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsSession")
            .field("nodelay", &self.nodelay)
            .field("keepalive", &self.keepalive)
            .finish_non_exhaustive()
    }
}

impl TlsSession {
    /// Read decrypted bytes, or raw bytes when the session is not TLS.
    ///
    /// A clean peer close returns `Ok(0)`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error on reentrant access, socket failure, or a failed handshake record.
    pub fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut io = self.borrow_wire()?;
        loop {
            let result = match &mut *io {
                Wire::Plain(tcp) => tcp.read(buf),
                Wire::Tls(tls) => tls.read(buf),
            };
            match result {
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                other => return other,
            }
        }
    }

    /// Write one slice. The returned count may be shorter than `buf`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error on reentrant access or when the socket or TLS layer rejects the write.
    pub fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut io = self.borrow_wire()?;
        loop {
            let result = match &mut *io {
                Wire::Plain(tcp) => tcp.write(buf),
                Wire::Tls(tls) => tls.write(buf),
            };
            match result {
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                other => return other,
            }
        }
    }

    /// Write the entire slice.
    ///
    /// # Errors
    ///
    /// Returns an I/O error on reentrant access or when a write fails or returns no bytes.
    pub fn write_all(&self, mut buf: &[u8]) -> std::io::Result<()> {
        while !buf.is_empty() {
            let wrote = self.write(buf)?;
            if wrote == 0 {
                return Err(Error::new(
                    ErrorKind::WriteZero,
                    "tls session write returned 0",
                ));
            }
            buf = buf
                .get(wrote..)
                .ok_or_else(|| Error::other("tls session write count exceeded the input slice"))?;
        }
        Ok(())
    }

    /// Flush plaintext into TLS records and the socket.
    ///
    /// # Errors
    ///
    /// Returns an I/O error on reentrant access or when the flush fails.
    pub fn flush(&self) -> std::io::Result<()> {
        let mut io = self.borrow_wire()?;
        match &mut *io {
            Wire::Plain(tcp) => tcp.flush(),
            Wire::Tls(tls) => tls.flush(),
        }
    }

    /// Report the `TCP_NODELAY` flag observed on the connected socket.
    #[must_use]
    pub fn tcp_nodelay(&self) -> bool {
        self.nodelay
    }

    /// Report whether TCP keepalive was enabled on the connected socket.
    #[must_use]
    pub fn tcp_keepalive(&self) -> bool {
        self.keepalive
    }

    fn borrow_wire(&self) -> std::io::Result<RefMut<'_, Wire>> {
        self.io
            .try_borrow_mut()
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "reentrant TLS session I/O access"))
    }
}

/// Open a completion-driven connection using `params`.
///
/// # Errors
///
/// Returns an I/O error when name resolution, the TCP handshake, certificate
/// verification, or the TLS handshake fails.
pub fn connect(params: &ConnectParams<'_>) -> std::io::Result<TlsSession> {
    let mut tcp = ProactorStream::dial(params.host, params.port, params.connect_timeout)?;
    let (nodelay, keepalive) = configure_tcp(&tcp, params)?;
    let handshake_read = if params.read_timeout.is_zero() {
        if params.connect_timeout.is_zero() {
            Duration::from_secs(5)
        } else {
            params.connect_timeout
        }
    } else {
        params.read_timeout
    };
    let handshake_write = if params.write_timeout.is_zero() {
        handshake_read
    } else {
        params.write_timeout
    };
    tcp.read_budget = handshake_read;
    tcp.write_budget = handshake_write;

    let wire = if params.secure {
        let config = client_config(params)?;
        let server_name = server_name(params)?;
        let mut connection = ClientConnection::new(Arc::new(config), server_name)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
        while connection.is_handshaking() {
            let transferred = connection.complete_io(&mut tcp)?;
            let _read = transferred.0;
            let _written = transferred.1;
        }
        tcp.read_budget = params.read_timeout;
        tcp.write_budget = params.write_timeout;
        Wire::Tls(Box::new(StreamOwned::new(connection, tcp)))
    } else {
        tcp.read_budget = params.read_timeout;
        tcp.write_budget = params.write_timeout;
        Wire::Plain(tcp)
    };

    Ok(TlsSession {
        io: RefCell::new(wire),
        _thread_affinity: PhantomData,
        nodelay,
        keepalive,
    })
}

/// Advances a retry deadline on the calling thread's persistent ring.
///
/// # Errors
/// Returns a native ring initialization, submission, or completion error.
pub fn wait_retry(duration: Duration) -> std::io::Result<()> {
    driver::with(|driver| driver.delay(duration))
}

pub(crate) fn ring_provider() -> &'static Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER.get_or_init(|| Arc::new(rustls::crypto::ring::default_provider()))
}

fn configure_tcp(
    tcp: &ProactorStream,
    params: &ConnectParams<'_>,
) -> std::io::Result<(bool, bool)> {
    let socket = socket2::SockRef::from(tcp);
    socket.set_nodelay(params.tcp_nodelay)?;
    let nodelay = socket.nodelay()?;
    if params.tcp_keepalive {
        let mut keepalive = socket2::TcpKeepalive::new()
            .with_time(params.keepalive_idle)
            .with_interval(params.keepalive_interval);
        if params.keepalive_count > 0 {
            keepalive = keepalive.with_retries(params.keepalive_count);
        }
        socket2::SockRef::from(tcp).set_tcp_keepalive(&keepalive)?;
    }
    let keepalive = socket2::SockRef::from(tcp).keepalive()?;
    Ok((nodelay, keepalive))
}

fn server_name(params: &ConnectParams<'_>) -> std::io::Result<ServerName<'static>> {
    let name = if params.sni.is_empty() {
        params.host
    } else {
        params.sni
    };
    ServerName::try_from(name)
        .map(|name| name.to_owned())
        .map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid server name `{name}`"),
            )
        })
}

fn client_config(params: &ConnectParams<'_>) -> std::io::Result<ClientConfig> {
    let provider = Arc::clone(ring_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
    if params.skip_verification {
        return Ok(builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification { provider }))
            .with_no_client_auth());
    }
    let roots = trust_roots(params.ca_pem, params.use_native_roots)?;
    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

fn trust_roots(ca_pem: &[u8], use_native_roots: bool) -> std::io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    if use_native_roots {
        let loaded = rustls_native_certs::load_native_certs();
        if !loaded.errors.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("native certificate store load failed: {:?}", loaded.errors),
            ));
        }
        for cert in loaded.certs {
            roots
                .add(cert)
                .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
        }
    }
    if !ca_pem.is_empty() {
        let mut cursor = std::io::Cursor::new(ca_pem);
        let mut count = 0;
        for cert in rustls_pemfile::certs(&mut cursor) {
            count += 1;
            let cert = cert?;
            roots
                .add(cert)
                .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
        }
        if count == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "custom CA PEM contains no certificates",
            ));
        }
    }
    if roots.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "no CA certificates configured for verification",
        ));
    }
    Ok(roots)
}

struct SkipServerVerification {
    provider: Arc<CryptoProvider>,
}

impl std::fmt::Debug for SkipServerVerification {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SkipServerVerification")
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
