//! Certificate trust and socket deadlines for Native TCP and TCPS.
//! Another synchronous transport may borrow the same trust configuration.

use std::{borrow::Cow, time::Duration};

/// Certificate and socket options applied to one dial.
#[derive(Debug, Clone)]
pub struct TlsSettings<'a> {
    ca_pem: Cow<'a, [u8]>,
    use_native_roots: bool,
    skip_verification: bool,
    sni: Cow<'a, str>,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
    tcp_nodelay: bool,
    tcp_keepalive: bool,
    keepalive_idle: Duration,
    keepalive_interval: Duration,
    keepalive_count: u32,
}

impl Default for TlsSettings<'static> {
    fn default() -> Self {
        Self {
            ca_pem: Cow::Borrowed(b""),
            use_native_roots: true,
            skip_verification: false,
            sni: Cow::Borrowed(""),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            tcp_nodelay: true,
            tcp_keepalive: false,
            keepalive_idle: Duration::from_secs(60),
            keepalive_interval: Duration::from_secs(5),
            keepalive_count: 3,
        }
    }
}

impl TlsSettings<'static> {
    /// Platform roots, `TCP_NODELAY`, and a 5 second connect timeout.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<'a> TlsSettings<'a> {
    /// PEM trust anchors. The bytes are borrowed when the caller passes a slice.
    #[must_use]
    pub fn with_ca_pem(mut self, ca_pem: impl Into<Cow<'a, [u8]>>) -> Self {
        self.ca_pem = ca_pem.into();
        self
    }

    /// Trust the platform store from `rustls-native-certs`.
    #[must_use]
    pub fn with_native_roots(mut self, use_native_roots: bool) -> Self {
        self.use_native_roots = use_native_roots;
        self
    }

    /// Skip certificate verification for local self-signed servers.
    ///
    /// Handshake signatures are still checked.
    #[must_use]
    pub fn with_skip_verification(mut self, skip_verification: bool) -> Self {
        self.skip_verification = skip_verification;
        self
    }

    /// DNS name advertised as SNI. Empty uses the dialed host.
    #[must_use]
    pub fn with_sni(mut self, sni: impl Into<Cow<'a, str>>) -> Self {
        self.sni = sni.into();
        self
    }

    /// TCP connect budget. Zero selects 5 seconds in the dialer.
    #[must_use]
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Read budget. Zero waits without a deadline.
    #[must_use]
    pub fn with_read_timeout(mut self, read_timeout: Duration) -> Self {
        self.read_timeout = read_timeout;
        self
    }

    /// Write budget. Zero waits without a deadline.
    #[must_use]
    pub fn with_write_timeout(mut self, write_timeout: Duration) -> Self {
        self.write_timeout = write_timeout;
        self
    }

    /// Set `TCP_NODELAY` on the dialed socket.
    #[must_use]
    pub fn with_tcp_nodelay(mut self, tcp_nodelay: bool) -> Self {
        self.tcp_nodelay = tcp_nodelay;
        self
    }

    /// Enable TCP keepalive with the given idle time, interval, and probe count.
    #[must_use]
    pub fn with_keepalive(mut self, idle: Duration, interval: Duration, count: u32) -> Self {
        self.tcp_keepalive = true;
        self.keepalive_idle = idle;
        self.keepalive_interval = interval;
        self.keepalive_count = count;
        self
    }

    /// Borrowed CA bytes.
    #[must_use]
    pub fn ca_pem(&self) -> &Cow<'a, [u8]> {
        &self.ca_pem
    }

    /// Report whether platform roots are trusted.
    #[must_use]
    pub fn use_native_roots(&self) -> bool {
        self.use_native_roots
    }

    /// Report whether certificate verification is skipped.
    #[must_use]
    pub fn skip_verification(&self) -> bool {
        self.skip_verification
    }

    /// Configured SNI override.
    #[must_use]
    pub fn sni(&self) -> &Cow<'a, str> {
        &self.sni
    }

    /// Connect timeout.
    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Read timeout.
    #[must_use]
    pub fn read_timeout(&self) -> Duration {
        self.read_timeout
    }

    /// Write timeout.
    #[must_use]
    pub fn write_timeout(&self) -> Duration {
        self.write_timeout
    }

    /// Report whether `TCP_NODELAY` is requested.
    #[must_use]
    pub fn tcp_nodelay(&self) -> bool {
        self.tcp_nodelay
    }

    /// Report whether keepalive is requested.
    #[must_use]
    pub fn tcp_keepalive(&self) -> bool {
        self.tcp_keepalive
    }

    /// Keepalive idle time.
    #[must_use]
    pub fn keepalive_idle(&self) -> Duration {
        self.keepalive_idle
    }

    /// Keepalive probe interval.
    #[must_use]
    pub fn keepalive_interval(&self) -> Duration {
        self.keepalive_interval
    }

    /// Keepalive probe count.
    #[must_use]
    pub fn keepalive_count(&self) -> u32 {
        self.keepalive_count
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn timeout_ms(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
    }
}
