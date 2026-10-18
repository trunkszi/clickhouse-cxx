# 06: Native `tcps://` (`rustls` `SocketFactory`) & Multi-Endpoint `FailoverPolicy`

**What to build:** Provide pure-Rust TLS (`rustls`, zero OpenSSL) for Native TCP (`tcps://`) by injecting a custom C++ `SocketFactory` bridged via `cxx`, and unify multi-endpoint high-availability `FailoverPolicy` across both `Native` and `Http` transports.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Connecting to a `tcps://` `Endpoint` uses a custom C++ `SocketFactory` / `SocketBase` whose `InputStream` and `OutputStream` delegate reads and writes across `cxx` to `rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>` (supporting SNI, native root certificates via `rustls-native-certs`, custom CA certificates, and optional verification skip for local testing) with zero OpenSSL linkage.
- [ ] `ClientConfig` and `FailoverPolicy` accept multiple `Endpoint`s and enforce identical round-robin rotation, `send_retries`, `retry_timeout`, `ping_before_query`, and `reset_connection_endpoint` semantics across both `Native` TCP and `Http` transports.
- [ ] Endpoint failover transitions emit `FailoverEvent` (containing `old_index`, `new_index`, `old_endpoint`, `new_endpoint` using `Cow`) to registered callbacks and, when `features = ["tracing"]` is enabled, emit exactly one structured `warn!` log per failover episode.
