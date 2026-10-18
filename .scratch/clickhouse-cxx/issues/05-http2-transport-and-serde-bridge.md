# 05: HTTP/2 `Transport` (`curl-rust`), `FORMAT Native` Parity, `SerdeBridge`, and `ArrowStream`

**What to build:** Implement the synchronous `Http` `Transport` powered by `trunkszi/curl-rust` (`branch = "feat/event-loop-ws"`, `features = ["http2"]`, backed by `rustls-ffi`) with `FORMAT Native` columnar parity, dual-transport `SerdeBridge` for `serde`-only types, raw format streaming (`FORMAT ArrowStream`), and deterministic batch deduplication tokens.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] `Client` routes `http://` and `https://` `Endpoint`s to the synchronous `curl-rust` `Http` `Transport` with HTTP/2 multiplexing, connection keep-alive, `TCP_NODELAY`, and LZ4/ZSTD compression support.
- [ ] Block-level APIs (`insert_block`, `insert_batch`, `query_blocks`, `select_stream`, `inserter`) over HTTP use `FORMAT Native` so that `Block` and zero-copy `BlockView<'a>` behave identically across `tcp://` and `http://`.
- [ ] `SerdeBridge` allows any Rust struct implementing `serde::Serialize` / `serde::Deserialize` (without `#[derive(Row)]`) to be inserted and queried over both HTTP (`FORMAT RowBinary`) and Native TCP (dynamic field dispatch into/from the server's schema `Block`).
- [ ] Raw formatted read/write APIs (`insert_formatted`, `fetch_bytes`) support `FORMAT ArrowStream` (preserving IEEE-754 `-0.0` and `NaN` bit patterns), FNV-1a batch content fingerprinting, and `insert_deduplication_token` propagation.
