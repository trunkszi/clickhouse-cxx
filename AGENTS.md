# clickhouse-cxx

A synchronous, thread-affine ClickHouse client SDK for Rust. Native TCP and
TCPS use `clickhouse-cpp` through `cxx`; HTTP and HTTPS use `curl-rust` with
HTTP/2 and `rustls`; typed rows use `#[derive(Row)]`.

## Synchronous & Thread-Affine Execution

- **Synchronous**: every call blocks on the calling thread. `tokio`, `futures`,
  `async-std`, and `smol` stay out of the workspace.
- **Thread-affine core**: `Client`, transport, `EventLoop`, and `TlsSession`
  handles are `!Send + !Sync`. Sockets, blocks, views, and callbacks remain on
  their creating thread; the I/O path takes no mutex.
- **`thread_local!` scaling**: `ThreadLocalClient` keeps one client per calling
  OS thread. `WorkerClient` leases lock-free slots `0..N` on the calling thread,
  configured by `ClientConfig::with_workers(N)`. Each slot stripes endpoints
  with `endpoints[(worker_id + step) % len]`. Data-plane execution stays on the
  calling thread; no cross-thread channel dispatch.
- **Local observers**: `ClientConfig::on_failover` stores one `FnMut` callback;
  shared state uses `Rc<RefCell<_>>` on the same thread.

## Cargo Features & Dependencies

- **Transport features**: `crates/clickhouse` defines `default = ["tcp"]`;
  `tcp` enables `clickhouse-sys` + `cxx`; `http` enables `curl` from
  `https://github.com/trunkszi/curl-rust.git` branch `feat/event-loop-ws` with
  `http2` + `rustls`, plus `lz4_flex` and `zstd`. `http`-only builds never
  compile `clickhouse-sys`.
- **Native TCP/TCPS**: `clickhouse-sys` builds `clickhouse-cpp` 3.x statically
  with CMake; `cxx` bridges the API; `rustls` supplies custom `SocketFactory`
  TLS streams.
- **HTTP/2**: the HTTP transport speaks `FORMAT Native`, `FORMAT RowBinary`, and
  `FORMAT ArrowStream` with the same typed rows and blocks.
- **`tracing`**: optional and off by default. It emits cold control-plane
  lifecycle, failover, and `OnServerLog` events; hot data-plane paths emit zero
  logs. Disabled builds compile telemetry to no-ops.
- **Inline manifests**: every dependency in every `Cargo.toml` uses
  inline-table syntax, for example `clickhouse-sys = { workspace = true }`.
- **Portable source resolution**: `crates/clickhouse-sys/build.rs` resolves
  `clickhouse-cpp` through `CLICKHOUSE_CPP_DIR`, then
  `../../vendor/clickhouse-cpp`, then `target/vendor-cache/clickhouse-cpp`,
  with no hardcoded absolute paths.

## Zero-Copy, `Cow` Borrowing & Read/Write Buffers

- **Borrowed inputs**: APIs accept `&str`, `&[u8]`, `Cow<'a, str>`, or
  `Cow<'a, [u8]>`.
- **Zero-copy views**: `BlockView<'a>` projects fixed-width columns as
  `&'a [T]` through `zerocopy`; string, array, nullable, and map projections
  borrow the block.
- **Reusable writes**: `Batch` and `Block` expose `clear()` and
  `clear_with_policy(&BufferPolicy)`; `BufferPolicy::default()` retains at most
  65,536 rows and 8 MiB.
- **Callback reads**: `query_views`, `query_for_each`, `query_row_refs`, and the
  `SerdeBridge` `_with` / `_for_each` forms stream rows while the backing block
  is alive.
- **Caller-buffer reads**: `read_into_slice`, `read_bytes_into`,
  `query_rows_into_slice`, `query_batch_into`, `query_block_into`, and the
  `SerdeBridge` `_into` / `_into_slice` forms fill memory the caller owns and
  reuses.

## Code & Repository Hygiene

- **Domain names**: `WireBlock`, `WireColumn`, `FixedWireColumn`, `WireError`,
  `column/`, and `wire_block.rs`; zero `cpp_*` or `Cpp*` identifiers in
  `crates/clickhouse` and `crates/clickhouse-macros`.
- **File size**: every `.rs`, `.cpp`, and `.h` file stays at or below 800 lines.
- **Safety and docs**: every `unsafe` block carries `// SAFETY:`; every fallible
  or panicking public function documents `# Errors` or `# Panics`; every
  `unsafe fn` documents `# Safety`. Source comments, rustdoc, errors, logs, and
  commits are English.
- **Committed surface**: only product source, manifests, tests, examples, and
  the root and crate READMEs are tracked; working notes remain outside the
  repository.
- **Rebased PRs**: features develop on dedicated branches and merge into `main`
  through rebased pull requests.
