# Specification: `clickhouse-cxx`

**Status:** ready-for-agent

## Problem Statement

Rust applications that strictly forbid asynchronous runtimes (`tokio`, `futures`, `async-std`, `smol`)—such as low-latency trading engines, deterministic replay services, and synchronous quantitative research pipelines—currently have no general-purpose, high-performance ClickHouse client SDK in the Rust ecosystem. Existing Rust ClickHouse libraries mandate an `async` runtime, forcing synchronous codebases to either isolate ClickHouse interactions into separate sidecar processes/repositories or sacrifice native columnar performance and type safety. Furthermore, existing solutions lack true zero-copy column slice projections (`zerocopy`) over C++ `Block` memory, require excessive heap cloning for row construction, and do not unify ClickHouse's Native TCP protocol and HTTP/2 protocol behind a single synchronous interface.

## Solution

Build `clickhouse-cxx`, a general-purpose, purely synchronous, high-performance ClickHouse client SDK for Rust organized as a three-crate workspace (`clickhouse-sys`, `clickhouse-macros`, and `clickhouse`).

1. **`clickhouse-sys`** automatically detects or fetches `ClickHouse/clickhouse-cpp` at build time, compiles it statically via CMake with the 3.x modern API (`CH_USE_3X_API=ON`) and zero OpenSSL dependency (`WITH_OPENSSL=OFF`), and bridges its entire feature set to Rust via `dtolnay/cxx`.
2. **`clickhouse`** provides a unified, idiomatic, synchronous `Client` that routes across the `Native` TCP `Transport` (`tcp://`, `tcps://` backed by `clickhouse-cpp` and a custom `rustls` `SocketFactory`) and the `Http` `Transport` (`http://`, `https://` backed by `trunkszi/curl-rust` on the `feat/event-loop-ws` branch with `http2` and `rustls-ffi` enabled by default).
3. **Columnar Parity & Zero-Copy**: Both transports support `Block`-level columnar operations (`FORMAT Native` over HTTP and native `Block` frames over TCP). Fixed-width columns inside a `Block` are projected directly into Rust as `&'a [T]` slices via `google/zerocopy` (`FromBytes`, `IntoBytes`, `Immutable`, `KnownLayout`) without allocation or copying.
4. **Single `#[derive(Row)]` Macro & `SerdeBridge`**: `clickhouse-macros` provides `#[derive(Row)]`, which simultaneously implements `Row`, generates a reusable Struct-of-Arrays (SoA) `<Name>Batch` write buffer (with `.clear()` capacity retention), a zero-copy `<Name>BlockView<'a>` column projection, and a zero-allocation borrowed `<Name>Ref<'a>` (`RowRef`). Types that only implement `serde::Serialize` / `serde::Deserialize` are transparently supported on both transports via `SerdeBridge` (`FORMAT RowBinary` on HTTP and dynamic schema `Block` column dispatch on Native TCP).

## User Stories

1. As a Rust systems engineer working in an async-free codebase, I want to connect to ClickHouse and execute queries synchronously without pulling `tokio` or `futures` into my dependency tree, so that my binary respects strict synchronous runtime invariants.
2. As a build engineer, I want `clickhouse-sys` to automatically detect `CLICKHOUSE_CPP_DIR` or clone `ClickHouse/clickhouse-cpp` with submodules during `cargo build` and link `clickhouse-cpp`, `lz4`, `zstd`, `cityhash`, and `abseil` statically, so that the resulting binary has zero runtime shared-library dependencies on C++ ClickHouse libraries.
3. As a security-conscious developer, I want both `tcps://` (Native TCP over TLS) and `https://` (HTTP/2 over TLS) connections to use `rustls` exclusively without compiling or linking OpenSSL, so that TLS behavior is memory-safe and consistent across my workspace.
4. As an application developer, I want a single `Client` type that accepts `tcp://`, `tcps://`, `http://`, or `https://` endpoints and exposes the same synchronous API, so that switching between Native TCP (port 9000/9440) and HTTP/2 (port 8123/8443) requires only a configuration change.
5. As a cluster operator, I want to configure multiple comma-separated or list-based `Endpoint`s with a `FailoverPolicy` (`send_retries`, `retry_timeout`, `ping_before_query`, round-robin rotation, and `FailoverEvent` callbacks) that behaves identically on both `Native` and `Http` transports, so that node failures are handled automatically without application downtime.
6. As a data engineer, I want to annotate my domain struct with `#[derive(Row)]` (and `#[row(...)]` attributes without redundant `ClickHouse` prefixes), so that the compiler automatically generates the `Row` trait implementation, a `<Name>Batch` SoA write buffer, a `<Name>BlockView<'a>` zero-copy reader, and a `<Name>Ref<'a>` borrowed row view.
7. As a performance engineer, I want `<Name>BlockView<'a>` and `Block::column_slice::<T>()` to cast fixed-width C++ column buffers (`Numeric`, `Bool`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Time`, `Time64`, `Decimal32/64/128/256`, `UUID`, `IPv4`, `IPv6`, `FixedString(N)`, `Enum8`, `Enum16`, and `#[repr(transparent)]` `zerocopy` newtypes) directly to `&'a [T]` slices via `zerocopy`, so that analytical scans and SIMD reductions run with zero memory copies and zero heap allocations.
8. As a performance engineer, I want variable-length and composite columns (`String`, `Nullable(T)`, `Array(T)`, `LowCardinality(T)`, `Tuple(...)`, `Map(K, V)`, `Geo` types `Point`/`Ring`/`Polygon`/`MultiPolygon`, `Json`, and `Nothing`) to be fully supported across `Block`, `Column`, `Batch`, and `BlockView<'a>`, so that every column type supported by `clickhouse-cpp` is usable in Rust.
9. As a high-throughput ingestion developer, I want `<Name>Batch` and `Block` to provide a `.clear()` method that resets row counts to zero while preserving allocated heap capacity, so that sustained ingestion loops perform zero heap allocations after warmup.
10. As a performance-conscious Rust developer, I want all configuration structs, `Query` builders, table names, parameters, settings, row definitions, and `Error` metadata to accept borrowed references (`&'a str`, `&'a [u8]`) or `std::borrow::Cow<'a, str>` / `Cow<'a, [u8]>`, so that static literals never trigger `.clone()` or `.to_owned()` heap allocations.
11. As a developer reading large datasets, I want `client.select_stream(query)` to return an RAII `BlockStream<'a>` implementing `Iterator<Item = Result<Block>>`, so that I can process arbitrarily large result sets block-by-block with bounded memory and have the query automatically canceled and drained (`Cancel()`) if the iterator is dropped early.
12. As a developer streaming large writes, I want `client.inserter(query)` to return an RAII `Inserter<'a>` session (`BeginInsert` / `SendInsertBlock` / `EndInsert` over Native TCP, or streaming chunks over HTTP/2), so that I can push multiple `Block`s or `Batch`es within a single `INSERT` transaction and guarantee connection cleanup on early drop.
13. As a quantitative researcher, I want to execute `SelectWithExternalData` / `SelectWithExternalDataCancelable` by attaching in-memory `ExternalTable` blocks to a `Query`, so that I can join or filter server tables against transient client-side datasets without creating temporary server tables.
14. As an observability engineer, I want `Query` to support `QueryId`, per-query `QuerySettings` (with `IMPORTANT`, `CUSTOM`, and `OBSOLETE` flags), parameterized `QueryParams`, OpenTelemetry `TracingContext`, and server event callbacks (`OnProgress`, `OnProfile`, `OnProfileEvents`, `OnServerLog`, `OnException`), so that I have full access to ClickHouse's execution telemetry and query controls.
15. As a developer working with existing `serde`-only types (`#[derive(Serialize, Deserialize)]` without `#[derive(Row)]`), I want `SerdeBridge` to read and write those structs seamlessly over both HTTP (`FORMAT RowBinary`) and Native TCP (dynamic column dispatch against the server's schema `Block`), so that third-party or legacy structs work out of the box.
16. As a data pipeline author using Apache Arrow or custom wire formats, I want `Client` to support raw formatted streaming reads and writes (such as `FORMAT ArrowStream` and `FORMAT RowBinary`) along with deterministic FNV-1a batch fingerprinting and `insert_deduplication_token` settings, so that bit-exact IEEE-754 float vectors (`-0.0`, `NaN`) and idempotent retries are supported natively.
17. As a library consumer, I want structured, strongly-typed errors defined with `thiserror` (`clickhouse::Error`) that interoperate cleanly with `anyhow`, so that I can pattern-match on protocol, server exception, schema mismatch, or zero-copy alignment errors while preserving full diagnostic context.
18. As an application integrator, I want `tracing` telemetry to be strictly optional (`features = ["tracing"]`, disabled by default) and restricted to cold control-plane paths (connection lifecycle, endpoint failover episodes, and `OnServerLog` forwarding), so that hot data paths remain completely free of logging overhead.

## Implementation Decisions

- **Workspace Structure**:
  - Three member crates managed under a root workspace (`edition = "2024"`, `resolver = "3"`):
    1. `clickhouse-sys`: CMake static build orchestration and `#[cxx::bridge]` FFI bindings for `clickhouse-cpp`.
    2. `clickhouse-macros`: Procedural macro crate implementing `#[derive(Row)]` and `#[row(...)]` attribute parsing using `syn`, `quote`, `proc-macro2`, `darling`, `heck`, `convert_case`, and `synstructure`.
    3. `clickhouse`: High-level safe library crate exposing `Client`, `Query`, `Block`, `Column`, `BlockStream`, `Inserter`, `ExternalTable`, `FailoverPolicy`, `SerdeBridge`, and `Error`, and re-exporting `#[derive(Row)]`.
- **Static C++ Compilation & 3.x API Enforcement (`clickhouse-sys`)**:
  - `build.rs` resolves the `clickhouse-cpp` source tree by checking `CLICKHOUSE_CPP_DIR`, then a workspace vendor path, and finally cloning `https://github.com/ClickHouse/clickhouse-cpp.git` (`--depth 1 --recurse-submodules`) into `OUT_DIR`.
  - CMake is invoked with `BUILD_SHARED_LIBS=OFF`, `WITH_OPENSSL=OFF`, `CH_USE_3X_API=ON`, `WITH_SYSTEM_ABSEIL=OFF`, `WITH_SYSTEM_CITYHASH=OFF`, `WITH_SYSTEM_LZ4=OFF`, `WITH_SYSTEM_ZSTD=OFF`, and `CMAKE_POSITION_INDEPENDENT_CODE=ON`.
  - All resulting static archives (`clickhouse-cpp-lib-static`, `cityhash`, `lz4`, `zstd`, and required `absl` targets) are linked statically (`cargo:rustc-link-lib=static=...`).
  - Enabling `CH_USE_3X_API=ON` ensures `ColumnBool` is distinct from `ColumnUInt8`, `Int128`/`UInt128` use native 128-bit layout compatible with Rust's `i128`/`u128` and `zerocopy`, and `GetCurrentEndpoint()` returns `Endpoint` by value.
- **Full `rustls` TLS Stack (Zero OpenSSL)**:
  - For Native TCP (`tcps://`), a custom C++ `SocketFactory` / `SocketBase` implementation delegates `InputStream::DoRead` and `OutputStream::DoWrite` across the `cxx` bridge to a Rust `rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>` configured with `rustls-native-certs`.
  - For HTTP (`https://`), `trunkszi/curl-rust` (`branch = "feat/event-loop-ws"`, `features = ["http2"]`) is used, which statically links `libcurl` with `rustls-ffi` and `libnghttp2-sys` (defaulting to HTTP/2 multiplexing with keep-alive and `TCP_NODELAY`).
- **Complete `clickhouse-cpp` Feature Parity**:
  - **17 Column Families**: Full read/write support for `Numeric` (`UInt8/16/32/64/128`, `Int8/16/32/64/128`, `Float32/64`), `Bool`, `String`, `FixedString(N)`, `Date`, `Date32`, `DateTime`, `DateTime64(p, tz)`, `Time`, `Time64`, `Decimal32/64/128/256`, `UUID`, `IPv4`, `IPv6`, `Enum8`, `Enum16`, `Nullable(T)`, `Array(T)`, `LowCardinality(T)`, `Tuple(...)`, `Map(K, V)`, `Geo` (`Point`, `Ring`, `Polygon`, `MultiPolygon`), `Json`, and `Nothing`.
  - **Client & Query Execution**: Full support for `Execute`, `Select` (callback), `SelectCancelable` (early-termination callback), `SelectWithExternalData` / `SelectWithExternalDataCancelable` (`ExternalTable` slice), `BeginSelect` / `NextBlock` / `Cancel` / `IsSelecting` (wrapped in RAII `BlockStream<'a>`), `Insert`, `BeginInsert` / `SendInsertBlock` / `EndInsert` / `IsInserting` (wrapped in RAII `Inserter<'a>`), `Ping`, `ResetConnection`, `ResetConnectionEndpoint`, `GetServerInfo`, `GetCurrentEndpoint`, and `GetVersion`.
  - **Query Metadata & Events**: `Query` supports `QueryId`, `QuerySettings` (`QuerySettingsField` with `IMPORTANT`, `CUSTOM`, `OBSOLETE` flags), `QueryParams` (`Option<Cow<'a, str>>`), OpenTelemetry `TracingContext` (`trace_id`, `span_id`, `tracestate`, `trace_flags`), and callbacks for `OnProgress` (`Progress`), `OnProfile` (`Profile`), `OnProfileEvents` (`Block`), `OnServerLog` (`Block`), and `OnException` (`ServerException`).
  - **Compression**: Native block compression supports `CompressionMethod::None`, `CompressionMethod::Lz4`, and `CompressionMethod::Zstd`.
- **Columnar Parity over HTTP (`FORMAT Native`)**:
  - When `Client` is configured with `http://` or `https://` endpoints, block-level operations (`insert_block`, `insert_batch`, `query_blocks`, `select_stream`) serialize and deserialize `Block` payloads using ClickHouse's `FORMAT Native` binary wire format, guaranteeing identical `BlockView<'a>` zero-copy semantics across TCP and HTTP.
- **Zero-Copy (`zerocopy`) & SoA `Batch` Architecture**:
  - C++ contiguous column buffers (`ColumnVector<T>`, `ColumnDecimal`, `ColumnFixedString`, `ColumnUUID`, `ColumnIPv4`, `ColumnIPv6`, `ColumnDate*`, `ColumnEnum*`, and inner values of `ColumnArray<T>` / `ColumnNullable<T>`) expose their raw byte slice `&[u8]` across `cxx`.
  - Rust casts those slices in-place using `zerocopy::FromBytes::ref_from_bytes` to `&'a [T]` for any `T: FromBytes + Immutable + KnownLayout` (including user-defined `#[repr(transparent)]` newtypes).
  - Write-side `Batch` buffers store fixed-width fields as `Vec<T>` (written to C++ via a single contiguous slice copy using `zerocopy::IntoBytes::as_bytes`) and variable-length fields (`String`, `Array`, etc.) using flat `Offsets (Vec<u64>) + Values` buffers that match ClickHouse's internal `ColumnString` / `ColumnArray` memory layout.
  - Every `Batch` and `Block` provides `.clear()` to reset element counts without releasing heap capacity.
- **Zero-Clone, `Cow` Discipline & Workspace Lints**:
  - All workspace crates inherit `[workspace.lints]` modeled on `questra` and `memora`, enforcing `deny` on `clone_on_copy`, `redundant_clone`, `expl_impl_clone_on_copy`, `non_canonical_clone_impl`, `cloned_instead_of_copied`, `unnecessary_to_owned`, `implicit_clone`, `inefficient_to_string`, `useless_conversion`, `useless_asref`, `to_string_in_format_args`, `manual_string_new`, `undocumented_unsafe_blocks`, `missing_safety_doc`, `missing_errors_doc`, `missing_panics_doc`, `let_underscore_must_use`, `todo`, and `unimplemented`.
  - Every source file (`.rs`, `.cpp`, `.h`) must remain `<= 800` lines.
  - `README.md`, source comments, rustdoc, error messages, and optional `tracing` log events are written in English.
- **Optional `tracing` Feature**:
  - `tracing` is gated behind an optional Cargo feature (`features = ["tracing"]`, off by default). Internal logging macros compile to no-ops when disabled and are strictly prohibited in hot data paths (`Batch`, `BlockView`, `RowBinary`, `Column` accessors).

## Testing Decisions

- **Primary Testing Seams**:
  1. **Top-Level `clickhouse::Client` Seam**: All protocol, streaming, compression, external table, failover, and `SerdeBridge` behaviors are tested through the public `clickhouse::Client` API rather than internal transport or FFI methods.
  2. **In-Memory `Block` $\leftrightarrow$ `#[derive(Row)]` Seam**: All 17 column families, `zerocopy` slice projections (`BlockView<'a>`), borrowed row views (`RowRef<'a>`), `Batch::clear()` capacity reuse, and `FORMAT Native` / `FORMAT RowBinary` binary round-trips are tested directly against the public `Block`, `Column`, and `#[derive(Row)]` types.
- **Dual-Layer Verification Strategy (Offline Mock + Automated Docker Container)**:
  - **Deterministic In-Process Tests**: Fast unit and integration tests run against in-memory `Block` buffers and lightweight in-process `std::net::TcpListener` peers (for injecting deterministic TCP/HTTP failures, verifying `FailoverPolicy` rotation, HTTP/2 headers, `insert_deduplication_token` propagation, and early-drop cancellation).
  - **Automated Docker E2E Suite**: A synchronous RAII test fixture (`TestClickHouseContainer`) automatically spawns an isolated `clickhouse/clickhouse-server:26.8.2.7` Docker container mapped to ephemeral host ports (`-p 0:9000 -p 0:8123`), waits for readiness, runs end-to-end tests across both `tcp://` and `http://` transports (covering DDL, all 17 column types, LZ4/ZSTD compression, `BlockStream`, `Inserter`, `ExternalTable`, `QueryParams`, `QuerySettings`, server callbacks, and `SerdeBridge`), and tears down the container on `Drop`.

## Out of Scope

- Asynchronous APIs, `Future`-returning methods, or integration with `tokio`, `async-std`, or `smol`.
- Legacy `clickhouse-cpp` 2.x compatibility flags (`CH_MAP_BOOL_TO_UINT8=ON` or `absl::int128` public wrappers).
- OpenSSL linkage (`openssl-sys` or system `libssl`).
- Application-specific domain schemas or business logic from downstream consumers (such as `memora` or `questra`).

## Further Notes

- References:
  - `GLOSSARY.md` for canonical domain terminology (`Client`, `Transport`, `Endpoint`, `FailoverPolicy`, `Block`, `Batch`, `BlockView`, `Row`, `RowRef`, `Column`, `Query`, `BlockStream`, `Inserter`, `ExternalTable`, `SerdeBridge`).
  - `docs/adr/0001-dual-transport-synchronous-client.md`
  - `docs/adr/0002-http-format-native-parity.md`
  - `docs/adr/0003-clickhouse-cpp-3x-modern-api.md`
