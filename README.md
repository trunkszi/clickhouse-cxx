# clickhouse-cxx

A general-purpose, purely synchronous, high-performance ClickHouse client SDK for Rust, powered by [`cxx`](https://cxx.rs/) (statically linking [`ClickHouse/clickhouse-cpp`](https://github.com/ClickHouse/clickhouse-cpp) for Native TCP) and [`curl-rust`](https://github.com/trunkszi/curl-rust) (with `rustls` and HTTP/2 for HTTP/HTTPS).

---

## Key Features

- **Zero Async Runtime**: Strictly synchronous execution across the entire workspace. No `tokio`, `futures`, `async-std`, or `smol` dependencies enter your dependency graph.
- **Dual Transport Parity**: A single `clickhouse::Client` connects over either:
  - **Native TCP (`tcp://`, `tcps://`)**: Backed by `ClickHouse/clickhouse-cpp` compiled with the 3.x modern API (`CH_USE_3X_API=ON`) and custom `rustls` TLS socket streams (`WITH_OPENSSL=OFF`).
  - **HTTP/2 (`http://`, `https://`)**: Backed by `curl-rust` with `http2` and `rustls-ffi` enabled by default, speaking ClickHouse `FORMAT Native` for full columnar parity with TCP as well as `FORMAT RowBinary` and `FORMAT ArrowStream`.
- **Zero-Copy Columnar Projection (`BlockView<'a>`)**: Fixed-width C++ column buffers (`Numeric`, `Bool`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Time`, `Time64`, `Decimal32/64/128/256`, `UUID`, `IPv4`, `IPv6`, `FixedString(N)`, `Enum8`, `Enum16`, and `#[repr(transparent)]` newtypes) are projected directly into Rust as `&'a [T]` slices via [`zerocopy`](https://crates.io/crates/zerocopy) without allocation or copying.
- **Single `#[derive(Row)]` Macro & Reusable SoA `Batch` Buffers**: Annotating a struct with `#[derive(Row)]` generates:
  - `Row` trait implementation,
  - `<Name>Batch`: a Struct-of-Arrays (SoA) columnar write buffer with `.clear()` to reset lengths to zero while retaining allocated heap capacity across ingestion cycles,
  - `<Name>BlockView<'a>`: a zero-copy columnar view over a `Block`, and
  - `<Name>Ref<'a>`: a zero-allocation borrowed row view (`RowRef<'a>`).
- **Zero-Clone & `Cow` First**: All configuration builders, SQL queries, table names, parameters, settings, and error metadata accept borrowed slices (`&'a str`, `&'a [u8]`) or `std::borrow::Cow<'a, str>` / `Cow<'a, [u8]>`.
- **Full `clickhouse-cpp` Feature Coverage**:
  - All 17 ClickHouse column families (`Numeric`, `Bool`, `String`, `FixedString`, `Date`/`DateTime`/`Time`, `Decimal`, `UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`, `Nullable`, `Array`, `LowCardinality`, `Tuple`, `Map`, `Geo`, `Json`, `Nothing`).
  - RAII `BlockStream<'a>` (`BeginSelect` / `NextBlock` / `Cancel`) and `Inserter<'a>` (`BeginInsert` / `SendInsertBlock` / `EndInsert`).
  - In-memory `ExternalTable` query execution (`SelectWithExternalData`).
  - Multi-endpoint `FailoverPolicy` with round-robin rotation, retry budgets, and `FailoverEvent` callbacks.
  - `SerdeBridge` for reading and writing `serde`-only structs (`#[derive(Serialize, Deserialize)]`) over both HTTP and Native TCP.
  - Optional `tracing` telemetry (`features = ["tracing"]`, disabled by default) restricted strictly to cold control-plane paths.

---

## Workspace Architecture

| Crate | Role |
| :--- | :--- |
| `clickhouse-sys` | CMake static build orchestration (`CH_USE_3X_API=ON`, `WITH_OPENSSL=OFF`) linking `clickhouse-cpp`, `lz4`, `zstd`, `cityhash`, and `abseil` statically, bridged to Rust via `#[cxx::bridge]`. |
| `clickhouse-macros` | Procedural macro crate providing `#[derive(Row)]` and `#[row(...)]` field/struct attributes. |
| `clickhouse` | Safe, idiomatic synchronous client library exposing `Client`, `Query`, `Block`, `Column`, `BlockStream`, `Inserter`, `ExternalTable`, `FailoverPolicy`, `SerdeBridge`, `Error`, and re-exporting `#[derive(Row)]`. |

---

## Documentation

- [`GLOSSARY.md`](./GLOSSARY.md): Ubiquitous domain language and core type definitions.
- [`docs/adr/0001-dual-transport-synchronous-client.md`](./docs/adr/0001-dual-transport-synchronous-client.md): Dual-Transport Synchronous Client Architecture.
- [`docs/adr/0002-http-format-native-parity.md`](./docs/adr/0002-http-format-native-parity.md): Columnar `Block` Parity over HTTP via `FORMAT Native`.
- [`docs/adr/0003-clickhouse-cpp-3x-modern-api.md`](./docs/adr/0003-clickhouse-cpp-3x-modern-api.md): Enforcing `clickhouse-cpp` 3.x Modern API & Static Compilation.
