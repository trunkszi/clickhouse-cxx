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

## Examples & Tutorials

The examples under `crates/clickhouse/examples/` form a progressive, runnable tutorial. Every example runs offline by default. Examples that include a live-server section read `CLICKHOUSE_URL` (for example `tcp://127.0.0.1:9000`) and optionally `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE`.

| Example | What it teaches |
| :--- | :--- |
| `01_quickstart_native_tcp` | Build a row-layer `Block` and a native `WireBlock`, inspect their schema, project `&[u64]` and `&[f64]` without copying, and optionally round-trip through the Native TCP `Client`. |
| `02_derive_row_and_zero_copy_views` | `#[derive(Row)]` attributes (`table`, `rename_all`, `rename`, `bytes`), borrowed `Cow` fields, `Option`, `Vec`, and `BTreeMap`, plus the generated `Batch`, `BlockView`, and `Ref` types. |
| `03_memory_policy_and_batch_reuse` | The real `clickhouse::BufferPolicy`: `default()`, `unbounded()`, `minimal()`, `allocated_bytes()`, policy-driven `clear_with_policy()`, and value-preserving `shrink_to_fit()`. |
| `04_full_column_type_matrix` | The complete column-family tour: numeric, Bool, String/FixedString/JSON, date/time, decimals, UUID, IP, enums, Nullable, Array, LowCardinality, Tuple, Map, Geo, and Nothing. |
| `05_dual_transport_and_row_client` | The typed `RowClient` surface (`insert_batch`, `insert_rows`, `query_rows`, `query_views`) over an in-memory transport and Native TCP, including Bool/Array/Nullable/Map bridge columns. |
| `06_serde_compatibility` | `SerdeBridge` for `serde`-only structs (RowBinary and `Block` conversion), `RowClient::insert_serde`/`query_serde` over an in-memory transport and Native TCP, and `#[serde(rename_all)]`, `#[serde(rename)]`, and `#[serde(with = "serde_bytes")]` support in `#[derive(Row)]`. |

```bash
cargo run -p clickhouse --example 01_quickstart_native_tcp
cargo run -p clickhouse --example 02_derive_row_and_zero_copy_views
cargo run -p clickhouse --example 03_memory_policy_and_batch_reuse
cargo run -p clickhouse --example 04_full_column_type_matrix
cargo run -p clickhouse --example 05_dual_transport_and_row_client
cargo run -p clickhouse --example 06_serde_compatibility
```

## Serde Compatibility

`SerdeBridge` accepts structs that implement only `serde::Serialize` and
`serde::Deserialize`; `#[derive(Row)]` is not required.

- `SerdeBridge::to_rowbinary(&rows)` encodes a slice into ClickHouse
  `RowBinary` and returns the serde field names captured from the first row.
- `SerdeBridge::from_rowbinary(&bytes)` decodes concatenated RowBinary rows
  back into owned values.
- `SerdeBridge::rows_to_block(&rows)` builds the row-layer `Block` by
  dispatching serde fields into typed ClickHouse columns. `Option` becomes
  `Nullable`, `Vec` becomes `Array`, and `serde_bytes` payloads become binary
  `String` columns.
- `SerdeBridge::block_to_rows(&block)` materializes rows from a block by
  matching column names to serde field names.

`RowClient` exposes the same bridge as `insert_serde(table, &rows)` and
`query_serde::<T>(sql)`, so an in-memory transport and the Native TCP `Client`
share one typed API.

`#[derive(Row)]` also understands the serde naming attributes:
`#[serde(rename_all = "...")]` and `#[serde(rename = "...")]` select column
names, and `#[serde(with = "serde_bytes")]` selects a binary `String` column
instead of `Array(UInt8)`.

## Documentation

- [`GLOSSARY.md`](./GLOSSARY.md): Ubiquitous domain language and core type definitions.
- [`docs/adr/0001-dual-transport-synchronous-client.md`](./docs/adr/0001-dual-transport-synchronous-client.md): Dual-Transport Synchronous Client Architecture.
- [`docs/adr/0002-http-format-native-parity.md`](./docs/adr/0002-http-format-native-parity.md): Columnar `Block` Parity over HTTP via `FORMAT Native`.
- [`docs/adr/0003-clickhouse-cpp-3x-modern-api.md`](./docs/adr/0003-clickhouse-cpp-3x-modern-api.md): Enforcing `clickhouse-cpp` 3.x Modern API & Static Compilation.
