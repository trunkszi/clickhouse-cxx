# clickhouse-cxx

A thread-affine ClickHouse SDK with completion-driven socket I/O. Native TCP and
TCPS use [`clickhouse-cpp`](https://github.com/ClickHouse/clickhouse-cpp)
through [`cxx`](https://cxx.rs/); HTTP and HTTPS use
[`nexora-rs`](https://github.com/trunkszi/nexora-rs) with HTTP/1.1, HTTP/2,
and `rustls`.
Typed rows use `#[derive(Row)]`; serde-only structs use `SerdeBridge`.

## Why clickhouse-cxx?

ClickHouse rewards large columnar inserts, but the worker filling those columns
may also need a small query result before its next decision. A market-data
collector or strategy engine often already owns its ingest buffers, flush cadence,
and query budget. In a shared-nothing design, keeping that work on its original
worker matters as much as batching the rows efficiently.

Async-first Rust clients such as
[clickhouse-rs](https://github.com/ClickHouse/clickhouse-rs) build on Tokio and
Hyper. Using that model from a synchronous pipeline means introducing a runtime
and a `block_on` bridge, or propagating `async` through the processing code.
Executor queues and wakeups add scheduling boundaries that can contribute to
jitter and tail latency around otherwise short ClickHouse operations.

clickhouse-cxx keeps connection progress on the calling thread. Native TCP and
TCPS use `clickhouse-cpp`, with socket operations driven by a persistent
single-threaded `liburing::Proactor` and hostname lookup through `cares-rs`.
HTTP/1.1 and HTTP/2 use `nexora-rs` on the same caller-owned execution model.
Neither transport requires an async runtime. Native queries and convenience
HTTP calls return synchronously; HTTP submission and polling also let the worker
interleave other work while a response is pending.

The data path stays column-oriented. Reusable batches hold insert columns, and
`BlockView<'a>` borrows fixed-width query columns from the backing block without
materializing an owned Rust object for every row. `ThreadLocalClient` reuses one
client per OS thread; `WorkerClient` leases worker slots and balances connections
on the calling thread. Blocks, borrowed views, and callbacks stay with their
owner instead of being handed to another task or thread.

That ownership also helps when an INSERT stalls or a query times out. Breakpoints
in batch construction, block decoding, and deadline handling lead through explicit
client calls and completion dispatch, without an executor's task-polling stack.
LLM-assisted query and ingestion code has the same concrete lifecycle to follow:
borrow a client, submit or execute the query, consume its result, and release the
borrow. There is no runtime handle to invent or borrow to carry across an `await`.

The goal is predictable client-side work for high-throughput ingestion and
latency-sensitive queries: explicit buffer reuse, local ownership, and no executor
handoffs. This does not bound ClickHouse execution time, network delay, or OS
scheduling; it gives each worker direct control over its part of the request.

## Design

- **Completion-driven**: Native sockets use a persistent thread-local
  `liburing::Proactor` and c-ares DNS. HTTP exposes nonblocking submission and
  single-turn polling through Nexora. No external executor runs the client.
- **Compatibility calls**: Native C++ protocol operations and the convenience
  HTTP calls retain synchronous return semantics. Native socket adapters drive
  SQEs to completion on that thread; they do not use blocking socket reads or
  writes. Use the HTTP submission interface to interleave application work.
- **Thread-affine**: `Client`, transport, `Proactor`, and `TlsSession` handles
  are `!Send + !Sync`. Sockets, blocks, views, and callbacks stay on the thread
  that created them, and the I/O path uses no blocking locks.
- **`thread_local!` scaling**: `ThreadLocalClient` keeps one client per calling
  OS thread. `WorkerClient` leases lock-free slots on the calling thread and
  balances requests across its own connections. No request or result crosses a
  thread boundary.
- **Dual transport**: Native TCP/TCPS and HTTP/HTTPS expose the same `Block`,
  `BlockView`, `Batch`, and row APIs.
- **Zero-copy reads**: `BlockView<'a>` projects fixed-width columns as `&'a [T]`
  over the native buffers.
- **Reusable buffers**: `Batch` and `Block` expose explicit capacity retention
  through `BufferPolicy`.

## Install and feature matrix

`crates/clickhouse` defines `default = ["tcp"]`. Native TCP and TCPS are
available out of the box; the HTTP transport is opt-in.

| Build | Cargo features | Transports | ClickHouse C++ toolchain |
| :--- | :--- | :--- | :--- |
| Native TCP/TCPS (default) | `default` (`tcp`) | `tcp://`, `tcps://` | `clickhouse-sys` + `clickhouse-cpp` |
| HTTP/2 only | `default-features = false`, `features = ["http"]` | `http://`, `https://` | none |
| Both | `features = ["tcp", "http"]` | all schemes | required |
| Tracing | add `"tracing"` to either build | unchanged | unchanged |

```toml
# Native TCP/TCPS
clickhouse = { path = "crates/clickhouse" }
```

```toml
# HTTP/2 only; clickhouse-sys is not compiled
clickhouse = { path = "crates/clickhouse", default-features = false, features = ["http"] }
```

```toml
# Both transports
clickhouse = { path = "crates/clickhouse", features = ["tcp", "http"] }
```

```toml
# Cold control-plane telemetry
clickhouse = { path = "crates/clickhouse", features = ["tracing"] }
```

`http` pulls `nexora` from the `main` branch of
`https://github.com/trunkszi/nexora-rs.git`, plus
`lz4_flex` and `zstd`. HTTP requires Linux `io_uring` and a C build toolchain
for the upstream ring and DNS bindings, without compiling ClickHouse C++.
`tcp` pulls `clickhouse-sys` and `cxx`, plus Git-main `liburing` and `cares` for
Native I/O and DNS;
`clickhouse-sys` builds `clickhouse-cpp` 3.x statically with CMake. The
`tracing` feature emits only connection lifecycle, failover, and `OnServerLog`
events; data-plane reads and writes emit no logs.

`crates/clickhouse-sys/build.rs` resolves the C++ source through
`CLICKHOUSE_CPP_DIR`, then `../../vendor/clickhouse-cpp`, then
`target/vendor-cache/clickhouse-cpp`. It contains no hardcoded absolute path.

## Quick start

```rust
use clickhouse::{Batch, Client, Endpoint, Row};
use clickhouse::row::RowClient;

#[derive(Row, Default)]
#[row(table = "events", rename_all = "snake_case")]
struct Event {
    event_id: u64,
    kind: String,
    score: f64,
}

fn main() -> clickhouse::Result<()> {
    let endpoint = Endpoint::parse("tcp://127.0.0.1:9000")?;
    let mut client = Client::connect(endpoint, "default", "", "default")?;

    let mut batch = EventBatch::with_capacity(2);
    batch.push(&Event { event_id: 1, kind: String::from("trade"), score: 0.5 });
    client.insert_batch(&batch)?;

    let rows = client.query_rows::<Event>("SELECT event_id, kind, score FROM events")?;
    println!("{} rows", rows.len());
    Ok(())
}
```

## Derived rows and zero-copy views

`#[derive(Row)]` generates four companions:

- `EventBatch`, the reusable struct-of-arrays write buffer;
- `EventBlockView<'a>`, the checked view over a `Block`;
- `EventRef<'a>`, a borrowed row made of `&str`, `&[u8]`, and borrowed slices;
- the `Row` and `OwnedRow` implementations used by `RowClient`.

```rust
let block = batch.to_block()?;
let view = EventBlockView::try_from_block(&block)?;

for row in view.iter() {
    println!("{} {}", row.event_id, row.kind);
}

// Fixed-width columns borrow block storage directly.
let ids: &[u64] = view.event_id;
assert_eq!(ids.as_ptr(), block.column_slice_by_name::<u64>("event_id")?.as_ptr());

// Refill a batch without rebuilding its column buffers.
batch.clear();
batch.push_ref(EventRef { event_id: 2, kind: "quote", score: 1.5 });
```

Row attributes cover table metadata, column naming, binary strings, and renamed
runtime crates: `#[row(table = "...")]`, `#[row(rename_all = "...")]`,
`#[row(column = "...")]`, `#[row(bytes)]`, and `#[row(crate_path = "...")]`.
The derive also honors `#[serde(rename_all)]`, `#[serde(rename)]`, and
`#[serde(with = "serde_bytes")]`.

## Three read modes

### Owned rows

Use owned materialization when the row must outlive its backing block.

```rust
let rows: Vec<Event> = client.query_rows("SELECT event_id, kind, score FROM events")?;
let serde_rows: Vec<Metric> = SerdeBridge::block_to_rows(&block)?;
```

### Streaming callbacks

Callbacks keep the backing block alive and avoid an intermediate row vector.

```rust
client.query_views::<Event, _>(sql, |view| {
    for row in view.iter() {
        process(row)?;
    }
    Ok(())
})?;

let mut kinds = Vec::new();
client.query_row_refs::<Event, _>(sql, |row| {
    kinds.push(row.kind.to_owned());
    Ok(())
})?;

let count = client.query_for_each::<Event, _>(sql, |row| {
    consume(row);
    Ok(())
})?;

let decoded = SerdeBridge::from_rowbinary_with::<Metric, _>(&bytes, |row| {
    consume(row);
    Ok(())
})?;

let decoded = SerdeBridge::block_to_rows_with::<Metric, _>(&block, |row| {
    consume(row);
    Ok(())
})?;
```

### Caller-supplied memory

The `_into` and `_into_slice` methods write into buffers the caller already
owns, so a steady-state reader can reuse one allocation.

```rust
let mut ids = vec![0u64; 1024];
let copied = native.read_into_slice(0, &mut ids)?;

let mut payload = Vec::new();
let len = native.read_bytes_into(1, 0, &mut payload)?;

let mut events: Vec<Event> = std::iter::repeat_with(Event::default).take(1024).collect();
let written = client.query_rows_into_slice(sql, &mut events)?;

let mut batch = EventBatch::with_capacity(1024);
let written = client.query_batch_into::<Event>(sql, &mut batch)?;

let mut block = Block::new();
let written = client.query_block_into::<Event>(sql, &mut block)?;

let mut metrics = std::iter::repeat_with(Metric::default).take(1024).collect::<Vec<_>>();
let written = SerdeBridge::from_rowbinary_into_slice(&bytes, &mut metrics)?;
```

`Column::read_into_slice` and `Column::read_bytes_into` provide the same
destination-buffer contract for the pure-Rust row layer.

## Multi-threaded scaling on the calling thread

`Client` is `!Send + !Sync`; share a factory or a worker pool instead of a
client. `WorkerClient` leases `N` slots per calling thread, so every connection
and every callback still runs on that thread.

```rust
use std::thread;
use clickhouse::{Client, ClientConfig};

let config = ClientConfig::parse(
    "tcp://10.0.0.1:9000,tcp://10.0.0.2:9000,tcp://10.0.0.3:9000",
)?.with_workers(8);

let workers = Client::connect_workers(config)?;
thread::scope(|scope| {
    for _ in 0..8 {
        scope.spawn(|| workers.with(|client| client.ping()));
    }
});
```

`ThreadLocalClient` is the one-client-per-thread form:

```rust
let local = Client::thread_local(
    ClientConfig::parse("tcp://10.0.0.1:9000")?.with_workers(4),
)?;

thread::scope(|scope| {
    for _ in 0..4 {
        scope.spawn(|| local.with(|client| client.ping()));
    }
});
```

Each worker slot stripes its endpoint list with
`endpoints[(worker_id + step) % endpoints.len()]`, so concurrent workers start
on different nodes while each slot keeps its own connections.

## BufferPolicy memory governance

`BufferPolicy::default()` retains at most 65,536 rows and 8 MiB per buffer.
`unbounded()` keeps every warmed allocation; `minimal()` releases capacity on
every clear.

```rust
let mut policy = BufferPolicy::default();
policy.max_retained_rows = 32_768;
policy.max_retained_bytes = 4 * 1024 * 1024;

let before = batch.allocated_bytes();
batch.clear_with_policy(policy);
batch.shrink_to_fit();
let after = batch.allocated_bytes();
```

The same policy applies to `Block::clear_with_policy`, `Block::shrink_to_fit`,
and every generated `<Name>Batch`.

## SerdeBridge

`SerdeBridge` accepts structs that implement only `serde::Serialize` and
`serde::Deserialize`.

```rust
use std::collections::BTreeMap;

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Metric {
    ts: u32,
    tags: BTreeMap<String, u64>,
}

let (names, bytes) = SerdeBridge::to_rowbinary(&metrics)?;
let rows: Vec<Metric> = SerdeBridge::from_rowbinary(&bytes)?;

let block = SerdeBridge::rows_to_block(&metrics)?;
let rows: Vec<Metric> = SerdeBridge::block_to_rows(&block)?;
```

`Option<T>` maps to `Nullable`, `Vec<T>` to `Array`, `BTreeMap`/`HashMap` to
`Map(K, V)`, and `serde_bytes` payloads to binary `String`. Logical fixed
columns (`Date`, `Date32`, `DateTime`, `DateTime64`, `IPv4`,
`Decimal32/64/128`) decode into their underlying integer width; `UUID` and
`IPv6` decode into a `serde_bytes` byte field.

## Native TCPS and failover

```rust
use std::time::Duration;
use clickhouse::{
    Client, ClientConfig, EndpointSelection, FailoverPolicy, TlsSettings,
};

let tls = TlsSettings::new()
    .with_ca_pem(std::fs::read("ca.pem")?)
    .with_sni("clickhouse.internal")
    .with_connect_timeout(Duration::from_secs(3))
    .with_read_timeout(Duration::from_secs(5));

let policy = FailoverPolicy::new()
    .with_selection(EndpointSelection::LatencyAware)
    .with_send_retries(2)
    .with_retry_timeout(Duration::from_millis(250))
    .with_circuit(3, Duration::from_secs(30));

let config = ClientConfig::parse("tcps://node-a:9440,tcps://node-b:9440")?
    .with_tls(tls)
    .with_policy(policy)
    .on_failover(|event| {
        eprintln!("{} -> {}", event.old_endpoint, event.new_endpoint);
    });

let mut client = Client::new(config)?;
client.ping()?;
```

The four selection strategies are `RoundRobin`, `Random`, `LatencyAware`, and
`Sticky`. A circuit opens after the configured consecutive failure threshold
and admits a half-open probe after the cooldown. Failover never replays a write
whose server-side outcome is uncertain.

## Column family map

| ClickHouse family | Rust surface | Notes |
| :--- | :--- | :--- |
| Numeric | `u8`..`i128`, `f32`, `f64`, `WireColumn::fixed` | fixed-width `&[T]` |
| Bool | `ChBool`, `bool` | one byte |
| String | `StringColumn`, `String`, `&str`, `Cow<str>` | UTF-8 or binary |
| FixedString(N) | `FixedStringColumn::new(N)` | fixed byte width |
| Date/DateTime/Time | `Date`, `Date32`, `DateTime`, `DateTime64`, `Time`, `Time64` | integer newtypes |
| Decimal | `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256Column` | width follows precision |
| UUID | `Uuid([u8; 16])` | 16 bytes |
| IPv4/IPv6 | `Ipv4(u32)`, `Ipv6([u8; 16])` | fixed width |
| Enum8/Enum16 | `Enum8(i8)`, `Enum16(i16)` | fixed width |
| Nullable(T) | `NullableColumn`, `Option<T>` | null map plus values |
| Array(T) | `ArrayColumn`, `Vec<T>` | flat offsets |
| LowCardinality(T) | `LowCardinalityColumn` | dictionary encoded |
| Tuple(...) | `TupleColumn` | composite columns |
| Map(K, V) | `MapColumn`, `BTreeMap`, `HashMap` | key and value columns |
| Geo | `GeoColumn`, `GeoKind` | Point, Ring, Polygon, MultiPolygon |
| JSON | `StringColumn::new_json()` | JSON column |
| Nothing | `NothingColumn` | empty payload |

## Native column ownership

Column projections borrow their owning column or block without copying. Native
block attachment shares storage initially; mutating an independent owner first
detaches the shared column tree. Extracted column and nested-child handles retain
their original storage across changes to another owner. This copy-on-write step
may allocate. Projection caches remain immutable until an exclusive mutation.
The raw sys mutation methods require `Pin<&mut ColumnHandle>`; shared handles
provide read-only access.

## Native completion-driven I/O

Each calling thread retains one ring and a fixed 1 MiB DMA allocation. Native
TCP and TCPS open nonblocking close-on-exec descriptors and submit connect,
receive, and send SQEs. TLS records use `StreamOwned<ClientConnection,
ProactorStream>` over the same socket adapter. IP literals bypass DNS; hostnames
submit IPv4 and IPv6 lookups through `cares::ProactorResolver` on that ring.
DNS and address attempts share one connection deadline.

Read and write budgets use independent timeout SQEs. When a deadline expires,
the adapter cancels the target and consumes both the target and cancellation
completions before reusing its DMA bytes. A failed drain quarantines the socket
and retains referenced allocations. The synchronous C++ callback ABI still waits
for each operation by advancing the ring; it does not provide a resumable Native
query state machine to application callers.

## HTTP, RowBinary, and Arrow

Enable `http` to compile the synchronous Nexora/rustls transport, LZ4/Zstd body
compression, and HTTP client settings. Shared Block/Batch/View, serde conversion,
Arrow IPC projection, and format codecs remain available in TCP-only and
HTTP-only builds. A disabled transport returns an explicit error before IO.

`Client::connect` accepts one `http://` or `https://` endpoint. Managed
`ClientConfig` endpoint lists, Native TLS settings, and failover policies remain
Native TCP/TCPS configuration; HTTP or mixed lists are rejected. HTTP setters
return `Result` and reject Native clients when both transports are enabled.

Each HTTP transport owns a `nexora::Proactor` and `nexora::HttpClient` on the
calling thread. IP literals are pinned with `resolve_host`; hostnames use
Nexora's non-blocking c-ares resolver within the request deadline.
Requests use the POST method and structured headers. The caller advances the
completion state machine with `submit` and `drive`; no background runtime runs.
Linux with working `io_uring` support is required for HTTP requests.

| `HttpVersionChoice` | Cleartext | TLS |
| --- | --- | --- |
| `Http1` | HTTP/1.1 | HTTP/1.1 |
| `Http2PriorKnowledge` | HTTP/2 prior knowledge | HTTP/2 prior knowledge preference |
| `Http2Tls` | Protocol error | Offers only h2; rejects incompatible TLS peers before POST |
| `Negotiate` | HTTP/1.1 | HTTP/2 ALPN preference, HTTP/1.1 fallback |

New HTTP and HTTPS clients default to `Negotiate`; select `Http2Tls` explicitly
to require HTTP/2 without fallback, including when a peer omits ALPN. `http_preferences()` describes requested options; `negotiated_http_version()`
reports the structured response version. Request deadlines include DNS and connection
establishment; connection attempts are also capped at ten seconds. After a
protocol change, `submit_http` returns a configuration error until the explicit,
synchronous `reset_http()` boundary applies it. Convenience HTTP calls apply
pending configuration synchronously. Destruction drains outstanding socket and
timer completions on the owning thread. Redirects are not
followed, so credentials are never replayed to a redirect destination.

For application-controlled progress, submit raw SQL without waiting for a peer:

```rust
let mut request = client.submit_http(&Query::new("SELECT 1 FORMAT TabSeparated"), &[])?;
while !request.is_complete() {
    client.poll_http()?;
    // Run other application work between bounded completion turns.
}
let body = request.try_take().expect("completed result")?;
assert_eq!(body, b"1\n");
```

`submit_http` owns the SQL and body before returning. `poll_http` advances one
bounded DNS/network turn; it never waits for response bytes. `try_take` consumes
the result once and may perform bounded decompression. `cancel_http(&request)`
terminates an owned request exactly once. Foreign-client cancellation and a
protocol change with pending requests return configuration errors. Dropping the
client terminates outstanding handles; dropping a handle alone does not cancel
its network request. `reset_http()` explicitly cancels all pending handles and
synchronously drains the old driver; keep it outside nonblocking polling loops.
A single request failure never implicitly resets the driver or aborts its sibling
requests. These entry points require an HTTP endpoint; Native query methods retain
their synchronous C++ ABI.

Typed `RowClient` operations encode shared `Block` columns as `FORMAT Native`.
HTTP serde inserts use `Client::serde_insert_rows` with `FORMAT RowBinary`;
`serde_query_rows(&Query)` requests `RowBinaryWithNamesAndTypes` and maps returned
columns to serde fields by name. The block-based trait methods `insert_serde`
and `query_serde` preserve their existing semantics. HTTP Native decoding supports
fixed widths, String, Nullable, Array, and Map; unsupported layouts return errors
rather than silently altering their representation.

`set_compression` selects HTTP LZ4 or Zstd body compression. `Query::compression`
overrides it for one request. Query settings and deduplication tokens are
forwarded to the server; the destination table engine controls deduplication
semantics. Statements and inserts are not automatically replayed on failure.

For raw data, `fetch_bytes(&Query)` collects a response and
`insert_formatted(&Query, &[u8])` sends the format specified by SQL. The raw
callback read APIs accept borrowed receivers and caller-owned destinations:

- `query_raw_with(&Query, callback)` calls `FnMut(&[u8]) -> Result<()>` with decoded
  chunks after the encoded response is collected. A chunk may only be borrowed
  during that invocation. Receiver errors stop delivery and propagate unchanged.
- `query_raw_into(&Query, &mut Vec<u8>)` clears length and reuses existing capacity.
- `query_raw_into_slice(&Query, &mut [u8])` writes from offset zero and preserves
  unused tail bytes. Overflow returns an error; an error may leave a partial
  prefix, which must not be consumed as a complete response.

Raw callback delivery supports identity, LZ4, and Zstd responses. Encoded and
decoded bodies each have a 256 MiB limit. The encoded response is buffered by
Nexora; decompression feeds a reusable 16 KiB stack buffer to the receiver.
Callback delivery therefore does not provide network backpressure or delivery
before the response completes. `select_http_stream(&Query)` instead fully buffers decoded shared
blocks. `http_inserter(table)` stages shared blocks and sends them only at `end`;
dropping it unfinished performs no insertion. These HTTP APIs do not replace
Native `select_stream` and `inserter`, which retain their interactive session,
cleanup, and caller-thread contracts.

`ArrowBlockView::decode_stream` validates and owns decoded Arrow record batches;
IPC parsing can allocate. Primitive slice and string projections borrow the
decoded arrays without copying. Nullable primitive columns require validity-aware
`column` access. Malformed IPC and trailing bytes return errors.

## Examples

The tutorial under `crates/clickhouse/examples/` runs offline by default. Live
sections read `CLICKHOUSE_URL` (`tcp://127.0.0.1:9000`), `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE`.

| Example | Topic |
| :--- | :--- |
| `01_quickstart_native_tcp` | Block and `WireBlock` construction, schema inspection, zero-copy slices, optional Native TCP round-trip. |
| `02_derive_row_and_zero_copy_views` | `#[derive(Row)]`, reusable `Batch`, `BlockView`, `Ref`, and callback/caller-buffer reads. |
| `03_memory_policy_and_batch_reuse` | `BufferPolicy`, `allocated_bytes`, `clear_with_policy`, and `shrink_to_fit`. |
| `04_full_column_type_matrix` | All 17 column families, including wrappers, Geo, JSON, and Nothing. |
| `05_dual_transport_and_row_client` | One `RowClient` surface over an in-memory transport and Native TCP. |
| `06_serde_compatibility` | RowBinary and Block conversion, maps, logical types, and serde attributes. |
| `07_thread_local_and_workers` | Thread-local factories and caller-thread worker slot leasing without sharing client handles. |
| `08_tls_and_failover_policy` | Native TCPS trust, four endpoint policies, observers, and circuit diagnostics. |
| `09_http2_rowbinary_and_arrow` | HTTP protocol selection, serde RowBinary, Arrow IPC projections, and incremental caller-owned response sinks. |

```bash
cargo run -p clickhouse --example 01_quickstart_native_tcp
cargo run -p clickhouse --example 02_derive_row_and_zero_copy_views
cargo run -p clickhouse --example 03_memory_policy_and_batch_reuse
cargo run -p clickhouse --example 04_full_column_type_matrix
cargo run -p clickhouse --example 05_dual_transport_and_row_client
cargo run -p clickhouse --example 06_serde_compatibility
cargo run -p clickhouse --example 07_thread_local_and_workers
cargo run -p clickhouse --example 08_tls_and_failover_policy
cargo run -p clickhouse --features http --example 09_http2_rowbinary_and_arrow
```

The TLS example reads `CLICKHOUSE_TCPS_URLS` for its live section, with
`CLICKHOUSE_CA_PEM` and `CLICKHOUSE_TLS_SNI` for custom trust and identity.

## Testing

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

`cargo test -p clickhouse -p clickhouse-macros` starts
`clickhouse/clickhouse-server:26.8.2.7` on ephemeral Native and HTTP ports,
runs the typed client operations, and removes its container on drop.

## Benchmarks

The three benchmark targets use a fixed-iteration harness with no statistics
framework dependency. Run `cargo bench -p clickhouse --all-features`; set
`CLICKHOUSE_BENCH_ITERATIONS` to a positive count (default 100). `-- --test`
runs one measured iteration per workload as a smoke check. Sonic serializes each
measurement as JSON with mean nanoseconds per iteration and elements per second,
without confidence intervals.

## Build and validation

Run the standard workspace checks from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

The GitHub workflow uses ubuntu-24.04 and the nightly Rust toolchain. Transport
changes also require `cargo test --workspace --all-features` and HTTP-only Clippy
with `cargo clippy -p clickhouse --no-default-features --features http --all-targets -- -D warnings`.
Live tests provision owned ClickHouse Docker fixtures; the Docker daemon must be
available. Default workspace tests cover Native TCP; the all-feature run also
covers the HTTP transport.
