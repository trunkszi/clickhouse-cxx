# clickhouse-cxx

A synchronous, thread-affine ClickHouse client SDK for Rust. Native TCP and
TCPS use [`clickhouse-cpp`](https://github.com/ClickHouse/clickhouse-cpp)
through [`cxx`](https://cxx.rs/); HTTP and HTTPS use
[`curl-rust`](https://github.com/trunkszi/curl-rust) with HTTP/2 and `rustls`.
Typed rows use `#[derive(Row)]`; serde-only structs use `SerdeBridge`.

## Design

- **Synchronous**: every call blocks on the calling thread. `tokio`, `futures`,
  `async-std`, and `smol` never enter the dependency graph.
- **Thread-affine**: `Client`, transport, `EventLoop`, and `TlsSession` handles
  are `!Send + !Sync`. Sockets, blocks, views, and callbacks stay on the thread
  that created them, and the I/O path takes no mutex.
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

| Build | Cargo features | Transports | Native toolchain |
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

`http` pulls `curl` from the `feat/event-loop-ws` branch of
`https://github.com/trunkszi/curl-rust.git` with `http2` and `rustls`, plus
`lz4_flex` and `zstd`. `tcp` pulls `clickhouse-sys` and `cxx`;
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
| `07_streaming_and_callbacks` | Reserved walkthrough for the streaming and caller-buffer APIs exercised by examples 02 and 06. |
| `08_tls_and_failover_policy` | Native TCPS trust, four endpoint policies, observers, and circuit diagnostics. |

```bash
cargo run -p clickhouse --example 01_quickstart_native_tcp
cargo run -p clickhouse --example 02_derive_row_and_zero_copy_views
cargo run -p clickhouse --example 03_memory_policy_and_batch_reuse
cargo run -p clickhouse --example 04_full_column_type_matrix
cargo run -p clickhouse --example 05_dual_transport_and_row_client
cargo run -p clickhouse --example 06_serde_compatibility
cargo run -p clickhouse --example 08_tls_and_failover_policy
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
