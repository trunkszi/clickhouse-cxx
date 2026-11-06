# clickhouse

The `clickhouse` crate is a thread-affine ClickHouse client with completion-driven I/O. It
provides Native TCP/TCPS and HTTP/2 transports, derived row batches, checked
zero-copy block views, a serde bridge, and explicit buffer reuse.

## Execution model

- **Caller-driven**: HTTP offers `submit_http`, bounded `poll_http` turns, owned
  `HttpRequest` result extraction, and explicit cancellation. No background
  executor advances the client.
- **Native compatibility**: Native protocol operations retain the synchronous
  C++ ABI. Socket callbacks advance a persistent thread-local liburing ring;
  nonblocking descriptors and fixed 1 MiB DMA buffers carry plain and TLS bytes.
  c-ares resolves hostnames on that ring under the connection budget.
- **Thread-affine**: `Client`, transport, `Proactor`, and `TlsSession` handles
  are `!Send + !Sync`. Native sockets, blocks, views, and callbacks stay on
  their creating thread; the I/O path uses no blocking locks.
- **`thread_local!` scaling**: `ThreadLocalClient` keeps one client per calling
  OS thread. `WorkerClient` leases lock-free slots `0..N` on the calling thread
  and balances requests across its own connections. No data-plane work crosses
  a thread boundary.
- **Observable failover**: `ClientConfig::on_failover` stores one `FnMut`
  observer; use `Rc<RefCell<_>>` for state shared with that callback.

## Cargo features

| Build | Cargo features | Transports | ClickHouse C++ toolchain |
| :--- | :--- | :--- | :--- |
| Native TCP/TCPS (default) | `default` (`tcp`) | `tcp://`, `tcps://` | `clickhouse-sys` + `clickhouse-cpp` |
| HTTP/2 only | `default-features = false`, `features = ["http"]` | `http://`, `https://` | none |
| Both | `features = ["tcp", "http"]` | all schemes | required |
| Tracing | add `"tracing"` | unchanged | unchanged |

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

`http`-only builds never compile `clickhouse-sys`. HTTP uses Nexora on Linux,
with C build prerequisites for its ring and c-ares bindings. The `tracing` feature emits
only cold control-plane lifecycle, failover, and `OnServerLog` events; hot
data-plane paths emit zero logs. Every dependency in this crate's manifest uses
inline-table syntax.

## Transport configuration and shared blocks

`Client::new` and `ClientBuilder` validate enabled schemes before opening a
connection. `ClientConfig::with_timeout` applies to either transport; positive
submillisecond deadlines round up to one millisecond. HTTP accepts one endpoint
per session and rejects Native-only TLS/socket options and custom failover
policies. HTTP worker factories stripe independent sessions across endpoints.

`insert_block`, `query_blocks`, and the typed row/serde convenience methods are
inherent `Client` methods in both transport builds. They use the pure-Rust
`Block`; Native interactive streaming also exposes `NativeBlock`. Native block
callbacks stream from the socket; HTTP block callbacks run after a bounded
response is decoded. `query_raw_with` receives decoded chunks on the caller
stack after Nexora collects the complete encoded response; it provides neither
network backpressure nor delivery before network completion.
Buffered HTTP responses and their decompressed bodies are capped at 256 MiB.

`try_server_info` returns the Native handshake metadata or an unsupported error
for HTTP. Thread-local handles permit nested calls through different handles;
reentry through the same handle returns a configuration error. Dropping a
handle clears its cached connection on the current thread.

## Nonblocking HTTP requests

`Client::submit_http(&Query, &[u8])` owns a raw request and returns before its
response. Interleave application work with `Client::poll_http()` turns. Once
`HttpRequest::is_complete()` is true, `try_take()` returns the decoded result
once. Result decoding may perform bounded CPU work but does not advance network
I/O. Cancel an owned request with `Client::cancel_http(&request)`; cancellation
through another client is rejected. Client destruction terminates live requests.
After a protocol change, nonblocking admission requires an explicit
`reset_http()` call. That method synchronously retires the old driver and cancels
pending requests; admission and polling do not perform this drain. A stream-local
error leaves other pending requests intact. Native endpoints reject these
HTTP-specific methods.

## Derived rows

```rust
use clickhouse::{Batch, Client, Endpoint, Row};

#[derive(Row, Default)]
#[row(table = "events", rename_all = "snake_case")]
struct Event {
    event_id: u64,
    kind: String,
    score: f64,
}

let endpoint = Endpoint::parse("tcp://127.0.0.1:9000")?;
let mut client = Client::connect(endpoint, "default", "", "default")?;

let mut batch = EventBatch::with_capacity(2);
batch.push(&Event { event_id: 1, kind: String::from("trade"), score: 0.5 });
batch.push_ref(EventRef { event_id: 2, kind: "quote", score: 1.5 });

let block = batch.to_block()?;
let view = EventBlockView::try_from_block(&block)?;
for row in view.iter() {
    println!("{} {}", row.event_id, row.kind);
}

batch.clear();
```

`#[derive(Row)]` generates `<Name>Batch` (struct of arrays), `<Name>BlockView<'a>`
(checked zero-copy view), `<Name>Ref<'a>` (borrowed row), and the `Row` /
`OwnedRow` implementations. `#[row(...)]` supports `table`, `rename_all`,
`column`, `bytes`, and `crate_path`; the derive also honors
`#[serde(rename_all)]`, `#[serde(rename)]`, and `#[serde(with = "serde_bytes")]`.

Fixed-width fields project as `&'a [T]` through `zerocopy`; strings, arrays,
nullable values, and maps borrow the block storage.

## Three read modes

**Owned** rows materialize values that outlive the block:

```rust
let rows: Vec<Event> = client.query_rows(sql)?;
let rows: Vec<Metric> = SerdeBridge::block_to_rows(&block)?;
```

**Callback** forms stream rows while the backing block stays alive:

```rust
client.query_views::<Event, _>(sql, |view| {
    for row in view.iter() {
        process(row)?;
    }
    Ok(())
})?;

client.query_row_refs::<Event, _>(sql, |row| {
    process_ref(row)
})?;

client.query_for_each::<Event, _>(sql, |row| {
    process_owned(row)
})?;

let count = SerdeBridge::from_rowbinary_with::<Metric, _>(&bytes, |row| {
    consume(row);
    Ok(())
})?;
```

**Caller-supplied memory** fills buffers the caller owns:

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
contract for the pure-Rust row layer.

## Multi-threaded scaling

Share a worker pool, not a `Client`. Every slot and callback runs on the thread
that called `with`.

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

`Client::thread_local(config)` provides one client per calling OS thread. Each
worker slot stripes endpoints with
`endpoints[(worker_id + step) % endpoints.len()]`; no request or result crosses
a thread boundary.

## BufferPolicy

`BufferPolicy::default()` retains at most 65,536 rows and 8 MiB per buffer.
`unbounded()` keeps every warmed allocation; `minimal()` releases capacity on
every clear.

```rust
let mut policy = BufferPolicy::default();
policy.max_retained_rows = 32_768;
policy.max_retained_bytes = 4 * 1024 * 1024;

batch.clear_with_policy(policy);
batch.shrink_to_fit();
let retained = batch.allocated_bytes();
```

`Block` exposes the same `clear_with_policy`, `clear_unbounded`,
`shrink_to_fit`, and `allocated_bytes` methods.

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
`Map(K, V)`, and `serde_bytes` to binary `String`. Logical fixed columns
(`Date`, `Date32`, `DateTime`, `DateTime64`, `IPv4`, `Decimal32/64/128`) decode
into their underlying integer width; `UUID` and `IPv6` decode into a
`serde_bytes` byte field.

## Native TCPS and failover

```rust
use std::time::Duration;
use clickhouse::{
    Client, ClientConfig, EndpointSelection, FailoverPolicy, TlsSettings,
};

let tls = TlsSettings::new()
    .with_ca_pem(std::fs::read("ca.pem")?)
    .with_sni("clickhouse.internal")
    .with_connect_timeout(Duration::from_secs(3));

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
```

The strategies are `RoundRobin`, `Random`, `LatencyAware`, and `Sticky`. A
circuit opens after the configured consecutive failures and admits a half-open
probe after the cooldown. Writes are never replayed automatically.

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

| Example | Topic |
| :--- | :--- |
| `01_quickstart_native_tcp` | `Block` and `WireBlock`, schema inspection, zero-copy slices, optional Native TCP round-trip. |
| `02_derive_row_and_zero_copy_views` | `#[derive(Row)]`, `Batch`, `BlockView`, `Ref`, and callback/caller-buffer reads. |
| `03_memory_policy_and_batch_reuse` | `BufferPolicy`, `allocated_bytes`, `clear_with_policy`, `shrink_to_fit`. |
| `04_full_column_type_matrix` | All 17 column families, including wrappers, Geo, JSON, and Nothing. |
| `05_dual_transport_and_row_client` | One `RowClient` surface over an in-memory transport and Native TCP. |
| `06_serde_compatibility` | RowBinary and Block conversion, maps, logical types, and serde attributes. |
| `07_thread_local_and_workers` | Thread-local clients and caller-thread worker slot leasing without transferring client handles. |
| `08_tls_and_failover_policy` | Native TCPS trust, four endpoint policies, observers, and circuit diagnostics. |
| `09_http2_rowbinary_and_arrow` | HTTP protocol selection, RowBinary rows, Arrow projections, and caller-owned response buffers. |

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

Examples run without a server. Live sections read `CLICKHOUSE_URL`,
`CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE`; the TLS
example also reads `CLICKHOUSE_TCPS_URLS`, `CLICKHOUSE_CA_PEM`, and
`CLICKHOUSE_TLS_SNI`.

## Tests

```bash
cargo test -p clickhouse -p clickhouse-macros
```

The test suite starts `clickhouse/clickhouse-server:26.8.2.7` on ephemeral
Native and HTTP ports, exercises the typed client operations, and removes its
container on drop.
