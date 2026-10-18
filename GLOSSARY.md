# clickhouse-cxx

A general-purpose, purely synchronous, high-performance ClickHouse client SDK for Rust, unifying the ClickHouse Native TCP protocol (`clickhouse-cpp` via `cxx`) and HTTP/HTTPS protocol (`curl-rust` with `rustls`) behind a single zero-copy-capable interface.

## Language

### Connectivity & Transport

**Client**:
The unified, purely synchronous entry point for interacting with ClickHouse clusters, dispatching operations across Native TCP or HTTP transports based on the configured endpoint scheme (`tcp://`, `tcps://`, `http://`, `https://`).
_Avoid_: NativeClient, HttpClient, AsyncClient, Connection

**Transport**:
The underlying wire protocol implementation (`Native` via `clickhouse-cpp` or `Http` via `curl-rust` with `http2` enabled by default) encapsulated behind `Client`.
_Avoid_: Driver, Backend, Adapter

**Endpoint**:
A single ClickHouse server address combining a protocol scheme, host, and port (e.g., `tcp://127.0.0.1:9000` or `http://127.0.0.1:8123`).
_Avoid_: Url, Host, Peer, Node

**FailoverPolicy**:
The high-availability configuration shared across both `Native` and `Http` transports, governing round-robin multi-endpoint rotation, `send_retries`, `retry_timeout`, `ping_before_query`, and `FailoverEvent` notifications.
_Avoid_: RetryStrategy, LoadBalancer, ClusterPool

### Data Model & Zero-Copy

**Block**:
A column-oriented chunk of ClickHouse data (matching ClickHouse's `Native` wire format and `clickhouse::Block` in C++), where each column's values are stored contiguously in memory.
_Avoid_: Chunk, RecordBatch, DataFrame, Table

**Batch**:
A generated Rust Struct-of-Arrays (SoA) write buffer (`<Name>Batch`, derived by `#[derive(Row)]`), accumulating columns in contiguous `Vec<T>` or flat `Offsets + Bytes` buffers before flushing as a `Block`.
_Avoid_: Inserter, RowBuffer, BatchBuffer

**BlockView**:
A lifetime-bound (`'a`), zero-copy read projection (`<Name>BlockView<'a>`, derived by `#[derive(Row)]`) over a `Block` that exposes fixed-width columns directly as `&'a [T]` slices via `zerocopy`.
_Avoid_: BlockReader, CursorView, SliceAdapter

**Row**:
The core trait and derive macro (`#[derive(Row)]`) for a user-defined Rust struct representing a ClickHouse table schema, simultaneously generating `<Name>Batch`, `<Name>BlockView<'a>`, and `<Name>Ref<'a>`.
_Avoid_: ClickHouseRow,ClickHouseBlock, Record, Entity, Model

**RowRef**:
A lifetime-bound (`'a`), zero-allocation borrowed view of a single row (`<Name>Ref<'a>`) indexed from a `BlockView<'a>`, where variable-length fields (`&'a str`, `&'a [u8]`, `&'a [T]`) borrow directly from the underlying `Block` memory.
_Avoid_: BorrowedRow, RowSlice, RowCursorItem

**Column**:
A single typed column inside a `Block`, supporting the complete `clickhouse-cpp` column type system (`Numeric`, `Bool`, `String`/`FixedString`, `Date`/`Date32`/`DateTime`/`DateTime64`, `Time`/`Time64`, `Decimal`, `UUID`, `IPv4`/`IPv6`, `Enum8`/`Enum16`, `Nullable`, `Array`, `LowCardinality`, `Tuple`, `Map`, `Geo` types, `Json`, and `Nothing`).
_Avoid_: Field, Series, ArrayColumn

### Streaming & Query Execution

**Query**:
A configurable request descriptor encapsulating SQL text, `QueryId`, per-query `QuerySettings` (`IMPORTANT`/`CUSTOM`/`OBSOLETE` flags), parameterized `QueryParams`, OpenTelemetry `TracingContext`, and server event callbacks (`OnProgress`, `OnProfile`, `OnProfileEvents`, `OnServerLog`, `OnException`).
_Avoid_: Statement, Command, SqlRequest

**BlockStream**:
An RAII synchronous iterator (`Iterator<Item = Result<Block>>`) holding `&'a mut Client` that drives streaming query execution (`BeginSelect` / `NextBlock` over Native TCP, or chunked streaming over HTTP) and automatically cancels/drains the in-flight query on `Drop` if terminated early.
_Avoid_: RowCursor, SelectIterator, AsyncStream

**Inserter**:
An RAII synchronous streaming write session holding `&'a mut Client` (`BeginInsert` / `SendInsertBlock` / `EndInsert` over Native TCP, or streaming body upload over HTTP) that accepts multiple `Block`s, `Batch`es, or `Row` slices within a single `INSERT` statement and safely resets the connection if dropped before `end()`.
_Avoid_: BatchWriter, Sink, WriteStream

**ExternalTable**:
An in-memory named `Block` attached to a `Query` (`SelectWithExternalData`) so the ClickHouse server can join or filter against transient client-side tables during query execution without persisting them.
_Avoid_: TempTable, EphemeralTable

**SerdeBridge**:
The dual-path adapter allowing any Rust type implementing `serde::Serialize` / `serde::Deserialize` (even without `#[derive(Row)]`) to be read and written over both `Http` (via `FORMAT RowBinary`) and `Native` TCP (by dynamically dispatching `serde` fields into/from the server-provided schema `Block`).
_Avoid_: JsonAdapter, RowSerializer





