# 04: Streaming `BlockStream`, `Inserter`, `ExternalTable`, and Full `Query` Telemetry

**What to build:** Expose the complete interactive streaming, external data, compression, and query telemetry capabilities of `clickhouse-cpp` through safe, zero-clone Rust abstractions (`BlockStream<'a>`, `Inserter<'a>`, `ExternalTable`, and `Query`).

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] `Client::select_stream(query)` returns an RAII `BlockStream<'a>` (`Iterator<Item = Result<Block>>`) backed by `BeginSelect` / `NextBlock`, automatically invoking `Cancel()` and draining the connection on `Drop` if dropped before exhaustion; `select_cancelable` supports early cancellation via callback.
- [ ] `Client::inserter(query)` returns an RAII `Inserter<'a>` backed by `BeginInsert` / `SendInsertBlock` / `EndInsert`, accepting multiple `Block`s or `Batch`es in a single streaming `INSERT` session and safely resetting connection state if dropped before `end()`.
- [ ] `Client::select_with_external_data` and `select_with_external_data_cancelable` attach in-memory `ExternalTable` (`&str` name + `&Block`) slices to queries for server-side transient joins/filters.
- [ ] `Query` uses `Cow<'a, str>` for SQL, `QueryId`, `QuerySettings` (with `IMPORTANT`, `CUSTOM`, `OBSOLETE` flags), and `QueryParams`, and supports OpenTelemetry `TracingContext`, block compression (`None`, `Lz4`, `Zstd`), server info inspection (`GetServerInfo`, `GetVersion`), and event callbacks (`OnProgress`, `OnProfile`, `OnProfileEvents`, `OnServerLog`, `OnException`).
