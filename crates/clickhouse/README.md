# Derived synchronous rows

`#[derive(Row)]` generates `EventBatch`, `EventBlockView<'a>`, and `EventRef<'a>`.
`#[row(table = "events", rename_all = "camelCase")]` configures table metadata
and column naming; a field can override its name with `#[row(column = "name")]`.
`#[row(crate_path = "renamed_dependency")]` supports a renamed runtime dependency.

Batches accept either `&Event` or `EventRef`. Strings, binary strings, arrays,
nullable values, and maps append into flat reusable column buffers. An append
copies borrowed values into owned batch storage without cloning the source row
or allocating an owned string for each row. `clear()` retains all capacities.
`to_block(&self)` materializes an independently owned block and therefore copies
column buffers; it does not claim an allocation-free write pipeline.

A checked block view exposes numeric columns as slices into block storage.
String and array fields in `EventRef` borrow that storage. Maps expose an
allocation-free entry iterator, and nullable fields preserve their null flags.
Boolean columns expose validated bytes, while row references decode them as
`bool`; invalid bit patterns are rejected instead of casting arbitrary bytes to
`&[bool]`. View construction, borrowed iteration, and warmed batch refills have
an allocation-counting test.

The row adapters support Rust numeric primitives, bool, String, borrowed str,
Cow strings and bytes, primitive Vec arrays, recursively nullable fields, and
BTreeMap/HashMap fields. Vec<u8> is an Array(UInt8); use `#[row(bytes)]` to select
a binary ClickHouse String. Borrowed byte slices and Cow byte slices select
binary String automatically. Arbitrary type and const generics are rejected;
struct lifetime parameters are supported.

Rows without borrowed input lifetimes also implement `OwnedRow`. `RowClient`
provides `insert_batch`, `insert_rows`, `query_views`, and `query_rows` using only
synchronous `insert_block` and `query_blocks` transport primitives. `query_views`
keeps backing blocks alive during the callback; `query_rows` explicitly
materializes owned values.

`cargo test -p clickhouse -p clickhouse-macros` includes a live Docker test. It
starts `clickhouse/clickhouse-server:26.8.2.7` on ephemeral Native and HTTP ports,
uses a controlled synchronous HTTP RowBinary fixture, verifies all four typed
client operations, and removes its owned container on Drop. This fixture is not
a replacement for integrated Native/HTTP transport tests.

## Examples & Tutorials

The runnable tutorial lives in `examples/`. Every example runs without a
server; examples with a live section read `CLICKHOUSE_URL` (for example
`tcp://127.0.0.1:9000`) and optional `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE` values.

| Example | Topic |
| :--- | :--- |
| `01_quickstart_native_tcp` | `Block` and `WireBlock` construction, schema inspection, zero-copy fixed-width projections, optional `Client` round-trip. |
| `02_derive_row_and_zero_copy_views` | `#[derive(Row)]` attributes, borrowed `Cow`/`Option`/`Vec`/`BTreeMap` fields, generated `Batch`, `BlockView`, and `Ref`. |
| `03_memory_policy_and_batch_reuse` | `clickhouse::BufferPolicy` (`default`, `unbounded`, `minimal`), aggregate `allocated_bytes()`, `clear_with_policy()`, and value-preserving `shrink_to_fit()`. |
| `04_full_column_type_matrix` | All column families from numeric and Bool through Nullable, Array, Tuple, Map, Geo, JSON, and Nothing. |
| `05_dual_transport_and_row_client` | `RowClient` over an in-memory transport and Native TCP, including Bool/Array/Nullable/Map rows. |
| `06_serde_compatibility` | `SerdeBridge` RowBinary and `Block` conversion for serde-only structs including `BTreeMap` fields, `insert_serde`/`query_serde`, and serde naming/bytes attributes in `#[derive(Row)]`. |

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
  `Nullable`, `Vec` becomes `Array`, `BTreeMap`/`HashMap` become `Map(K, V)`,
  and `serde_bytes` payloads become binary `String` columns.
- `SerdeBridge::block_to_rows(&block)` materializes rows from a block by
  matching column names to serde field names. Logical fixed columns (`Date`,
  `Date32`, `DateTime`, `DateTime64`, `IPv4`, `Decimal32/64/128`) decode into
  their underlying integer width, and `UUID`/`IPv6` decode into a
  `serde_bytes` byte field.

`RowClient` exposes the same bridge as `insert_serde(table, &rows)` and
`query_serde::<T>(sql)`, so an in-memory transport and the Native TCP `Client`
share one typed API.

`#[derive(Row)]` also understands the serde naming attributes:
`#[serde(rename_all = "...")]` and `#[serde(rename = "...")]` select column
names, and `#[serde(with = "serde_bytes")]` selects a binary `String` column
instead of `Array(UInt8)`.
