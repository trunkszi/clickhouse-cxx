# 02: Complete 17-Family `Column` & `Block` Zero-Copy Type System

**What to build:** Support the entire 17-family `clickhouse-cpp` `Column` type system inside `Block`, providing zero-copy `zerocopy` `&'a [T]` slice projections for all fixed-width column types (including `#[repr(transparent)]` newtypes) and zero-allocation borrowed views / flat `Offsets + Values` builders for variable-length and composite columns, with `.clear()` capacity reuse across all column types.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Fixed-width columns (`UInt8/16/32/64/128`, `Int8/16/32/64/128`, `Float32/64`, `Bool`, `Date`, `Date32`, `DateTime`, `DateTime64(p, tz)`, `Time`, `Time64`, `Decimal32/64/128/256`, `UUID`, `IPv4`, `IPv6`, `FixedString(N)`, `Enum8`, `Enum16`, and user-defined `#[repr(transparent)]` `zerocopy` types) can be appended from contiguous slices and read back from a `Block` as `&'a [T]` via `zerocopy` with zero allocation.
- [ ] Variable-length and composite columns (`String`, `Nullable(T)`, `Array(T)`, `LowCardinality(T)`, `Tuple(...)`, `Map(K, V)`, `Geo` types `Point`/`Ring`/`Polygon`/`MultiPolygon`, `Json`, and `Nothing`) can be constructed, appended, cleared (`.clear()`), and read back through `Block` and `Column`.
- [ ] Every column family is verified by round-trip unit tests and end-to-end queries against ClickHouse.
