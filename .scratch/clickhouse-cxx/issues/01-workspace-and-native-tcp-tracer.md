# 01: Workspace Foundation & Native TCP Tracer Bullet (`Ping`, `DDL`, Primitive `Block` Round-Trip)

**What to build:** Establish the three-crate Cargo workspace (`clickhouse-sys`, `clickhouse-macros`, `clickhouse`) enforcing strict synchronous, zero-clone, and `<= 800`-line rules (`[workspace.lints]` aligned with `questra`/`memora`), automatically fetch and statically compile `clickhouse-cpp` (`CH_USE_3X_API=ON`, `WITH_OPENSSL=OFF`) in `clickhouse-sys/build.rs`, and deliver a working `clickhouse::Client` over Native TCP (`tcp://`) capable of `ping`, `execute` (DDL), and inserting/selecting a primitive `Block` with `zerocopy` `&'a [T]` column slice projection and `.clear()` buffer reuse.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Root `Cargo.toml`, `rust-toolchain.toml` (nightly, edition 2024), and `rustfmt.toml` are configured with strict `deny` lints (`clone_on_copy`, `redundant_clone`, `implicit_clone`, `unnecessary_to_owned`, `cloned_instead_of_copied`, `inefficient_to_string`, `undocumented_unsafe_blocks`, `missing_safety_doc`, `missing_errors_doc`, `missing_panics_doc`) and zero async dependencies.
- [ ] `clickhouse-sys/build.rs` automatically resolves `CLICKHOUSE_CPP_DIR` or clones `https://github.com/ClickHouse/clickhouse-cpp.git` (`--depth 1 --recurse-submodules`), builds it statically via CMake with `-DCH_USE_3X_API=ON` and `-DWITH_OPENSSL=OFF`, and links `clickhouse-cpp-lib-static`, `cityhash`, `lz4`, `zstd`, and required `abseil` archives statically.
- [ ] `clickhouse::Error` (built with `thiserror` and `Cow<'static, str>` metadata) and optional `tracing` feature (`features = ["tracing"]`, off by default, restricted to cold control paths) are in place.
- [ ] `clickhouse::Client` connects to a `tcp://` `Endpoint`, executes `ping()` and `execute()` (DDL), inserts a `Block` containing fixed-width numeric and `String` columns, queries it back, projects fixed-width columns as `&'a [T]` slices via `zerocopy`, and supports `Block::clear()` without releasing heap capacity.
- [ ] Automated synchronous Docker test fixture (`TestClickHouseContainer` using `clickhouse/clickhouse-server:26.8.2.7`) and in-memory unit tests verify the end-to-end tracer bullet.
