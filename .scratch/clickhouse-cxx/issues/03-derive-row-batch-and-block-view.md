# 03: `#[derive(Row)]` Procedural Macro (`Batch`, `BlockView<'a>`, and `RowRef<'a>`)

**What to build:** Implement the unified `#[derive(Row)]` procedural macro and `#[row(...)]` attributes in `clickhouse-macros` (re-exported by `clickhouse`) so that annotating a Rust struct automatically implements `Row` and generates its Struct-of-Arrays `<Name>Batch` write buffer, zero-copy `<Name>BlockView<'a>` column projection, and zero-allocation borrowed `<Name>Ref<'a>` (`RowRef`).

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Annotating a struct with `#[derive(Row)]` (and optional `#[row(table = "...", rename_all = "...", column = "...")]` attributes parsed via `darling`/`syn`/`quote`/`heck`) implements `clickhouse::Row` without any `ClickHouse` prefix in the macro or attribute names.
- [ ] The generated `<Name>Batch` stores columns in contiguous SoA buffers (`Vec<T>` for fixed-width types and flat `Offsets + Values` for variable-length types), accepts borrowed strings/slices (`&str`, `Cow<'a, str>`, `&[u8]`) without cloning, converts directly into a `Block`, and provides `.clear()` to retain heap capacity across ingestion cycles.
- [ ] The generated `<Name>BlockView<'a>` projects fixed-width columns directly as `&'a [T]` slices via `zerocopy` and provides `.get(row_idx)` / `.iter()` yielding `<Name>Ref<'a>` (`RowRef`) where variable-length fields (`&'a str`, `&'a [u8]`, `&'a [T]`) borrow directly from the `Block`.
- [ ] `Client::insert_batch`, `Client::insert_rows`, and `Client::query_rows` work end-to-end with `#[derive(Row)]` types.
