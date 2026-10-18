# clickhouse-cxx

A general-purpose, purely synchronous, high-performance ClickHouse client SDK for Rust, powered by `cxx` (statically linking `clickhouse-cpp` for Native TCP) and `curl-rust` (with `rustls` and `http2` for HTTP/HTTPS).

## Coding & Architecture Standards

- **Zero Async Runtime**: The entire workspace is strictly synchronous. `tokio`, `futures`, `async-std`, and `smol` are forbidden.
- **Zero-Clone & `Cow` First**: Prefer borrowing (`&'a T`, `&'a str`, `&'a [u8]`) and `std::borrow::Cow<'a, str>` / `Cow<'a, [u8]>` for configuration, SQL queries, table names, parameters, and error metadata. All clippy clone/to_owned lints (`clone_on_copy`, `redundant_clone`, `implicit_clone`, `unnecessary_to_owned`, `cloned_instead_of_copied`, `inefficient_to_string`) are enforced as `deny`.
- **Buffer Reuse (`clear`)**: All `Batch` and `Block` buffers provide `.clear()` to reset lengths to zero while retaining allocated heap capacity across ingestion cycles.
- **File Size Limit**: Every `.rs`, `.cpp`, and `.h` file must stay `<= 800` lines and contain zero chain-of-thought or session residue.
- **Unsafe & Documentation**: Every `unsafe` block requires a `// SAFETY:` comment (`undocumented_unsafe_blocks = "deny"`), every `unsafe fn` requires `# Safety`, and every fallible/panicking public function requires `# Errors` / `# Panics` doc sections. `README.md`, source comments, rustdoc, error messages, and log events are all written in English.
- **Git & Branch Discipline**: Every feature/ticket is developed in an isolated git worktree on a dedicated feature branch and merged into `main` strictly via rebase (`allow_rebase_merge = true`, `allow_merge_commit = false`, `allow_squash_merge = false`). Direct commits to `main` are forbidden once branch protection is active.
- **Optional `tracing` Telemetry**: `tracing` is an optional cargo feature (`features = ["tracing"]`). When enabled, logs are emitted strictly on cold control-plane paths (connection lifecycle, endpoint failover episodes, and server `OnServerLog` events). Hot data-plane paths (`Batch` append, `BlockView` zero-copy slice projection, `RowBinary` serialization) emit zero logs; when the `tracing` feature is disabled, internal telemetry macros compile to no-ops.

## Agent skills

### Issue tracker

Issues and specs live as local markdown files under `.scratch/<feature-slug>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the five default canonical triage labels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` and `docs/adr/` at the repo root). See `docs/agents/domain.md`.
