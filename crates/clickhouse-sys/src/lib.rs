//! Raw synchronous bindings to `clickhouse-cpp`.
//!
//! The bridge exposes only the surface required by the high-level crate:
//! client lifecycle, ping, DDL execution, block insert, block selection, and
//! fixed-width/String column access. Every call is synchronous and owns no
//! asynchronous runtime.
//! Column storage cannot be changed through a shared handle while bytes are borrowed:
//! ```compile_fail
//! let column = clickhouse_sys::new_column("UInt64").unwrap();
//! let bytes = column.fixed_bytes();
//! column.reserve(1024);
//! assert!(!bytes.is_empty());
//! ```
//! Exclusive mutation cannot overlap a byte projection:
//! ```compile_fail,E0502
//! let mut column = clickhouse_sys::new_column("UInt64").unwrap();
//! column.pin_mut().append_fixed(&7_u64.to_ne_bytes()).unwrap();
//! let bytes = column.fixed_bytes();
//! column.pin_mut().reserve(1024);
//! assert_eq!(bytes.len(), 8);
//! ```
//!
//! Mutation followed by immutable projection is supported:
//! ```
//! let mut column = clickhouse_sys::new_column("UInt64").unwrap();
//! column.pin_mut().append_fixed(&7_u64.to_ne_bytes()).unwrap();
//! column.pin_mut().reserve(1024);
//! assert_eq!(column.fixed_bytes(), 7_u64.to_ne_bytes());
//! ```

#[allow(clippy::missing_errors_doc)]
#[cxx::bridge(namespace = "chcxx")]
pub mod ffi {
    // C++ handles retain their buffers for every Rust borrow.
    /// Scalar server event fields shared across the language boundary.
    #[derive(Default)]
    struct EventFields {
        rows: u64,
        bytes: u64,
        total_rows: u64,
        written_rows: u64,
        written_bytes: u64,
        blocks: u64,
        rows_before_limit: u64,
        applied_limit: bool,
        calculated_rows_before_limit: bool,
    }
    struct ExceptionFields {
        code: i32,
        name: String,
        display_text: String,
        stack_trace: String,
    }
    struct ServerInfo {
        name: String,
        timezone: String,
        display_name: String,
        version_major: u64,
        version_minor: u64,
        version_patch: u64,
        revision: u64,
    }
    struct Version {
        major: u16,
        minor: u16,
        patch: u16,
        build: u16,
        extra: String,
    }

    // SAFETY: Handles retain borrowed storage. Exclusive mutations detach shared
    // column trees; immutable byte projections keep their storage and caches stable.
    unsafe extern "C++" {
        include!("shim.h");

        /// Synchronous Native TCP client handle.
        type Client;
        /// Column-oriented data block owned by the C++ layer.
        type Block;
        /// Owning list of blocks returned by a select query.
        type BlockList;
        /// Query metadata retained for one synchronous request.
        type NativeQuery;
        /// Server event delivered to a stack-scoped callback.
        type NativeEvent;

        /// Open a Native TCP connection.
        fn new_client(
            host: &str,
            port: u16,
            user: &str,
            password: &str,
            database: &str,
        ) -> Result<UniquePtr<Client>>;

        /// Ping the server.
        fn ping(client: Pin<&mut Client>) -> Result<()>;

        /// Reconnect by walking the configured endpoint list.
        fn reset_connection_endpoint(client: Pin<&mut Client>) -> Result<()>;
        /// Current connected endpoint host.
        fn current_host(client: &Client) -> Result<String>;
        /// Current connected endpoint port.
        fn current_port(client: &Client) -> Result<u16>;

        /// Execute a statement such as a DDL query.
        fn execute(client: Pin<&mut Client>, sql: &str) -> Result<()>;

        /// Insert one block into a table.
        fn insert_block(client: Pin<&mut Client>, table: &str, block: &Block) -> Result<()>;

        /// Execute a select and collect every returned block.
        fn query_blocks(client: Pin<&mut Client>, sql: &str) -> Result<UniquePtr<BlockList>>;

        /// Number of blocks collected by a select.
        fn block_list_len(list: &BlockList) -> usize;

        /// Clone one collected block into an owned handle.
        fn block_list_get(list: &BlockList, index: usize) -> UniquePtr<Block>;

        /// Create an empty block.
        fn block_new() -> UniquePtr<Block>;

        /// Append a `UInt8` column.
        fn block_append_u8(block: Pin<&mut Block>, name: &str, values: &[u8]) -> Result<()>;
        /// Append a `UInt16` column.
        fn block_append_u16(block: Pin<&mut Block>, name: &str, values: &[u16]) -> Result<()>;
        /// Append a `UInt32` column.
        fn block_append_u32(block: Pin<&mut Block>, name: &str, values: &[u32]) -> Result<()>;
        /// Append a `UInt64` column.
        fn block_append_u64(block: Pin<&mut Block>, name: &str, values: &[u64]) -> Result<()>;
        /// Append an `Int8` column.
        fn block_append_i8(block: Pin<&mut Block>, name: &str, values: &[i8]) -> Result<()>;
        /// Append an `Int16` column.
        fn block_append_i16(block: Pin<&mut Block>, name: &str, values: &[i16]) -> Result<()>;
        /// Append an `Int32` column.
        fn block_append_i32(block: Pin<&mut Block>, name: &str, values: &[i32]) -> Result<()>;
        /// Append an `Int64` column.
        fn block_append_i64(block: Pin<&mut Block>, name: &str, values: &[i64]) -> Result<()>;
        /// Append a `Float32` column.
        fn block_append_f32(block: Pin<&mut Block>, name: &str, values: &[f32]) -> Result<()>;
        /// Append a `Float64` column.
        fn block_append_f64(block: Pin<&mut Block>, name: &str, values: &[f64]) -> Result<()>;
        /// Append a `String` column.
        fn block_append_string(block: Pin<&mut Block>, name: &str, values: &[String])
        -> Result<()>;

        /// Append a `String` column from validated flat offsets and bytes.
        fn block_append_string_flat(
            block: Pin<&mut Block>,
            name: &str,
            offsets: &[u64],
            bytes: &[u8],
        ) -> Result<()>;

        /// Drop every row while keeping column capacity.
        fn block_clear(block: Pin<&mut Block>);

        /// Recompute the C++ row count from the filled columns.
        fn block_refresh(block: Pin<&mut Block>) -> Result<()>;

        /// Number of rows in the block.
        fn block_rows(block: &Block) -> usize;

        /// Number of columns in the block.
        fn block_columns(block: &Block) -> usize;

        /// Name of a column by index.
        fn block_column_name(block: &Block, index: usize) -> String;

        /// Stable column-kind tag used by the Rust projection.
        fn block_column_kind(block: &Block, index: usize) -> u32;

        /// Raw little-endian column bytes for fixed-width kinds.
        fn block_column_bytes<'a>(block: &'a Block, index: usize) -> &'a [u8];

        /// Materialized strings for a `String` column.
        fn block_column_strings(block: &Block, index: usize) -> Vec<String>;

        /// Exclusive end offsets for a `String` column.
        fn block_column_string_offsets(block: &Block, index: usize) -> Vec<u64>;

        /// Flat bytes for a `String` column.
        fn block_column_string_flat(block: &Block, index: usize) -> Vec<u8>;

        /// Retained element capacity for fixed-width columns.
        fn block_column_capacity(block: &Block, index: usize) -> usize;

        /// Server type name of a column by index.
        fn block_column_type(block: &Block, index: usize) -> Result<String>;

        /// Open a Native TCP connection with compression and timeout options.
        fn new_client_options(
            host: &str,
            port: u16,
            user: &str,
            password: &str,
            database: &str,
            compression: u8,
            timeout_ms: u64,
        ) -> Result<UniquePtr<Client>>;

        /// Reset the underlying connection.
        fn reset(client: Pin<&mut Client>) -> Result<()>;

        /// Create one query with caller-supplied identity.
        fn new_query(sql: &str, id: &str) -> UniquePtr<NativeQuery>;
        /// Attach one flagged setting.
        fn set_setting(query: Pin<&mut NativeQuery>, key: &str, value: &str, flags: u64);
        /// Attach one typed placeholder value; `is_null` distinguishes NULL.
        fn set_param(query: Pin<&mut NativeQuery>, key: &str, value: &str, is_null: bool);
        /// Attach an OpenTelemetry tracing context.
        fn set_tracing(
            query: Pin<&mut NativeQuery>,
            high: u64,
            low: u64,
            span: u64,
            state: &str,
            flags: u8,
        );

        /// # Safety
        /// The callback context must remain valid throughout this synchronous call.
        unsafe fn begin_select(
            client: Pin<&mut Client>,
            query: Pin<&mut NativeQuery>,
            events: u8,
            context: *mut u8,
            callback: unsafe fn(*mut u8, &NativeEvent) -> bool,
        ) -> Result<()>;
        /// # Safety
        /// The callback context must remain valid throughout this synchronous call.
        unsafe fn next_block(
            client: Pin<&mut Client>,
            context: *mut u8,
            callback: unsafe fn(*mut u8, &NativeEvent) -> bool,
        ) -> Result<UniquePtr<Block>>;
        /// # Safety
        /// The callback context must remain valid throughout this synchronous call.
        unsafe fn cancel(
            client: Pin<&mut Client>,
            context: *mut u8,
            callback: unsafe fn(*mut u8, &NativeEvent) -> bool,
        ) -> Result<()>;
        /// Whether a select still needs output or cancellation cleanup.
        fn is_selecting(client: &Client) -> bool;
        /// Begin one interactive insert and return its server schema.
        fn begin_insert(client: Pin<&mut Client>, query: &NativeQuery) -> Result<UniquePtr<Block>>;
        /// Send one block inside the active insert statement.
        fn send_insert_block(client: Pin<&mut Client>, block: &Block) -> Result<()>;
        /// Finish the active insert statement.
        fn end_insert(client: Pin<&mut Client>) -> Result<()>;
        /// Take structured server details from a failed interactive INSERT operation.
        fn take_insert_exception(client: Pin<&mut Client>) -> UniquePtr<NativeEvent>;
        /// Insert a complete block while retaining typed server rejection details.
        fn insert_block_checked(client: Pin<&mut Client>, table: &str, block: &Block)
        -> Result<()>;
        /// Whether the server still expects insert blocks.
        fn is_inserting(client: &Client) -> bool;
        /// Register one named in-memory table for the next select.
        fn add_external(client: Pin<&mut Client>, name: &str, block: &Block);
        /// # Safety
        /// The callback context and every registered external table name
        /// and block must remain valid throughout this synchronous call.
        unsafe fn select_external(
            client: Pin<&mut Client>,
            query: Pin<&mut NativeQuery>,
            events: u8,
            context: *mut u8,
            callback: unsafe fn(*mut u8, &NativeEvent) -> bool,
        ) -> Result<()>;
        /// Drop every registered external table.
        fn clear_external(client: Pin<&mut Client>);
        /// Handshake information reported by the connected server.
        fn get_server_info(client: &Client) -> ServerInfo;
        /// Statically linked `clickhouse-cpp` version.
        fn get_version() -> Version;

        /// Event kind tag: data, progress, profile, profile events, log, exception.
        fn event_kind(event: &NativeEvent) -> u8;
        /// Scalar fields for progress and profile events.
        fn event_fields(event: &NativeEvent) -> EventFields;
        /// Structured server exception fields.
        fn event_exception(event: &NativeEvent) -> ExceptionFields;
        /// Copy the event's block into an owned handle.
        fn event_block(event: &NativeEvent) -> UniquePtr<Block>;

        // C++ column-matrix handles used by the 17-family type system.

        /// Owned handle around a clickhouse-cpp Block.
        type BlockHandle;
        /// Column storage retained by its handle, with copy-on-write for independent aliases.
        /// Mutations require exclusive pinned borrows; shared handles expose immutable views.
        type ColumnHandle;
        /// Synchronous client used by the direct column matrix tests.
        type ClientHandle;

        /// Create an empty matrix block handle.
        fn new_block_handle() -> UniquePtr<BlockHandle>;
        /// Create a matrix column from a ClickHouse type name.
        fn new_column_handle(type_name: &str) -> Result<UniquePtr<ColumnHandle>>;
        /// Open a direct matrix client.
        fn new_column_client(host: &str, port: u16) -> Result<UniquePtr<ClientHandle>>;

        /// Append a matrix column to a native block.
        fn block_append_column_handle(
            block: Pin<&mut Block>,
            name: &str,
            column: &ColumnHandle,
        ) -> Result<()>;
        /// Borrow a native block column as a matrix handle.
        fn block_column_handle(block: &Block, index: usize) -> SharedPtr<ColumnHandle>;

        /// Append a named matrix column.
        fn append_column(
            self: Pin<&mut BlockHandle>,
            name: &str,
            column: &ColumnHandle,
        ) -> Result<()>;
        /// Number of matrix columns.
        fn column_count(self: &BlockHandle) -> usize;
        /// Number of matrix rows.
        fn row_count(self: &BlockHandle) -> usize;
        /// Column name by index.
        fn column_name(self: &BlockHandle, index: usize) -> Result<String>;
        /// Column type code by index.
        fn column_type_code(self: &BlockHandle, index: usize) -> Result<i32>;
        /// Column type name by index.
        fn column_type_name(self: &BlockHandle, index: usize) -> Result<String>;
        /// Borrow a matrix column by index.
        fn column(self: &BlockHandle, index: usize) -> Result<SharedPtr<ColumnHandle>>;
        /// Clear matrix rows.
        fn clear(self: Pin<&mut BlockHandle>);
        /// Reserve matrix capacity.
        fn reserve(self: Pin<&mut BlockHandle>, capacity: usize);

        /// Number of rows in a matrix column.
        fn size(self: &ColumnHandle) -> usize;
        /// Matrix column type code.
        fn type_code(self: &ColumnHandle) -> i32;
        /// Matrix column type name.
        fn type_name(self: &ColumnHandle) -> String;
        /// Clear a matrix column.
        fn clear(self: Pin<&mut ColumnHandle>);
        /// Reserve matrix column capacity.
        fn reserve(self: Pin<&mut ColumnHandle>, capacity: usize);
        /// Matrix column capacity.
        fn capacity(self: &ColumnHandle) -> usize;
        /// Flat fixed-width bytes.
        fn fixed_bytes(self: &ColumnHandle) -> &[u8];
        /// Bytes for one item.
        fn item_bytes(self: &ColumnHandle, index: usize) -> &[u8];
        /// Append fixed-width bytes.
        fn append_fixed(self: Pin<&mut ColumnHandle>, bytes: &[u8]) -> Result<()>;
        /// Append another column of the same type.
        fn append_column(self: Pin<&mut ColumnHandle>, other: &ColumnHandle) -> Result<()>;
        /// Append one string value.
        fn append_string(self: Pin<&mut ColumnHandle>, value: &[u8]) -> Result<()>;
        /// Append one default value.
        fn append_default(self: Pin<&mut ColumnHandle>) -> Result<()>;
        /// Borrow one string value.
        fn string_at(self: &ColumnHandle, index: usize) -> &[u8];
        /// FixedString byte width.
        fn fixed_string_size(self: &ColumnHandle) -> usize;
        /// Nested column handle.
        fn nested(self: &ColumnHandle) -> SharedPtr<ColumnHandle>;
        /// Nullable null bitmap.
        fn nulls(self: &ColumnHandle) -> &[u8];
        /// Append a Nullable null flag.
        fn append_nullable_flag(self: Pin<&mut ColumnHandle>, is_null: bool) -> Result<()>;
        /// Array offsets.
        fn array_offsets(self: &ColumnHandle) -> &[u64];
        /// Append one Array row.
        fn array_append_row(self: Pin<&mut ColumnHandle>, values: &ColumnHandle) -> Result<()>;
        /// Tuple element count.
        fn tuple_len(self: &ColumnHandle) -> usize;
        /// Tuple element column.
        fn tuple_at(self: &ColumnHandle, index: usize) -> SharedPtr<ColumnHandle>;
        /// Append fixed bytes to a tuple element.
        fn tuple_append_fixed(
            self: Pin<&mut ColumnHandle>,
            index: usize,
            bytes: &[u8],
        ) -> Result<()>;
        /// Append a string to a tuple element.
        fn tuple_append_string(
            self: Pin<&mut ColumnHandle>,
            index: usize,
            value: &[u8],
        ) -> Result<()>;
        /// Append one Map row.
        fn map_append_row(
            self: Pin<&mut ColumnHandle>,
            keys: &ColumnHandle,
            values: &ColumnHandle,
        ) -> Result<()>;
        /// Borrow one Map row as a tuple column.
        fn map_row(self: &ColumnHandle, index: usize) -> SharedPtr<ColumnHandle>;
        /// Append a LowCardinality string value.
        fn low_cardinality_append_string(self: Pin<&mut ColumnHandle>, value: &[u8]) -> Result<()>;
        /// Append a LowCardinality signed value.
        fn low_cardinality_append_i64(self: Pin<&mut ColumnHandle>, value: i64) -> Result<()>;
        /// Append a LowCardinality unsigned value.
        fn low_cardinality_append_u64(self: Pin<&mut ColumnHandle>, value: u64) -> Result<()>;
        /// Append one Geo Point.
        fn geo_append_point(self: Pin<&mut ColumnHandle>, x: f64, y: f64) -> Result<()>;

        /// Append fixed-width data to the Nullable child under its parent's exclusive borrow.
        fn nullable_append_fixed(self: Pin<&mut ColumnHandle>, bytes: &[u8]) -> Result<()>;
        /// Append a string to the Nullable child under its parent's exclusive borrow.
        fn nullable_append_string(self: Pin<&mut ColumnHandle>, value: &[u8]) -> Result<()>;
        /// Append a column to the Nullable child under its parent's exclusive borrow.
        fn nullable_append_column(self: Pin<&mut ColumnHandle>, other: &ColumnHandle)
        -> Result<()>;
        /// Append a default to the Nullable child under its parent's exclusive borrow.
        fn nullable_append_default(self: Pin<&mut ColumnHandle>) -> Result<()>;

        /// Ping through a matrix client.
        fn ping(self: Pin<&mut ClientHandle>) -> Result<()>;
        /// Execute through a matrix client.
        fn execute(self: Pin<&mut ClientHandle>, query: &str) -> Result<()>;
        /// Insert a matrix block.
        fn insert(self: Pin<&mut ClientHandle>, table: &str, block: &BlockHandle) -> Result<()>;
        /// Select the first non-empty matrix block.
        fn select_one(self: Pin<&mut ClientHandle>, query: &str) -> Result<UniquePtr<BlockHandle>>;
    }
}

pub use ffi::{
    BlockHandle, ClientHandle, ColumnHandle, new_block_handle as new_block,
    new_column_client as new_client, new_column_handle as new_column,
};

mod bridge;
mod session;
pub use bridge::{EchoParams, NativeOptions, factory_echo, new_native_client};
pub use session::{ConnectParams, TlsSession, connect, wait_retry};
