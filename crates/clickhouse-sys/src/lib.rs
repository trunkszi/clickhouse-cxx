//! Raw synchronous bindings to `clickhouse-cpp`.
//!
//! The bridge exposes only the surface required by the high-level crate:
//! client lifecycle, ping, DDL execution, block insert, block selection, and
//! fixed-width/String column access. Every call is synchronous and owns no
//! asynchronous runtime.

#[allow(clippy::missing_errors_doc)]
#[cxx::bridge(namespace = "chcxx")]
pub mod ffi {
    unsafe extern "C++" {
        include!("shim.h");

        /// Synchronous Native TCP client handle.
        type Client;
        /// Column-oriented data block owned by the C++ layer.
        type Block;
        /// Owning list of blocks returned by a select query.
        type BlockList;

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
    }
}
