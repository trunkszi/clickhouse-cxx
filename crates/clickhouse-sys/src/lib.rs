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

        // C++ column-matrix handles used by the 17-family type system.

        /// Owned handle around a clickhouse-cpp Block.
        type BlockHandle;
        /// Shared handle around a clickhouse-cpp Column.
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
        fn clear(self: &ColumnHandle);
        /// Reserve matrix column capacity.
        fn reserve(self: &ColumnHandle, capacity: usize);
        /// Matrix column capacity.
        fn capacity(self: &ColumnHandle) -> usize;
        /// Flat fixed-width bytes.
        fn fixed_bytes(self: &ColumnHandle) -> &[u8];
        /// Bytes for one item.
        fn item_bytes(self: &ColumnHandle, index: usize) -> &[u8];
        /// Append fixed-width bytes.
        fn append_fixed(self: &ColumnHandle, bytes: &[u8]) -> Result<()>;
        /// Append another column of the same type.
        fn append_column(self: &ColumnHandle, other: &ColumnHandle) -> Result<()>;
        /// Append one string value.
        fn append_string(self: &ColumnHandle, value: &[u8]) -> Result<()>;
        /// Append one default value.
        fn append_default(self: &ColumnHandle) -> Result<()>;
        /// Borrow one string value.
        fn string_at(self: &ColumnHandle, index: usize) -> &[u8];
        /// FixedString byte width.
        fn fixed_string_size(self: &ColumnHandle) -> usize;
        /// Nested column handle.
        fn nested(self: &ColumnHandle) -> SharedPtr<ColumnHandle>;
        /// Nullable null bitmap.
        fn nulls(self: &ColumnHandle) -> &[u8];
        /// Append a Nullable null flag.
        fn append_nullable_flag(self: &ColumnHandle, is_null: bool) -> Result<()>;
        /// Array offsets.
        fn array_offsets(self: &ColumnHandle) -> &[u64];
        /// Append one Array row.
        fn array_append_row(self: &ColumnHandle, values: &ColumnHandle) -> Result<()>;
        /// Tuple element count.
        fn tuple_len(self: &ColumnHandle) -> usize;
        /// Tuple element column.
        fn tuple_at(self: &ColumnHandle, index: usize) -> SharedPtr<ColumnHandle>;
        /// Append fixed bytes to a tuple element.
        fn tuple_append_fixed(self: &ColumnHandle, index: usize, bytes: &[u8]) -> Result<()>;
        /// Append a string to a tuple element.
        fn tuple_append_string(self: &ColumnHandle, index: usize, value: &[u8]) -> Result<()>;
        /// Append one Map row.
        fn map_append_row(
            self: &ColumnHandle,
            keys: &ColumnHandle,
            values: &ColumnHandle,
        ) -> Result<()>;
        /// Borrow one Map row as a tuple column.
        fn map_row(self: &ColumnHandle, index: usize) -> SharedPtr<ColumnHandle>;
        /// Append a LowCardinality string value.
        fn low_cardinality_append_string(self: &ColumnHandle, value: &[u8]) -> Result<()>;
        /// Append a LowCardinality signed value.
        fn low_cardinality_append_i64(self: &ColumnHandle, value: i64) -> Result<()>;
        /// Append a LowCardinality unsigned value.
        fn low_cardinality_append_u64(self: &ColumnHandle, value: u64) -> Result<()>;
        /// Append one Geo Point.
        fn geo_append_point(self: &ColumnHandle, x: f64, y: f64) -> Result<()>;

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
