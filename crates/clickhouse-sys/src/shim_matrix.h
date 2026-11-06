#pragma once

#include "rust/cxx.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clickhouse/block.h"
#include "clickhouse/client.h"

namespace chcxx {

/// Owned handle around a clickhouse-cpp Block.
class BlockHandle {
public:
    BlockHandle();
    explicit BlockHandle(const clickhouse::Block& block);

    void append_column(rust::Str name, const class ColumnHandle& column);
    size_t column_count() const;
    size_t row_count() const;
    rust::String column_name(size_t index) const;
    int32_t column_type_code(size_t index) const;
    rust::String column_type_name(size_t index) const;
    std::shared_ptr<class ColumnHandle> column(size_t index) const;
    void clear();
    void reserve(size_t capacity);

    const clickhouse::Block& ref() const { return block_; }

private:
    clickhouse::Block block_;
};

/// Column owner with copy-on-write storage and stable immutable projections.
/// Rust mutations require a pinned exclusive borrow; independent handles detach
/// shared column trees before changing any vector or nested value.
class ColumnHandle {
public:
    explicit ColumnHandle(clickhouse::ColumnRef column);

    size_t size() const;
    int32_t type_code() const;
    rust::String type_name() const;
    void clear();
    void reserve(size_t capacity);
    size_t capacity() const;

    /// Flat bytes of a fixed-width column, using the native storage width.
    rust::Slice<const uint8_t> fixed_bytes() const;
    /// Bytes of one item, valid until the column is mutated.
    rust::Slice<const uint8_t> item_bytes(size_t index) const;
    void append_fixed(rust::Slice<const uint8_t> bytes);
    void append_column(const ColumnHandle& other);
    void append_string(rust::Slice<const uint8_t> value);
    void append_default();

    rust::Slice<const uint8_t> string_at(size_t index) const;
    size_t fixed_string_size() const;

    std::shared_ptr<ColumnHandle> nested() const;
    rust::Slice<const uint8_t> nulls() const;
    void append_nullable_flag(bool is_null);

    rust::Slice<const uint64_t> array_offsets() const;
    void array_append_row(const ColumnHandle& values);

    size_t tuple_len() const;
    std::shared_ptr<ColumnHandle> tuple_at(size_t index) const;
    void tuple_append_fixed(size_t index, rust::Slice<const uint8_t> bytes);
    void tuple_append_string(size_t index, rust::Slice<const uint8_t> value);

    void map_append_row(const ColumnHandle& keys, const ColumnHandle& values);
    std::shared_ptr<ColumnHandle> map_row(size_t index) const;
    void low_cardinality_append_string(rust::Slice<const uint8_t> value);
    void low_cardinality_append_i64(int64_t value);
    void low_cardinality_append_u64(uint64_t value);
    void geo_append_point(double x, double y);

    void nullable_append_fixed(rust::Slice<const uint8_t> bytes);
    void nullable_append_string(rust::Slice<const uint8_t> value);
    void nullable_append_column(const ColumnHandle& other);
    void nullable_append_default();

    const clickhouse::ColumnRef& ref() const { return column_; }

private:
    // Child adapters exist only within a parent mutation that detached the whole tree.
    ColumnHandle(clickhouse::ColumnRef column, bool detach_on_write);
    void prepare_mutation();
    clickhouse::ColumnRef column_;
    bool detach_on_write_ = true;
    mutable std::vector<uint8_t> cache_;
    mutable bool cache_valid_ = false;
};

/// Synchronous Native TCP client used by the integration tests.
class ClientHandle {
public:
    ClientHandle(rust::Str host, uint16_t port);

    void ping();
    void execute(rust::Str query);
    void insert(rust::Str table, const BlockHandle& block);
    std::unique_ptr<BlockHandle> select_one(rust::Str query);

private:
    clickhouse::Client client_;
};

/// Copy the complete column schema and values into independent storage.
clickhouse::ColumnRef clone_column_storage(const clickhouse::ColumnRef& column);
/// Whether independently owned handles share any publicly reachable column storage.
bool column_storage_shared(const clickhouse::ColumnRef& column, long owners = 1);
/// Detach a block from all shared column and nested-column storage before mutation.
void detach_block_storage(clickhouse::Block& block);

std::unique_ptr<BlockHandle> new_block_handle();
std::unique_ptr<ColumnHandle> new_column_handle(rust::Str type_name);
std::unique_ptr<ClientHandle> new_column_client(rust::Str host, uint16_t port);

}  // namespace chcxx
