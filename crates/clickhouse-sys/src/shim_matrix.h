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

/// Shared handle around a clickhouse-cpp Column.
///
/// Mutating methods are const at the C++ boundary because the handle stores a
/// shared_ptr to a mutable column. The Rust wrapper exposes them as &mut self.
class ColumnHandle {
public:
    explicit ColumnHandle(clickhouse::ColumnRef column);

    size_t size() const;
    int32_t type_code() const;
    rust::String type_name() const;
    void clear() const;
    void reserve(size_t capacity) const;
    size_t capacity() const;

    /// Flat bytes of a fixed-width column, using the native storage width.
    rust::Slice<const uint8_t> fixed_bytes() const;
    /// Bytes of one item, valid until the column is mutated.
    rust::Slice<const uint8_t> item_bytes(size_t index) const;
    void append_fixed(rust::Slice<const uint8_t> bytes) const;
    void append_column(const ColumnHandle& other) const;
    void append_string(rust::Slice<const uint8_t> value) const;
    void append_default() const;

    rust::Slice<const uint8_t> string_at(size_t index) const;
    size_t fixed_string_size() const;

    std::shared_ptr<ColumnHandle> nested() const;
    rust::Slice<const uint8_t> nulls() const;
    void append_nullable_flag(bool is_null) const;

    rust::Slice<const uint64_t> array_offsets() const;
    void array_append_row(const ColumnHandle& values) const;

    size_t tuple_len() const;
    std::shared_ptr<ColumnHandle> tuple_at(size_t index) const;
    void tuple_append_fixed(size_t index, rust::Slice<const uint8_t> bytes) const;
    void tuple_append_string(size_t index, rust::Slice<const uint8_t> value) const;

    void map_append_row(const ColumnHandle& keys, const ColumnHandle& values) const;
    std::shared_ptr<ColumnHandle> map_row(size_t index) const;
    void low_cardinality_append_string(rust::Slice<const uint8_t> value) const;
    void low_cardinality_append_i64(int64_t value) const;
    void low_cardinality_append_u64(uint64_t value) const;
    void geo_append_point(double x, double y) const;

    const clickhouse::ColumnRef& ref() const { return column_; }

private:
    clickhouse::ColumnRef column_;
    mutable std::vector<uint8_t> cache_;
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

std::unique_ptr<BlockHandle> new_block_handle();
std::unique_ptr<ColumnHandle> new_column_handle(rust::Str type_name);
std::unique_ptr<ClientHandle> new_column_client(rust::Str host, uint16_t port);

}  // namespace chcxx
