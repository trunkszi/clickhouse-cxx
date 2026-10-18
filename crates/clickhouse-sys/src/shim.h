#pragma once

#include "clickhouse/block.h"
#include "clickhouse/client.h"
#include "rust/cxx.h"
#include "shim_matrix.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace chcxx {

/// Owning wrapper around a synchronous clickhouse-cpp client.
class Client {
public:
    Client(rust::Str host, uint16_t port, rust::Str user, rust::Str password, rust::Str database);

    clickhouse::Client& inner();
    const clickhouse::Client& inner() const;

private:
    std::unique_ptr<clickhouse::Client> inner_;
};

/// Owning wrapper around a clickhouse-cpp block.
class Block {
public:
    Block();
    explicit Block(const clickhouse::Block& block);

    clickhouse::Block& inner();
    const clickhouse::Block& inner() const;

private:
    clickhouse::Block inner_;
};

/// Owning collection returned by a select query.
class BlockList {
public:
    std::vector<std::unique_ptr<Block>> blocks;
};

std::unique_ptr<Client> new_client(rust::Str host, uint16_t port, rust::Str user,
                                   rust::Str password, rust::Str database);

void ping(Client& client);
void execute(Client& client, rust::Str sql);
void insert_block(Client& client, rust::Str table, const Block& block);
std::unique_ptr<BlockList> query_blocks(Client& client, rust::Str sql);
size_t block_list_len(const BlockList& list);
std::unique_ptr<Block> block_list_get(const BlockList& list, size_t index);

std::unique_ptr<Block> block_new();
void block_append_column_handle(Block& block, rust::Str name, const ColumnHandle& column);
std::shared_ptr<ColumnHandle> block_column_handle(const Block& block, size_t index);
void block_append_u8(Block& block, rust::Str name, rust::Slice<const uint8_t> values);
void block_append_u16(Block& block, rust::Str name, rust::Slice<const uint16_t> values);
void block_append_u32(Block& block, rust::Str name, rust::Slice<const uint32_t> values);
void block_append_u64(Block& block, rust::Str name, rust::Slice<const uint64_t> values);
void block_append_i8(Block& block, rust::Str name, rust::Slice<const int8_t> values);
void block_append_i16(Block& block, rust::Str name, rust::Slice<const int16_t> values);
void block_append_i32(Block& block, rust::Str name, rust::Slice<const int32_t> values);
void block_append_i64(Block& block, rust::Str name, rust::Slice<const int64_t> values);
void block_append_f32(Block& block, rust::Str name, rust::Slice<const float> values);
void block_append_f64(Block& block, rust::Str name, rust::Slice<const double> values);
void block_append_string(Block& block, rust::Str name, rust::Slice<const rust::String> values);
void block_append_string_flat(Block& block, rust::Str name,
                              rust::Slice<const uint64_t> offsets,
                              rust::Slice<const uint8_t> bytes);
rust::Vec<uint64_t> block_column_string_offsets(const Block& block, size_t index);
rust::Vec<uint8_t> block_column_string_flat(const Block& block, size_t index);

void block_clear(Block& block);
void block_refresh(Block& block);
size_t block_rows(const Block& block);
size_t block_columns(const Block& block);
rust::String block_column_name(const Block& block, size_t index);
uint32_t block_column_kind(const Block& block, size_t index);
rust::Slice<const uint8_t> block_column_bytes(const Block& block, size_t index);
rust::Vec<rust::String> block_column_strings(const Block& block, size_t index);
size_t block_column_capacity(const Block& block, size_t index);

}  // namespace chcxx
