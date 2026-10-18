#include "shim.h"

#include "clickhouse/block.h"
#include "clickhouse/client.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/query.h"

#include <stdexcept>
#include <string>
#include <vector>

namespace chcxx {
namespace {

constexpr uint32_t KIND_U8 = 1;
constexpr uint32_t KIND_U16 = 2;
constexpr uint32_t KIND_U32 = 3;
constexpr uint32_t KIND_U64 = 4;
constexpr uint32_t KIND_I8 = 5;
constexpr uint32_t KIND_I16 = 6;
constexpr uint32_t KIND_I32 = 7;
constexpr uint32_t KIND_I64 = 8;
constexpr uint32_t KIND_F32 = 9;
constexpr uint32_t KIND_F64 = 10;
constexpr uint32_t KIND_STRING = 11;

std::string to_std(rust::Str text) {
    return std::string(text.data(), text.size());
}

template <typename T>
std::shared_ptr<clickhouse::ColumnVector<T>> numeric(
    const clickhouse::ColumnRef& column) {
    return std::dynamic_pointer_cast<clickhouse::ColumnVector<T>>(column);
}

clickhouse::ColumnRef find_column(clickhouse::Block& block,
                                  const std::string& name) {
    for (size_t index = 0; index < block.GetColumnCount(); ++index) {
        if (block.GetColumnName(index) == name) {
            return block.At(index);
        }
    }
    return nullptr;
}


template <typename T>
void append_numeric(clickhouse::Block& block, rust::Str name,
                    rust::Slice<const T> values) {
    const std::string key = to_std(name);
    if (auto existing = find_column(block, key)) {
        auto typed = std::dynamic_pointer_cast<clickhouse::ColumnVector<T>>(existing);
        if (!typed) {
            throw std::runtime_error("column kind mismatch for " + key);
        }
        for (const auto& value : values) {
            typed->Append(value);
        }
        return;
    }
    auto column = std::make_shared<clickhouse::ColumnVector<T>>();
    for (const auto& value : values) {
        column->Append(value);
    }
    block.AppendColumn(key, column);
}

template <typename T>
const std::vector<T>& numeric_data(const clickhouse::ColumnRef& column) {
    auto typed = numeric<T>(column);
    if (!typed) {
        throw std::runtime_error("clickhouse-cpp column kind mismatch");
    }
    return typed->GetWritableData();
}

uint32_t kind_of(const clickhouse::ColumnRef& column) {
    if (numeric<uint8_t>(column)) return KIND_U8;
    if (numeric<uint16_t>(column)) return KIND_U16;
    if (numeric<uint32_t>(column)) return KIND_U32;
    if (numeric<uint64_t>(column)) return KIND_U64;
    if (numeric<int8_t>(column)) return KIND_I8;
    if (numeric<int16_t>(column)) return KIND_I16;
    if (numeric<int32_t>(column)) return KIND_I32;
    if (numeric<int64_t>(column)) return KIND_I64;
    if (numeric<float>(column)) return KIND_F32;
    if (numeric<double>(column)) return KIND_F64;
    if (std::dynamic_pointer_cast<clickhouse::ColumnString>(column)) {
        return KIND_STRING;
    }
    return 0;
}

rust::Slice<const uint8_t> bytes_of(const clickhouse::ColumnRef& column,
                                    uint32_t kind) {
    const uint8_t* data = nullptr;
    size_t size = 0;
    switch (kind) {
        case KIND_U8: {
            const auto& values = numeric_data<uint8_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(uint8_t);
            break;
        }
        case KIND_U16: {
            const auto& values = numeric_data<uint16_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(uint16_t);
            break;
        }
        case KIND_U32: {
            const auto& values = numeric_data<uint32_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(uint32_t);
            break;
        }
        case KIND_U64: {
            const auto& values = numeric_data<uint64_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(uint64_t);
            break;
        }
        case KIND_I8: {
            const auto& values = numeric_data<int8_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(int8_t);
            break;
        }
        case KIND_I16: {
            const auto& values = numeric_data<int16_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(int16_t);
            break;
        }
        case KIND_I32: {
            const auto& values = numeric_data<int32_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(int32_t);
            break;
        }
        case KIND_I64: {
            const auto& values = numeric_data<int64_t>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(int64_t);
            break;
        }
        case KIND_F32: {
            const auto& values = numeric_data<float>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(float);
            break;
        }
        case KIND_F64: {
            const auto& values = numeric_data<double>(column);
            data = reinterpret_cast<const uint8_t*>(values.data());
            size = values.size() * sizeof(double);
            break;
        }
        default:
            throw std::runtime_error("column is not a fixed-width column");
    }
    return rust::Slice<const uint8_t>(data, size);
}

}  // namespace

Client::Client(rust::Str host, uint16_t port, rust::Str user, rust::Str password,
               rust::Str database) {
    clickhouse::ClientOptions options;
    options.SetHost(to_std(host));
    options.SetPort(port);
    options.SetUser(to_std(user));
    options.SetPassword(to_std(password));
    options.SetDefaultDatabase(to_std(database));
    options.SetRethrowException(true);
    inner_ = std::make_unique<clickhouse::Client>(options);
}

clickhouse::Client& Client::inner() { return *inner_; }
const clickhouse::Client& Client::inner() const { return *inner_; }

Block::Block() = default;
Block::Block(const clickhouse::Block& block) : inner_(block) {}

clickhouse::Block& Block::inner() { return inner_; }
const clickhouse::Block& Block::inner() const { return inner_; }

std::unique_ptr<Client> new_client(rust::Str host, uint16_t port, rust::Str user,
                                   rust::Str password, rust::Str database) {
    return std::make_unique<Client>(host, port, user, password, database);
}

void ping(Client& client) { client.inner().Ping(); }

void execute(Client& client, rust::Str sql) {
    clickhouse::Query query(to_std(sql));
    client.inner().Execute(query);
}

void insert_block(Client& client, rust::Str table, const Block& block) {
    client.inner().Insert(to_std(table), block.inner());
}

std::unique_ptr<BlockList> query_blocks(Client& client, rust::Str sql) {
    auto list = std::make_unique<BlockList>();
    client.inner().Select(to_std(sql), [&list](const clickhouse::Block& block) {
        list->blocks.push_back(std::make_unique<Block>(block));
    });
    return list;
}

size_t block_list_len(const BlockList& list) { return list.blocks.size(); }

std::unique_ptr<Block> block_list_get(const BlockList& list, size_t index) {
    return std::make_unique<Block>(*list.blocks.at(index));
}

std::unique_ptr<Block> block_new() { return std::make_unique<Block>(); }

void block_append_column_handle(Block& block, rust::Str name,
                                const ColumnHandle& column) {
    block.inner().AppendColumn(std::string(name.data(), name.size()), column.ref());
}

std::shared_ptr<ColumnHandle> block_column_handle(const Block& block, size_t index) {
    return std::make_shared<ColumnHandle>(block.inner().At(index));
}

void block_append_u8(Block& block, rust::Str name,
                     rust::Slice<const uint8_t> values) {
    append_numeric<uint8_t>(block.inner(), name, values);
}

void block_append_u16(Block& block, rust::Str name,
                      rust::Slice<const uint16_t> values) {
    append_numeric<uint16_t>(block.inner(), name, values);
}

void block_append_u32(Block& block, rust::Str name,
                      rust::Slice<const uint32_t> values) {
    append_numeric<uint32_t>(block.inner(), name, values);
}

void block_append_u64(Block& block, rust::Str name,
                      rust::Slice<const uint64_t> values) {
    append_numeric<uint64_t>(block.inner(), name, values);
}

void block_append_i8(Block& block, rust::Str name,
                     rust::Slice<const int8_t> values) {
    append_numeric<int8_t>(block.inner(), name, values);
}

void block_append_i16(Block& block, rust::Str name,
                      rust::Slice<const int16_t> values) {
    append_numeric<int16_t>(block.inner(), name, values);
}

void block_append_i32(Block& block, rust::Str name,
                      rust::Slice<const int32_t> values) {
    append_numeric<int32_t>(block.inner(), name, values);
}

void block_append_i64(Block& block, rust::Str name,
                      rust::Slice<const int64_t> values) {
    append_numeric<int64_t>(block.inner(), name, values);
}

void block_append_f32(Block& block, rust::Str name,
                      rust::Slice<const float> values) {
    append_numeric<float>(block.inner(), name, values);
}

void block_append_f64(Block& block, rust::Str name,
                      rust::Slice<const double> values) {
    append_numeric<double>(block.inner(), name, values);
}

void block_append_string(Block& block, rust::Str name,
                         rust::Slice<const rust::String> values) {
    const std::string key = to_std(name);
    if (auto existing = find_column(block.inner(), key)) {
        auto typed = std::dynamic_pointer_cast<clickhouse::ColumnString>(existing);
        if (!typed) {
            throw std::runtime_error("column kind mismatch for " + key);
        }
        for (const auto& value : values) {
            typed->Append(std::string_view(value.data(), value.size()));
        }
        return;
    }
    auto column = std::make_shared<clickhouse::ColumnString>();
    for (const auto& value : values) {
        column->Append(std::string_view(value.data(), value.size()));
    }
    block.inner().AppendColumn(key, column);
}

void block_append_string_flat(Block& block, rust::Str name,
                              rust::Slice<const uint64_t> offsets,
                              rust::Slice<const uint8_t> bytes) {
    const std::string key = to_std(name);
    if (offsets.empty()) {
        if (!bytes.empty()) {
            throw std::runtime_error("string offsets are empty but bytes are not");
        }
    }
    uint64_t previous = 0;
    for (const auto& offset : offsets) {
        if (offset < previous || offset > bytes.size()) {
            throw std::runtime_error("invalid string offsets");
        }
        previous = offset;
    }
    if (previous != bytes.size()) {
        throw std::runtime_error("string offsets do not cover every byte");
    }
    std::shared_ptr<clickhouse::ColumnString> column;
    bool created = false;
    if (auto existing = find_column(block.inner(), key)) {
        column = std::dynamic_pointer_cast<clickhouse::ColumnString>(existing);
        if (!column) {
            throw std::runtime_error("column kind mismatch for " + key);
        }
    } else {
        column = std::make_shared<clickhouse::ColumnString>();
        created = true;
    }
    previous = 0;
    for (const auto& offset : offsets) {
        const auto* start = reinterpret_cast<const char*>(bytes.data() + previous);
        column->Append(std::string_view(start, offset - previous));
        previous = offset;
    }
    if (created) {
        block.inner().AppendColumn(key, column);
    }
}

rust::Vec<uint64_t> block_column_string_offsets(const Block& block, size_t index) {
    auto column = std::dynamic_pointer_cast<clickhouse::ColumnString>(block.inner().At(index));
    if (!column) {
        throw std::runtime_error("column is not a String column");
    }
    rust::Vec<uint64_t> offsets;
    uint64_t total = 0;
    for (size_t row = 0; row < column->Size(); ++row) {
        total += column->At(row).size();
        offsets.push_back(total);
    }
    return offsets;
}

rust::Vec<uint8_t> block_column_string_flat(const Block& block, size_t index) {
    auto column = std::dynamic_pointer_cast<clickhouse::ColumnString>(block.inner().At(index));
    if (!column) {
        throw std::runtime_error("column is not a String column");
    }
    rust::Vec<uint8_t> bytes;
    for (size_t row = 0; row < column->Size(); ++row) {
        const auto value = column->At(row);
        for (char byte : value) {
            bytes.push_back(static_cast<uint8_t>(byte));
        }
    }
    return bytes;
}

void block_clear(Block& block) { block.inner().Clear(); }

void block_refresh(Block& block) { block.inner().RefreshRowCount(); }

size_t block_rows(const Block& block) { return block.inner().GetRowCount(); }

size_t block_columns(const Block& block) {
    return block.inner().GetColumnCount();
}

rust::String block_column_name(const Block& block, size_t index) {
    return rust::String(block.inner().GetColumnName(index));
}

uint32_t block_column_kind(const Block& block, size_t index) {
    return kind_of(block.inner().At(index));
}

rust::Slice<const uint8_t> block_column_bytes(const Block& block, size_t index) {
    auto column = block.inner().At(index);
    return bytes_of(column, kind_of(column));
}

rust::Vec<rust::String> block_column_strings(const Block& block, size_t index) {
    auto column =
        std::dynamic_pointer_cast<clickhouse::ColumnString>(block.inner().At(index));
    if (!column) {
        throw std::runtime_error("column is not a String column");
    }
    rust::Vec<rust::String> values;
    for (size_t row = 0; row < column->Size(); ++row) {
        values.push_back(rust::String(std::string(column->At(row))));
    }
    return values;
}

size_t block_column_capacity(const Block& block, size_t index) {
    auto column = block.inner().At(index);
    switch (kind_of(column)) {
        case KIND_U8:
            return numeric<uint8_t>(column)->Capacity();
        case KIND_U16:
            return numeric<uint16_t>(column)->Capacity();
        case KIND_U32:
            return numeric<uint32_t>(column)->Capacity();
        case KIND_U64:
            return numeric<uint64_t>(column)->Capacity();
        case KIND_I8:
            return numeric<int8_t>(column)->Capacity();
        case KIND_I16:
            return numeric<int16_t>(column)->Capacity();
        case KIND_I32:
            return numeric<int32_t>(column)->Capacity();
        case KIND_I64:
            return numeric<int64_t>(column)->Capacity();
        case KIND_F32:
            return numeric<float>(column)->Capacity();
        case KIND_F64:
            return numeric<double>(column)->Capacity();
        default:
            return 0;
    }
}

}  // namespace chcxx
