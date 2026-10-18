#include "shim_matrix.h"

#include <memory>
#include <stdexcept>
#include <string>

#include "clickhouse/columns/factory.h"

namespace chcxx {

BlockHandle::BlockHandle() = default;

BlockHandle::BlockHandle(const clickhouse::Block& block) : block_(block) {}

void BlockHandle::append_column(rust::Str name, const ColumnHandle& column) {
    block_.AppendColumn(std::string(name.data(), name.size()), column.ref());
}

size_t BlockHandle::column_count() const {
    return block_.GetColumnCount();
}

size_t BlockHandle::row_count() const {
    return block_.GetRowCount();
}

rust::String BlockHandle::column_name(size_t index) const {
    return rust::String(block_.GetColumnName(index));
}

int32_t BlockHandle::column_type_code(size_t index) const {
    return static_cast<int32_t>(block_.At(index)->Type()->GetCode());
}

rust::String BlockHandle::column_type_name(size_t index) const {
    return rust::String(block_.At(index)->Type()->GetName());
}

std::shared_ptr<ColumnHandle> BlockHandle::column(size_t index) const {
    return std::make_shared<ColumnHandle>(block_.At(index));
}

void BlockHandle::clear() {
    block_.Clear();
}

void BlockHandle::reserve(size_t capacity) {
    block_.Reserve(capacity);
}

std::unique_ptr<BlockHandle> new_block_handle() {
    return std::make_unique<BlockHandle>();
}

std::unique_ptr<ColumnHandle> new_column_handle(rust::Str type_name) {
    auto column = clickhouse::CreateColumnByType(std::string(type_name.data(), type_name.size()));
    if (!column) {
        throw std::invalid_argument(
            "clickhouse-cpp does not support column type '" +
            std::string(type_name.data(), type_name.size()) + "'");
    }
    return std::make_unique<ColumnHandle>(std::move(column));
}

}  // namespace chcxx
