#include "shim_matrix.h"

#include <memory>
#include <stdexcept>
#include <string>

#include "clickhouse/columns/factory.h"

namespace chcxx {

void detach_block_storage(clickhouse::Block& block) {
    bool shared = false;
    for (std::size_t i = 0; i < block.GetColumnCount(); ++i) {
        auto column = block.At(i);
        if (column_storage_shared(column, 2)) shared = true;
    }
    if (!shared) return;
    clickhouse::Block detached;
    detached.SetInfo(block.Info());
    for (std::size_t i = 0; i < block.GetColumnCount(); ++i) {
        auto column = block.At(i);
        detached.AppendColumn(block.GetColumnName(i), clone_column_storage(column));
    }
    block = std::move(detached);
}


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
    detach_block_storage(block_);
    block_.Clear();
}

void BlockHandle::reserve(size_t capacity) {
    detach_block_storage(block_);
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
