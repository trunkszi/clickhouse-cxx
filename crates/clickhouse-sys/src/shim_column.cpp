#include "shim_matrix.h"

#include <algorithm>
#include <cstring>
#include <ctime>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/tuple.h"
#include "clickhouse/columns/bool.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/enum.h"
#include "clickhouse/columns/factory.h"
#include "clickhouse/columns/ip4.h"
#include "clickhouse/columns/ip6.h"
#include "clickhouse/columns/json.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/time.h"
#include "clickhouse/columns/uuid.h"

namespace chcxx {
namespace {

using Code = clickhouse::Type::Code;

template <typename T>
T* cast(const clickhouse::ColumnRef& column) {
    auto* typed = dynamic_cast<T*>(column.get());
    if (!typed) {
        throw std::runtime_error("column type does not match requested C++ column class");
    }
    return typed;
}

template <typename T>
rust::Slice<const uint8_t> vector_bytes(const clickhouse::ColumnRef& column) {
    const auto& data = cast<clickhouse::ColumnVector<T>>(column)->GetWritableData();
    return rust::Slice<const uint8_t>(
        reinterpret_cast<const uint8_t*>(data.data()), data.size() * sizeof(T));
}

template <typename T>
rust::Slice<const uint8_t> plain_vector_bytes(const clickhouse::ColumnRef& column) {
    const auto& data = cast<T>(column)->GetWritableData();
    using Element = typename std::decay_t<decltype(data)>::value_type;
    return rust::Slice<const uint8_t>(
        reinterpret_cast<const uint8_t*>(data.data()), data.size() * sizeof(Element));
}

std::size_t fixed_width(const clickhouse::ColumnRef& column) {
    switch (column->Type()->GetCode()) {
        case Code::UInt8:
        case Code::Int8:
        case Code::Bool:
        case Code::Enum8:
            return 1;
        case Code::UInt16:
        case Code::Int16:
        case Code::Date:
        case Code::Enum16:
            return 2;
        case Code::UInt32:
        case Code::Int32:
        case Code::Float32:
        case Code::Date32:
        case Code::DateTime:
        case Code::IPv4:
        case Code::Time:
            return 4;
        case Code::UInt64:
        case Code::Int64:
        case Code::Float64:
        case Code::DateTime64:
        case Code::Time64:
            return 8;
        case Code::UInt128:
        case Code::Int128:
        case Code::UUID:
        case Code::IPv6:
            return 16;
        case Code::Decimal:
        case Code::Decimal32:
        case Code::Decimal64:
        case Code::Decimal128: {
            const auto precision = cast<clickhouse::ColumnDecimal>(column)->GetPrecision();
            if (precision <= 9) {
                return 4;
            }
            if (precision <= 18) {
                return 8;
            }
            return 16;
        }
        case Code::FixedString:
            return cast<clickhouse::ColumnFixedString>(column)->FixedSize();
        default:
            return 0;
    }
}

template <typename T>
void append_values(const clickhouse::ColumnRef& column, const uint8_t* data, std::size_t count) {
    auto& target = cast<clickhouse::ColumnVector<T>>(column)->GetWritableData();
    if (count == 0) return;
    if (count > target.max_size() - target.size()) {
        throw std::length_error("fixed column capacity overflow");
    }
    const auto offset = target.size();
    target.resize(offset + count);
    std::memcpy(target.data() + offset, data, count * sizeof(T));
}

clickhouse::Int128 int128_from(const uint8_t* data, std::size_t width) {
    if (width == 16) {
        clickhouse::Int128 value;
        std::memcpy(value.limbs, data, 16);
        return value;
    }
    std::int64_t narrow = 0;
    std::memcpy(&narrow, data, width);
    if (width == 4) {
        narrow = static_cast<std::int32_t>(narrow);
    }
    return clickhouse::Int128(narrow);
}

}  // namespace

clickhouse::ColumnRef clone_column_storage(const clickhouse::ColumnRef& column) {
    // Recreate the full schema, including nullable LowCardinality dictionary
    // sentinels. Append retains partially assembled Nullable/Tuple children.
    auto copy = clickhouse::CreateColumnByType(column->Type()->GetName());
    if (!copy) throw std::invalid_argument("cannot clone native column schema");
    copy->Append(column);
    return copy;
}

bool column_storage_shared(const clickhouse::ColumnRef& column, long owners) {
    if (column.use_count() > owners) return true;
    // These are the child handles exposed by nested()/tuple_at(). Each local
    // ColumnRef adds one reference to the one retained by its parent.
    // map_row() owns a Slice copy; other composite children are not exported.
    if (auto* value = dynamic_cast<clickhouse::ColumnNullable*>(column.get())) {
        auto child = value->Nested();
        return column_storage_shared(child, 2);
    }
    if (auto* value = dynamic_cast<clickhouse::ColumnArray*>(column.get())) {
        auto child = value->GetData();
        return column_storage_shared(child, 2);
    }
    if (auto* value = dynamic_cast<clickhouse::ColumnTuple*>(column.get())) {
        for (std::size_t i = 0; i < value->TupleSize(); ++i) {
            auto child = value->At(i);
            if (column_storage_shared(child, 2)) return true;
        }
    }
    return false;
}

ColumnHandle::ColumnHandle(clickhouse::ColumnRef column) : column_(std::move(column)) {}

ColumnHandle::ColumnHandle(clickhouse::ColumnRef column, bool detach_on_write)
    : column_(std::move(column)), detach_on_write_(detach_on_write) {}

void ColumnHandle::prepare_mutation() {
    if (detach_on_write_ && column_storage_shared(column_)) {
        auto detached = clone_column_storage(column_);
        column_ = std::move(detached);
    }
    cache_valid_ = false;
}


std::size_t ColumnHandle::size() const {
    return column_->Size();
}

std::int32_t ColumnHandle::type_code() const {
    return static_cast<std::int32_t>(column_->Type()->GetCode());
}

rust::String ColumnHandle::type_name() const {
    return rust::String(column_->Type()->GetName());
}

void ColumnHandle::clear() {
    prepare_mutation();
    column_->Clear();
    cache_.clear();
}

void ColumnHandle::reserve(std::size_t capacity) {
    prepare_mutation();
    column_->Reserve(capacity);
}

std::size_t ColumnHandle::capacity() const {
    switch (column_->Type()->GetCode()) {
        case Code::UInt8:
            return cast<clickhouse::ColumnUInt8>(column_)->Capacity();
        case Code::UInt16:
            return cast<clickhouse::ColumnUInt16>(column_)->Capacity();
        case Code::UInt32:
            return cast<clickhouse::ColumnUInt32>(column_)->Capacity();
        case Code::UInt64:
            return cast<clickhouse::ColumnUInt64>(column_)->Capacity();
        case Code::Int8:
            return cast<clickhouse::ColumnInt8>(column_)->Capacity();
        case Code::Int16:
            return cast<clickhouse::ColumnInt16>(column_)->Capacity();
        case Code::Int32:
            return cast<clickhouse::ColumnInt32>(column_)->Capacity();
        case Code::Int64:
            return cast<clickhouse::ColumnInt64>(column_)->Capacity();
        case Code::Float32:
            return cast<clickhouse::ColumnFloat32>(column_)->Capacity();
        case Code::Float64:
            return cast<clickhouse::ColumnFloat64>(column_)->Capacity();
        case Code::Bool:
            return cast<clickhouse::ColumnBool>(column_)->Capacity();
        case Code::Date:
            return cast<clickhouse::ColumnDate>(column_)->Capacity();
        case Code::Date32:
            return cast<clickhouse::ColumnDate32>(column_)->Capacity();
        case Code::DateTime:
            return cast<clickhouse::ColumnDateTime>(column_)->Capacity();
        case Code::Time:
            return cast<clickhouse::ColumnTime>(column_)->Capacity();
        case Code::Time64:
            return cast<clickhouse::ColumnTime64>(column_)->Capacity();
        default:
            return column_->Size();
    }
}

rust::Slice<const uint8_t> ColumnHandle::fixed_bytes() const {
    if (cache_valid_) return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
    switch (column_->Type()->GetCode()) {
        case Code::UInt8:
            return vector_bytes<uint8_t>(column_);
        case Code::UInt16:
            return vector_bytes<uint16_t>(column_);
        case Code::UInt32:
            return vector_bytes<uint32_t>(column_);
        case Code::UInt64:
            return vector_bytes<uint64_t>(column_);
        case Code::UInt128:
            return vector_bytes<clickhouse::UInt128>(column_);
        case Code::Int8:
            return vector_bytes<int8_t>(column_);
        case Code::Int16:
            return vector_bytes<int16_t>(column_);
        case Code::Int32:
            return vector_bytes<int32_t>(column_);
        case Code::Int64:
            return vector_bytes<int64_t>(column_);
        case Code::Int128:
            return vector_bytes<clickhouse::Int128>(column_);
        case Code::Float32:
            return vector_bytes<float>(column_);
        case Code::Float64:
            return vector_bytes<double>(column_);
        case Code::Date:
            return plain_vector_bytes<clickhouse::ColumnDate>(column_);
        case Code::Date32:
            return plain_vector_bytes<clickhouse::ColumnDate32>(column_);
        case Code::DateTime:
            return plain_vector_bytes<clickhouse::ColumnDateTime>(column_);
        case Code::Time:
            return plain_vector_bytes<clickhouse::ColumnTime>(column_);
        case Code::Time64:
            return plain_vector_bytes<clickhouse::ColumnTime64>(column_);
        case Code::Bool: {
            const auto* typed = cast<clickhouse::ColumnBool>(column_);
            cache_.assign(typed->Size(), 0);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                cache_[i] = typed->At(i) ? 1 : 0;
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::Enum8: {
            const auto* typed = cast<clickhouse::ColumnEnum8>(column_);
            cache_.resize(typed->Size());
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                cache_[i] = static_cast<uint8_t>(typed->At(i));
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::Enum16: {
            const auto* typed = cast<clickhouse::ColumnEnum16>(column_);
            cache_.resize(typed->Size() * 2);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * 2, &value, 2);
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::Decimal:
        case Code::Decimal32:
        case Code::Decimal64:
        case Code::Decimal128: {
            const auto* typed = cast<clickhouse::ColumnDecimal>(column_);
            const auto width = fixed_width(column_);
            cache_.assign(typed->Size() * width, 0);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * width, value.limbs, width);
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::DateTime64: {
            const auto* typed = cast<clickhouse::ColumnDateTime64>(column_);
            cache_.resize(typed->Size() * 8);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * 8, &value, 8);
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::UUID: {
            const auto* typed = cast<clickhouse::ColumnUUID>(column_);
            cache_.resize(typed->Size() * 16);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * 16, &value.first, 8);
                std::memcpy(cache_.data() + i * 16 + 8, &value.second, 8);
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::IPv4: {
            const auto* typed = cast<clickhouse::ColumnIPv4>(column_);
            cache_.resize(typed->Size() * 4);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * 4, &value.s_addr, 4);
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::IPv6: {
            const auto* typed = cast<clickhouse::ColumnIPv6>(column_);
            cache_.resize(typed->Size() * 16);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * 16, &value, 16);
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        case Code::FixedString: {
            const auto* typed = cast<clickhouse::ColumnFixedString>(column_);
            const auto width = typed->FixedSize();
            cache_.assign(typed->Size() * width, 0);
            for (std::size_t i = 0; i < typed->Size(); ++i) {
                const auto value = typed->At(i);
                std::memcpy(cache_.data() + i * width, value.data(), std::min(width, value.size()));
            }
            cache_valid_ = true;
            return rust::Slice<const uint8_t>(cache_.data(), cache_.size());
        }
        default:
            return rust::Slice<const uint8_t>(nullptr, 0);
    }
}

rust::Slice<const uint8_t> ColumnHandle::item_bytes(std::size_t index) const {
    try {
        const auto width = fixed_width(column_);
        if (width != 0) {
            if (index >= column_->Size()) return rust::Slice<const uint8_t>(nullptr, 0);
            const auto bytes = fixed_bytes();
            return rust::Slice<const uint8_t>(bytes.data() + index * width, width);
        }
        const auto view = column_->GetItem(index).AsBinaryData();
        return rust::Slice<const uint8_t>(
            reinterpret_cast<const uint8_t*>(view.data()), view.size());
    } catch (...) {
        return rust::Slice<const uint8_t>(nullptr, 0);
    }
}

void ColumnHandle::append_fixed(rust::Slice<const uint8_t> bytes) {
    prepare_mutation();
    const auto width = fixed_width(column_);
    if (width == 0) {
        throw std::invalid_argument("column type is not fixed width");
    }
    if (bytes.size() % width != 0) {
        throw std::invalid_argument("fixed-width byte slice is not a multiple of the element width");
    }
    const auto count = bytes.size() / width;
    const auto* data = bytes.data();
    switch (column_->Type()->GetCode()) {
        case Code::UInt8:
            append_values<uint8_t>(column_, data, count);
            break;
        case Code::UInt16:
            append_values<uint16_t>(column_, data, count);
            break;
        case Code::UInt32:
            append_values<uint32_t>(column_, data, count);
            break;
        case Code::UInt64:
            append_values<uint64_t>(column_, data, count);
            break;
        case Code::Int8:
            append_values<int8_t>(column_, data, count);
            break;
        case Code::Int16:
            append_values<int16_t>(column_, data, count);
            break;
        case Code::Int32:
            append_values<int32_t>(column_, data, count);
            break;
        case Code::Int64:
            append_values<int64_t>(column_, data, count);
            break;
        case Code::Float32:
            append_values<float>(column_, data, count);
            break;
        case Code::Float64:
            append_values<double>(column_, data, count);
            break;
        case Code::UInt128: {
            auto& target = cast<clickhouse::ColumnUInt128>(column_)->GetWritableData();
            for (std::size_t i = 0; i < count; ++i) {
                clickhouse::UInt128 value;
                std::memcpy(value.limbs, data + i * 16, 16);
                target.push_back(value);
            }
            break;
        }
        case Code::Int128: {
            auto& target = cast<clickhouse::ColumnInt128>(column_)->GetWritableData();
            for (std::size_t i = 0; i < count; ++i) {
                clickhouse::Int128 value;
                std::memcpy(value.limbs, data + i * 16, 16);
                target.push_back(value);
            }
            break;
        }
        case Code::Bool: {
            auto* typed = cast<clickhouse::ColumnBool>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                typed->Append(data[i] != 0);
            }
            break;
        }
        case Code::Date: {
            auto* typed = cast<clickhouse::ColumnDate>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                uint16_t value = 0;
                std::memcpy(&value, data + i * 2, 2);
                typed->AppendRaw(value);
            }
            break;
        }
        case Code::Date32: {
            auto* typed = cast<clickhouse::ColumnDate32>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                int32_t value = 0;
                std::memcpy(&value, data + i * 4, 4);
                typed->AppendRaw(value);
            }
            break;
        }
        case Code::DateTime: {
            auto* typed = cast<clickhouse::ColumnDateTime>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                uint32_t value = 0;
                std::memcpy(&value, data + i * 4, 4);
                typed->Append(static_cast<std::time_t>(value));
            }
            break;
        }
        case Code::DateTime64: {
            auto* typed = cast<clickhouse::ColumnDateTime64>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                std::int64_t value = 0;
                std::memcpy(&value, data + i * 8, 8);
                typed->Append(value);
            }
            break;
        }
        case Code::Time: {
            auto* typed = cast<clickhouse::ColumnTime>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                std::int32_t value = 0;
                std::memcpy(&value, data + i * 4, 4);
                typed->Append(value);
            }
            break;
        }
        case Code::Time64: {
            auto* typed = cast<clickhouse::ColumnTime64>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                std::int64_t value = 0;
                std::memcpy(&value, data + i * 8, 8);
                typed->Append(value);
            }
            break;
        }
        case Code::Decimal:
        case Code::Decimal32:
        case Code::Decimal64:
        case Code::Decimal128: {
            auto* typed = cast<clickhouse::ColumnDecimal>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                typed->Append(int128_from(data + i * width, width));
            }
            break;
        }
        case Code::Enum8: {
            auto* typed = cast<clickhouse::ColumnEnum8>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                typed->Append(static_cast<int8_t>(data[i]));
            }
            break;
        }
        case Code::Enum16: {
            auto* typed = cast<clickhouse::ColumnEnum16>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                int16_t value = 0;
                std::memcpy(&value, data + i * 2, 2);
                typed->Append(value);
            }
            break;
        }
        case Code::UUID: {
            auto* typed = cast<clickhouse::ColumnUUID>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                uint64_t low = 0;
                uint64_t high = 0;
                std::memcpy(&low, data + i * 16, 8);
                std::memcpy(&high, data + i * 16 + 8, 8);
                typed->Append(clickhouse::UUID{low, high});
            }
            break;
        }
        case Code::IPv4: {
            auto* typed = cast<clickhouse::ColumnIPv4>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                uint32_t value = 0;
                std::memcpy(&value, data + i * 4, 4);
                typed->Append(value);
            }
            break;
        }
        case Code::IPv6: {
            auto* typed = cast<clickhouse::ColumnIPv6>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                in6_addr value{};
                std::memcpy(&value, data + i * 16, 16);
                typed->Append(value);
            }
            break;
        }
        case Code::FixedString: {
            auto* typed = cast<clickhouse::ColumnFixedString>(column_);
            for (std::size_t i = 0; i < count; ++i) {
                typed->Append(std::string_view(
                    reinterpret_cast<const char*>(data + i * width), width));
            }
            break;
        }
        default:
            throw std::invalid_argument("column type cannot be appended as fixed-width data");
    }
    cache_.clear();
}

void ColumnHandle::append_column(const ColumnHandle& other) {
    prepare_mutation();
    column_->Append(other.ref());
    cache_.clear();
}

void ColumnHandle::append_string(rust::Slice<const uint8_t> value) {
    prepare_mutation();
    const std::string_view view(reinterpret_cast<const char*>(value.data()), value.size());
    switch (column_->Type()->GetCode()) {
        case Code::String:
            cast<clickhouse::ColumnString>(column_)->Append(view);
            break;
        case Code::JSON:
            cast<clickhouse::ColumnJSON>(column_)->Append(view);
            break;
        default:
            throw std::invalid_argument("column type does not accept string appends");
    }
    cache_.clear();
}

rust::Slice<const uint8_t> ColumnHandle::string_at(std::size_t index) const {
    switch (column_->Type()->GetCode()) {
        case Code::String: {
            const auto view = cast<clickhouse::ColumnString>(column_)->At(index);
            return rust::Slice<const uint8_t>(
                reinterpret_cast<const uint8_t*>(view.data()), view.size());
        }
        case Code::JSON: {
            const auto view = cast<clickhouse::ColumnJSON>(column_)->At(index);
            return rust::Slice<const uint8_t>(
                reinterpret_cast<const uint8_t*>(view.data()), view.size());
        }
        case Code::FixedString: {
            const auto view = cast<clickhouse::ColumnFixedString>(column_)->At(index);
            return rust::Slice<const uint8_t>(
                reinterpret_cast<const uint8_t*>(view.data()), view.size());
        }
        default:
            return item_bytes(index);
    }
}

std::size_t ColumnHandle::fixed_string_size() const {
    if (column_->Type()->GetCode() != Code::FixedString) {
        return 0;
    }
    return cast<clickhouse::ColumnFixedString>(column_)->FixedSize();
}

}  // namespace chcxx
