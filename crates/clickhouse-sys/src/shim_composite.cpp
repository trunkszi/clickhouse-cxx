#include "shim_matrix.h"

#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/bool.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/enum.h"
#include "clickhouse/columns/factory.h"
#include "clickhouse/columns/geo.h"
#include "clickhouse/columns/ip4.h"
#include "clickhouse/columns/ip6.h"
#include "clickhouse/columns/json.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/map.h"
#include "clickhouse/columns/nothing.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/time.h"
#include "clickhouse/columns/tuple.h"
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

std::shared_ptr<ColumnHandle> handle(const clickhouse::ColumnRef& column) {
    return std::make_shared<ColumnHandle>(column);
}

template <typename K, typename V>
bool append_map_typed(
    const clickhouse::ColumnRef& target,
    const clickhouse::ColumnRef& keys,
    const clickhouse::ColumnRef& values) {
    auto typed = target->As<clickhouse::ColumnMapT<K, V>>();
    if (!typed) {
        return false;
    }
    auto* key_column = dynamic_cast<K*>(keys.get());
    auto* value_column = dynamic_cast<V*>(values.get());
    if (!key_column || !value_column) {
        throw std::invalid_argument("map key/value column types do not match the map type");
    }
    if (key_column->Size() != value_column->Size()) {
        throw std::invalid_argument("map key and value columns have different row counts");
    }
    using Key = std::decay_t<decltype(std::declval<K>().At(0))>;
    using Value = std::decay_t<decltype(std::declval<V>().At(0))>;
    std::vector<std::tuple<Key, Value>> rows;
    rows.reserve(key_column->Size());
    for (std::size_t i = 0; i < key_column->Size(); ++i) {
        rows.emplace_back(key_column->At(i), value_column->At(i));
    }
    typed->Append(rows);
    return true;
}

template <typename T>
bool append_low_cardinality_string(const clickhouse::ColumnRef& target, std::string_view value) {
    auto typed = target->As<clickhouse::ColumnLowCardinalityT<T>>();
    if (!typed) {
        return false;
    }
    typed->Append(value);
    return true;
}

template <typename T>
bool append_low_cardinality_int(const clickhouse::ColumnRef& target, T value) {
    auto typed = target->As<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnVector<T>>>();
    if (!typed) {
        return false;
    }
    typed->Append(value);
    return true;
}

}  // namespace

std::shared_ptr<ColumnHandle> ColumnHandle::nested() const {
    switch (column_->Type()->GetCode()) {
        case Code::Nullable:
            return handle(cast<clickhouse::ColumnNullable>(column_)->Nested());
        case Code::Array:
            return handle(cast<clickhouse::ColumnArray>(column_)->GetData());
        default:
            return std::shared_ptr<ColumnHandle>(nullptr);
    }
}

rust::Slice<const uint8_t> ColumnHandle::nulls() const {
    if (column_->Type()->GetCode() != Code::Nullable) {
        return rust::Slice<const uint8_t>(nullptr, 0);
    }
    auto nulls = cast<clickhouse::ColumnNullable>(column_)->Nulls();
    const auto& data = cast<clickhouse::ColumnUInt8>(nulls)->GetWritableData();
    return rust::Slice<const uint8_t>(
        reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

void ColumnHandle::append_nullable_flag(bool is_null) const {
    cast<clickhouse::ColumnNullable>(column_)->Append(is_null);
}

rust::Slice<const uint64_t> ColumnHandle::array_offsets() const {
    if (column_->Type()->GetCode() != Code::Array) {
        return rust::Slice<const uint64_t>(nullptr, 0);
    }
    const auto& data = cast<clickhouse::ColumnArray>(column_)->GetOffsets()->GetWritableData();
    return rust::Slice<const uint64_t>(
        reinterpret_cast<const uint64_t*>(data.data()), data.size());
}

void ColumnHandle::array_append_row(const ColumnHandle& values) const {
    cast<clickhouse::ColumnArray>(column_)->AppendAsColumn(values.ref());
}

std::size_t ColumnHandle::tuple_len() const {
    if (column_->Type()->GetCode() != Code::Tuple) {
        return 0;
    }
    return cast<clickhouse::ColumnTuple>(column_)->TupleSize();
}

std::shared_ptr<ColumnHandle> ColumnHandle::tuple_at(std::size_t index) const {
    return handle(cast<clickhouse::ColumnTuple>(column_)->At(index));
}

void ColumnHandle::tuple_append_fixed(
    std::size_t index, rust::Slice<const uint8_t> bytes) const {
    tuple_at(index)->append_fixed(bytes);
}

void ColumnHandle::tuple_append_string(
    std::size_t index, rust::Slice<const uint8_t> value) const {
    tuple_at(index)->append_string(value);
}

void ColumnHandle::map_append_row(
    const ColumnHandle& keys, const ColumnHandle& values) const {
    if (column_->Type()->GetCode() != Code::Map) {
        throw std::invalid_argument("column is not a Map");
    }
    if (append_map_typed<clickhouse::ColumnString, clickhouse::ColumnUInt64>(
            column_, keys.ref(), values.ref()) ||
        append_map_typed<clickhouse::ColumnString, clickhouse::ColumnString>(
            column_, keys.ref(), values.ref()) ||
        append_map_typed<clickhouse::ColumnUInt64, clickhouse::ColumnUInt64>(
            column_, keys.ref(), values.ref()) ||
        append_map_typed<clickhouse::ColumnInt64, clickhouse::ColumnFloat64>(
            column_, keys.ref(), values.ref())) {
        return;
    }
    throw std::invalid_argument("unsupported Map key/value column combination");
}

std::shared_ptr<ColumnHandle> ColumnHandle::map_row(std::size_t index) const {
    return std::make_shared<ColumnHandle>(
        cast<clickhouse::ColumnMap>(column_)->GetAsColumn(index));
}

void ColumnHandle::low_cardinality_append_string(rust::Slice<const uint8_t> value) const {
    const std::string_view view(reinterpret_cast<const char*>(value.data()), value.size());
    if (append_low_cardinality_string<clickhouse::ColumnString>(column_, view) ||
        append_low_cardinality_string<clickhouse::ColumnFixedString>(column_, view)) {
        return;
    }
    throw std::invalid_argument("LowCardinality dictionary does not accept strings");
}

void ColumnHandle::low_cardinality_append_i64(std::int64_t value) const {
    if (append_low_cardinality_int<std::int8_t>(column_, static_cast<std::int8_t>(value)) ||
        append_low_cardinality_int<std::int16_t>(column_, static_cast<std::int16_t>(value)) ||
        append_low_cardinality_int<std::int32_t>(column_, static_cast<std::int32_t>(value)) ||
        append_low_cardinality_int<std::int64_t>(column_, value)) {
        return;
    }
    throw std::invalid_argument("LowCardinality dictionary does not accept signed integers");
}

void ColumnHandle::low_cardinality_append_u64(std::uint64_t value) const {
    if (append_low_cardinality_int<std::uint8_t>(column_, static_cast<std::uint8_t>(value)) ||
        append_low_cardinality_int<std::uint16_t>(column_, static_cast<std::uint16_t>(value)) ||
        append_low_cardinality_int<std::uint32_t>(column_, static_cast<std::uint32_t>(value)) ||
        append_low_cardinality_int<std::uint64_t>(column_, value)) {
        return;
    }
    throw std::invalid_argument("LowCardinality dictionary does not accept unsigned integers");
}

void ColumnHandle::geo_append_point(double x, double y) const {
    auto typed = column_->As<clickhouse::ColumnPoint>();
    if (!typed) {
        throw std::invalid_argument("column is not a Point");
    }
    typed->Append(std::make_tuple(x, y));
}

void ColumnHandle::append_default() const {
    const auto code = column_->Type()->GetCode();
    if (code == Code::Nullable) {
        auto nested_column = cast<clickhouse::ColumnNullable>(column_)->Nested();
        ColumnHandle nested(nested_column);
        nested.append_default();
        append_nullable_flag(true);
        return;
    }
    if (code == Code::Array) {
        auto* array = cast<clickhouse::ColumnArray>(column_);
        const auto item_name = array->Type()->As<clickhouse::ArrayType>()->GetItemType()->GetName();
        auto empty = clickhouse::CreateColumnByType(item_name);
        if (!empty) {
            throw std::invalid_argument("cannot create an empty Array item column");
        }
        array->AppendAsColumn(empty);
        return;
    }
    if (code == Code::Tuple) {
        auto* tuple = cast<clickhouse::ColumnTuple>(column_);
        for (std::size_t i = 0; i < tuple->TupleSize(); ++i) {
            ColumnHandle child(tuple->At(i));
            child.append_default();
        }
        return;
    }
    if (code == Code::Map) {
        auto* map_type = column_->Type()->As<clickhouse::MapType>();
        auto keys = clickhouse::CreateColumnByType(map_type->GetKeyType()->GetName());
        auto values = clickhouse::CreateColumnByType(map_type->GetValueType()->GetName());
        if (!keys || !values) {
            throw std::invalid_argument("cannot create empty Map key/value columns");
        }
        ColumnHandle key_handle(std::move(keys));
        ColumnHandle value_handle(std::move(values));
        map_append_row(key_handle, value_handle);
        return;
    }
    if (code == Code::LowCardinality) {
        auto* low_cardinality = cast<clickhouse::ColumnLowCardinality>(column_);
        switch (low_cardinality->GetNestedType()->GetCode()) {
            case Code::String:
            case Code::FixedString: {
                const std::string_view empty;
                low_cardinality_append_string(rust::Slice<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(empty.data()), empty.size()));
                return;
            }
            case Code::Nullable:
                low_cardinality_append_string(rust::Slice<const uint8_t>(nullptr, 0));
                return;
            default:
                low_cardinality_append_i64(0);
                return;
        }
    }
    if (code == Code::Point) {
        geo_append_point(0.0, 0.0);
        return;
    }
    if (code == Code::Void) {
        cast<clickhouse::ColumnNothing>(column_)->Append(std::unique_ptr<void*>(nullptr));
        return;
    }
    if (code == Code::String) {
        append_string(rust::Slice<const uint8_t>(nullptr, 0));
        return;
    }
    if (code == Code::JSON) {
        const std::string_view empty_object("{}");
        append_string(rust::Slice<const uint8_t>(
            reinterpret_cast<const uint8_t*>(empty_object.data()), empty_object.size()));
        return;
    }
    if (code == Code::Ring || code == Code::Polygon || code == Code::MultiPolygon) {
        throw std::invalid_argument("default append for nested Geo arrays is not supported");
    }
    std::size_t width = 0;
    switch (code) {
        case Code::UInt8:
        case Code::Int8:
        case Code::Bool:
        case Code::Enum8:
            width = 1;
            break;
        case Code::UInt16:
        case Code::Int16:
        case Code::Date:
        case Code::Enum16:
            width = 2;
            break;
        case Code::UInt32:
        case Code::Int32:
        case Code::Float32:
        case Code::Date32:
        case Code::DateTime:
        case Code::IPv4:
        case Code::Time:
            width = 4;
            break;
        case Code::UInt64:
        case Code::Int64:
        case Code::Float64:
        case Code::DateTime64:
        case Code::Time64:
            width = 8;
            break;
        case Code::UInt128:
        case Code::Int128:
        case Code::UUID:
        case Code::IPv6:
            width = 16;
            break;
        case Code::Decimal:
        case Code::Decimal32:
        case Code::Decimal64:
        case Code::Decimal128: {
            const auto precision = cast<clickhouse::ColumnDecimal>(column_)->GetPrecision();
            width = precision <= 9 ? 4 : (precision <= 18 ? 8 : 16);
            break;
        }
        case Code::FixedString:
            width = cast<clickhouse::ColumnFixedString>(column_)->FixedSize();
            break;
        default:
            throw std::invalid_argument("default append is not supported for this column type");
    }
    std::vector<uint8_t> zeros(width, 0);
    append_fixed(rust::Slice<const uint8_t>(zeros.data(), zeros.size()));
}

}  // namespace chcxx
