#include "shim.h"

#include "clickhouse-sys/src/lib.rs.h"

#include <chrono>
#include <stdexcept>
#include <string>
#include <string_view>

namespace chcxx {

Client::Client(rust::Str host, uint16_t port, rust::Str user, rust::Str password,
               rust::Str database, std::uint8_t compression, std::uint64_t timeout_ms) {
    clickhouse::ClientOptions options = base_client_options(
        std::string(host), port, std::string(user), std::string(password),
        std::string(database));
    options.SetRethrowException(true)
        .SetConnectionConnectTimeout(std::chrono::milliseconds(timeout_ms))
        .SetConnectionRecvTimeout(std::chrono::milliseconds(timeout_ms))
        .SetConnectionSendTimeout(std::chrono::milliseconds(timeout_ms));
    apply_compression(options, compression);
    inner_ = std::make_unique<clickhouse::Client>(options);
}

std::unique_ptr<Client> new_client_options(rust::Str host, uint16_t port, rust::Str user,
                                           rust::Str password, rust::Str database,
                                           std::uint8_t compression, std::uint64_t timeout_ms) {
    return std::make_unique<Client>(host, port, user, password, database, compression,
                                    timeout_ms);
}

void reset(Client& client) { client.inner().ResetConnection(); }

rust::String block_column_type(const Block& block, size_t index) {
    return rust::String(block.inner().At(index)->Type()->GetName());
}

NativeQuery::NativeQuery(rust::Str sql, rust::Str id)
    : value(std::string(sql), std::string(id)) {}

std::unique_ptr<NativeQuery> new_query(rust::Str sql, rust::Str id) {
    return std::make_unique<NativeQuery>(sql, id);
}

void set_setting(NativeQuery& query, rust::Str key, rust::Str value, std::uint64_t flags) {
    query.value.SetSetting(std::string(key), {std::string(value), flags});
}

void set_param(NativeQuery& query, rust::Str key, rust::Str value, bool is_null) {
    query.value.SetParam(
        std::string(key),
        is_null ? std::nullopt : std::optional<std::string>(std::string(value)));
}

void set_tracing(NativeQuery& query, std::uint64_t high, std::uint64_t low,
                 std::uint64_t span, rust::Str state, std::uint8_t flags) {
    clickhouse::open_telemetry::TracingContext trace;
    trace.trace_id = {high, low};
    trace.span_id = span;
    trace.tracestate = std::string(state);
    trace.trace_flags = flags;
    query.value.SetTracingContext(std::move(trace));
}

bool Client::emit(const NativeEvent& event) {
    if (!callback || !context) throw std::logic_error("server event outside query scope");
    return (*callback)(context, event);
}

EventScope::EventScope(Client& c, std::uint8_t* ctx, const EventCallback& cb) : client(c) {
    if (c.context || !ctx) throw std::logic_error("invalid or reentrant query callback scope");
    c.context = ctx;
    c.callback = &cb;
}

EventScope::~EventScope() {
    client.callback = nullptr;
    client.context = nullptr;
}

void install_events(Client& client, NativeQuery& query, std::uint8_t mask) {
    // OnProgress/OnProfile cannot signal cancellation by returning a value, so
    // a rejected event must unwind through the client; swallowing it would
    // keep transferring rows after the Rust callback asked to stop.
    if (mask & 1) query.value.OnProgress([&client](const clickhouse::Progress& progress) {
        NativeEvent event;
        event.kind = 1;
        event.progress = progress;
        if (!client.emit(event)) {
            throw std::runtime_error("query cancelled by progress callback");
        }
    });
    if (mask & 2) query.value.OnProfile([&client](const clickhouse::Profile& profile) {
        NativeEvent event;
        event.kind = 2;
        event.profile = profile;
        if (!client.emit(event)) {
            throw std::runtime_error("query cancelled by profile callback");
        }
    });
    if (mask & 4) query.value.OnProfileEvents([&client](const clickhouse::Block& block) {
        NativeEvent event;
        event.kind = 3;
        event.block = block;
        return client.emit(event);
    });
    if (mask & 8) query.value.OnServerLog([&client](const clickhouse::Block& block) {
        NativeEvent event;
        event.kind = 4;
        event.block = block;
        return client.emit(event);
    });
    // Preserve typed errors even when no application exception callback is set.
    query.value.OnException([&client](const clickhouse::Exception& error) {
        NativeEvent event;
        event.kind = 5;
        event.exception = error;
        client.emit(event);
    });
}

void begin_select(Client& client, NativeQuery& query, std::uint8_t mask,
                  std::uint8_t* context, EventCallback callback) {
    EventScope scope(client, context, callback);
    install_events(client, query, mask);
    client.inner().BeginSelect(query.value);
}

std::unique_ptr<Block> next_block(Client& client, std::uint8_t* context,
                                  EventCallback callback) {
    EventScope scope(client, context, callback);
    auto result = client.inner().NextBlock();
    if (!result) return nullptr;
    return std::make_unique<Block>(*result);
}

void cancel(Client& client, std::uint8_t* context, EventCallback callback) {
    EventScope scope(client, context, callback);
    client.inner().Cancel();
}

bool is_selecting(const Client& client) { return client.inner().IsSelecting(); }

std::unique_ptr<Block> begin_insert(Client& client, const NativeQuery& query) {
    auto block = client.inner().BeginInsert(query.value);
    return std::make_unique<Block>(block);
}

void send_insert_block(Client& client, const Block& block) {
    client.inner().SendInsertBlock(block.inner());
}

void end_insert(Client& client) { client.inner().EndInsert(); }

bool is_inserting(const Client& client) { return client.inner().IsInserting(); }

void add_external(Client& client, rust::Str name, const Block& block) {
    client.external_names.emplace_back(name.data(), name.size());
    const std::string& owned = client.external_names.back();
    client.external.push_back({std::string_view(owned.data(), owned.size()), block.inner()});
}

void select_external(Client& client, NativeQuery& query, std::uint8_t mask,
                     std::uint8_t* context, EventCallback callback) {
    EventScope scope(client, context, callback);
    install_events(client, query, mask);
    query.value.OnDataCancelable([&client](const clickhouse::Block& block) {
        if (block.GetColumnCount() == 0) return true;
        NativeEvent event;
        event.block = block;
        return client.emit(event);
    });
    client.inner().SelectWithExternalData(query.value, client.external);
}

void clear_external(Client& client) {
    client.external.clear();
    client.external_names.clear();
}

ServerInfo get_server_info(const Client& client) {
    const auto& info = client.inner().GetServerInfo();
    return {rust::String(info.name), rust::String(info.timezone),
        rust::String(info.display_name), info.version_major, info.version_minor,
        info.version_patch, info.revision};
}

Version get_version() {
    const auto version = clickhouse::Client::GetVersion();
    return {version.major, version.minor, version.patch, version.build,
        rust::String(version.extra)};
}

std::uint8_t event_kind(const NativeEvent& event) { return event.kind; }

EventFields event_fields(const NativeEvent& event) {
    EventFields fields{};
    if (event.kind == 1) {
        fields.rows = event.progress.rows;
        fields.bytes = event.progress.bytes;
        fields.total_rows = event.progress.total_rows;
        fields.written_rows = event.progress.written_rows;
        fields.written_bytes = event.progress.written_bytes;
    } else {
        fields.rows = event.profile.rows;
        fields.bytes = event.profile.bytes;
        fields.blocks = event.profile.blocks;
        fields.rows_before_limit = event.profile.rows_before_limit;
        fields.applied_limit = event.profile.applied_limit;
        fields.calculated_rows_before_limit = event.profile.calculated_rows_before_limit;
    }
    return fields;
}

ExceptionFields event_exception(const NativeEvent& event) {
    return {event.exception.code, rust::String(event.exception.name),
        rust::String(event.exception.display_text), rust::String(event.exception.stack_trace)};
}

std::unique_ptr<Block> event_block(const NativeEvent& event) {
    return std::make_unique<Block>(event.block);
}

} // namespace chcxx
