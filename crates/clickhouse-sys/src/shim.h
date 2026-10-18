#pragma once

#include "clickhouse/block.h"
#include "clickhouse/client.h"
#include "rust/cxx.h"
#include "shim_matrix.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

namespace chcxx {

struct EventFields;
struct ExceptionFields;
struct ServerInfo;
struct Version;

struct NativeEvent {
    // Data=0, progress=1, profile=2, profile events=3, log=4, exception=5.
    std::uint8_t kind = 0;
    clickhouse::Progress progress;
    clickhouse::Profile profile;
    clickhouse::Exception exception;
    clickhouse::Block block;
};
using EventCallback = rust::Fn<bool(std::uint8_t*, const NativeEvent&)>;

/// Owning wrapper around a synchronous clickhouse-cpp client.
class Client {
public:
    Client(rust::Str host, uint16_t port, rust::Str user, rust::Str password, rust::Str database);
    Client(rust::Str host, uint16_t port, rust::Str user, rust::Str password, rust::Str database,
           std::uint8_t compression, std::uint64_t timeout_ms);

    explicit Client(std::unique_ptr<clickhouse::Client> client);

    clickhouse::Client& inner();
    const clickhouse::Client& inner() const;

    std::uint8_t* context = nullptr;
    const EventCallback* callback = nullptr;
    clickhouse::ExternalTables external;
    // `ExternalTable::name` is a string_view, so the name must outlive the
    // select call. A deque keeps earlier elements at a stable address when a
    // later add_external grows the store.
    std::deque<std::string> external_names;
    bool emit(const NativeEvent& event);

private:
    std::unique_ptr<clickhouse::Client> inner_;
};

/// Connection fields shared by every client construction path.
clickhouse::ClientOptions base_client_options(const std::string& host, uint16_t port,
                                              const std::string& user,
                                              const std::string& password,
                                              const std::string& database);

/// Map the FFI compression tag to a clickhouse-cpp method.
void apply_compression(clickhouse::ClientOptions& options, std::uint8_t method);

/// A sink that exists only during one synchronous FFI call, including unwinding.
struct EventScope {
    Client& client;
    EventScope(Client& client, std::uint8_t* context, const EventCallback& callback);
    ~EventScope();
    EventScope(const EventScope&) = delete;
    EventScope& operator=(const EventScope&) = delete;
};

/// Owning wrapper around a clickhouse-cpp query.
struct NativeQuery {
    clickhouse::Query value;
    explicit NativeQuery(rust::Str sql, rust::Str id);
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
void reset_connection_endpoint(Client& client);
rust::String current_host(const Client& client);
uint16_t current_port(const Client& client);
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
rust::String block_column_type(const Block& block, size_t index);

std::unique_ptr<Client> new_client_options(rust::Str host, uint16_t port, rust::Str user,
                                           rust::Str password, rust::Str database,
                                           uint8_t compression, uint64_t timeout_ms);
void reset(Client& client);

std::unique_ptr<NativeQuery> new_query(rust::Str sql, rust::Str id);
void set_setting(NativeQuery& query, rust::Str key, rust::Str value, uint64_t flags);
void set_param(NativeQuery& query, rust::Str key, rust::Str value, bool is_null);
void set_tracing(NativeQuery& query, uint64_t high, uint64_t low, uint64_t span,
                 rust::Str state, uint8_t flags);
void install_events(Client& client, NativeQuery& query, uint8_t mask);

void begin_select(Client& client, NativeQuery& query, uint8_t mask, uint8_t* context,
                  EventCallback callback);
std::unique_ptr<Block> next_block(Client& client, uint8_t* context, EventCallback callback);
void cancel(Client& client, uint8_t* context, EventCallback callback);
bool is_selecting(const Client& client);
std::unique_ptr<Block> begin_insert(Client& client, const NativeQuery& query);
void send_insert_block(Client& client, const Block& block);
void end_insert(Client& client);
bool is_inserting(const Client& client);
void add_external(Client& client, rust::Str name, const Block& block);
void select_external(Client& client, NativeQuery& query, uint8_t mask, uint8_t* context,
                     EventCallback callback);
void clear_external(Client& client);
ServerInfo get_server_info(const Client& client);
Version get_version();

std::uint8_t event_kind(const NativeEvent& event);
EventFields event_fields(const NativeEvent& event);
ExceptionFields event_exception(const NativeEvent& event);
std::unique_ptr<Block> event_block(const NativeEvent& event);

}  // namespace chcxx
