#include "shim_matrix.h"
#include "rustls_socket.h"

#include <memory>
#include <string>

#include "clickhouse/client.h"
#include "clickhouse/query.h"

namespace chcxx {

ClientHandle::ClientHandle(rust::Str host, uint16_t port)
    : client_(clickhouse::ClientOptions()
                  .SetHost(std::string(host.data(), host.size()))
                  .SetPort(port)
                  .SetDefaultDatabase("default")
                  .SetUser("default")
                  .SetPingBeforeQuery(true)
                  .SetConnectionConnectTimeout(std::chrono::milliseconds(500))
                  .SetConnectionRecvTimeout(std::chrono::milliseconds(5000)),
              std::make_unique<RustlsSocketFactory>(RustlsSocketFactory::Settings{})) {}

void ClientHandle::ping() {
    client_.Ping();
}

void ClientHandle::execute(rust::Str query) {
    client_.Execute(clickhouse::Query(std::string(query.data(), query.size())));
}

void ClientHandle::insert(rust::Str table, const BlockHandle& block) {
    client_.Insert(std::string(table.data(), table.size()), block.ref());
}

std::unique_ptr<BlockHandle> ClientHandle::select_one(rust::Str query) {
    std::unique_ptr<BlockHandle> result;
    client_.Select(
        std::string(query.data(), query.size()),
        [&result](const clickhouse::Block& block) {
            if (!result && block.GetRowCount() > 0) {
                result = std::make_unique<BlockHandle>(block);
            }
        });
    return result;
}

std::unique_ptr<ClientHandle> new_column_client(rust::Str host, uint16_t port) {
    return std::make_unique<ClientHandle>(host, port);
}

}  // namespace chcxx
