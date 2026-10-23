#include "native_client.h"
#include "shim.h"

#include "clickhouse-sys/src/bridge.rs.h"
#include "rustls_socket.h"

#include <clickhouse/columns/numeric.h>
#include <clickhouse/exceptions.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

[[noreturn]] void rethrow_classified(const std::exception& err, bool retriable) {
    const std::string message = err.what();
    if (message.rfind("retriable:", 0) == 0 || message.rfind("fatal:", 0) == 0) {
        throw std::runtime_error(message);
    }
    const char* prefix = retriable ? "retriable: " : "fatal: ";
    throw std::runtime_error(std::string(prefix) + message);
}

template <typename F>
auto trap(F&& body) -> decltype(body()) {
    try {
        return body();
    } catch (const std::system_error& err) {
        rethrow_classified(err, true);
    } catch (const std::exception& err) {
        rethrow_classified(err, false);
    }
}

RustlsSocketFactory::Settings tls_settings(const NativeOpen& opts) {
    RustlsSocketFactory::Settings settings;
    settings.secure = opts.secure;
    settings.sni = std::string(opts.sni);
    settings.use_native_roots = opts.use_native_roots;
    settings.skip_verification = opts.skip_verification;
    settings.ca_pem.assign(opts.ca_pem.begin(), opts.ca_pem.end());
    return settings;
}

clickhouse::ClientOptions client_options(const NativeOpen& opts) {
    if (opts.extra_hosts.size() != opts.extra_ports.size()) {
        throw std::runtime_error("fatal: extra endpoint hosts and ports differ in length");
    }
    clickhouse::ClientOptions options = chcxx::base_client_options(
        std::string(opts.host), opts.port, std::string(opts.user),
        std::string(opts.password), std::string(opts.database));
    options.SetSendRetries(opts.send_retries);
    options.SetRetryTimeout(std::chrono::seconds(opts.retry_timeout_secs));
    options.SetPingBeforeQuery(opts.ping_before_query);
    options.SetConnectionConnectTimeout(std::chrono::milliseconds(opts.connect_timeout_ms));
    options.SetConnectionRecvTimeout(std::chrono::milliseconds(opts.recv_timeout_ms));
    options.SetConnectionSendTimeout(std::chrono::milliseconds(opts.send_timeout_ms));
    options.TcpNoDelay(opts.tcp_nodelay);
    options.TcpKeepAlive(opts.tcp_keepalive);
    options.SetTcpKeepAliveIdle(std::chrono::seconds(opts.keepalive_idle_secs));
    options.SetTcpKeepAliveInterval(std::chrono::seconds(opts.keepalive_interval_secs));
    options.SetTcpKeepAliveCount(opts.keepalive_count);
    try {
        chcxx::apply_compression(options, opts.compression);
    } catch (const std::exception& error) {
        // The managed dialer classifies failures by the "fatal:" prefix; an
        // unknown compression tag is a programming error, not a retriable dial.
        throw std::runtime_error(std::string("fatal: ") + error.what());
    }

    std::vector<clickhouse::Endpoint> extras;
    extras.reserve(opts.extra_hosts.size());
    for (size_t i = 0; i < opts.extra_hosts.size(); ++i) {
        clickhouse::Endpoint endpoint;
        endpoint.host = std::string(opts.extra_hosts[i]);
        endpoint.port = opts.extra_ports[i];
        extras.push_back(std::move(endpoint));
    }
    if (!extras.empty()) {
        options.SetEndpoints(std::move(extras));
    }
    return options;
}

}  // namespace

rust::Vec<std::uint8_t> factory_echo(const EchoOptions& opts) {
    return trap([&] {
        NativeOpen open;
        open.host = opts.host;
        open.port = opts.port;
        open.secure = opts.secure;
        open.sni = opts.sni;
        open.ca_pem = opts.ca_pem;
        open.use_native_roots = opts.use_native_roots;
        open.skip_verification = opts.skip_verification;
        open.connect_timeout_ms = opts.connect_timeout_ms;
        open.recv_timeout_ms = opts.recv_timeout_ms;
        open.send_timeout_ms = opts.send_timeout_ms;
        open.tcp_nodelay = opts.tcp_nodelay;
        open.tcp_keepalive = opts.tcp_keepalive;
        open.keepalive_idle_secs = opts.keepalive_idle_secs;
        open.keepalive_interval_secs = opts.keepalive_interval_secs;
        open.keepalive_count = opts.keepalive_count;
        open.user = rust::String("default");
        open.password = rust::String();
        open.database = rust::String("default");

        clickhouse::ClientOptions options = client_options(open);
        RustlsSocketFactory factory(tls_settings(open));
        clickhouse::Endpoint endpoint;
        endpoint.host = std::string(opts.host);
        endpoint.port = opts.port;
        auto socket = factory.connect(options, endpoint);
        auto output = socket->makeOutputStream();
        auto input = socket->makeInputStream();

        const std::uint8_t* cursor = opts.payload.data();
        size_t left = opts.payload.size();
        while (left > 0) {
            const size_t wrote = output->Write(cursor, left);
            cursor += wrote;
            left -= wrote;
        }
        output->Flush();

        rust::Vec<std::uint8_t> echoed;
        echoed.reserve(opts.payload.size());
        std::uint8_t tmp[1024];
        while (echoed.size() < opts.payload.size()) {
            const size_t need = std::min(sizeof(tmp), opts.payload.size() - echoed.size());
            const size_t got = input->Read(tmp, need);
            for (size_t i = 0; i < got; ++i) {
                echoed.push_back(tmp[i]);
            }
        }
        return echoed;
    });
}

namespace chcxx {
Client::Client(std::unique_ptr<clickhouse::Client> client) : inner_(std::move(client)) {}

std::unique_ptr<Client> new_native_client(const NativeOpen& opts) {
    return trap([&] {
        auto factory = std::make_unique<RustlsSocketFactory>(tls_settings(opts));
        auto native = std::make_unique<clickhouse::Client>(client_options(opts), std::move(factory));
        return std::make_unique<Client>(std::move(native));
    });
}

void reset_connection_endpoint(Client& client) {
    trap([&] { client.inner().ResetConnectionEndpoint(); return 0; });
}
rust::String current_host(const Client& client) {
    return rust::String(client.inner().GetCurrentEndpoint().host);
}
std::uint16_t current_port(const Client& client) {
    return client.inner().GetCurrentEndpoint().port;
}
}
