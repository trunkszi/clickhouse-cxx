#include "rustls_socket.h"

#include "clickhouse-sys/src/bridge.rs.h"

#include <clickhouse/exceptions.h>

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace {

[[noreturn]] void throw_socket_error(const rust::Error& error) {
    const std::string message = error.what();
    if (message.rfind("fatal:", 0) == 0) {
        throw std::runtime_error(message);
    }
    throw std::system_error(std::make_error_code(std::errc::io_error), message);
}

std::uint64_t non_negative_ms(std::chrono::milliseconds value) {
    const auto count = value.count();
    if (count < 0) {
        return 0;
    }
    return static_cast<std::uint64_t>(count);
}

class RustlsInput : public clickhouse::InputStream {
public:
    explicit RustlsInput(std::shared_ptr<rust::Box<TlsSession>> session)
        : session_(std::move(session)) {}

    ~RustlsInput() override = default;

protected:
    bool Skip(size_t bytes) override {
        std::uint8_t tmp[512];
        while (bytes > 0) {
            const size_t chunk = std::min(bytes, sizeof(tmp));
            const size_t got = DoRead(tmp, chunk);
            bytes -= got;
        }
        return true;
    }

    size_t DoRead(void* buf, size_t len) override {
        if (len == 0) {
            return 0;
        }
        try {
            const size_t n = tls_read(
                **session_,
                rust::Slice<std::uint8_t>(static_cast<std::uint8_t*>(buf), len));
            if (n == 0) {
                throw clickhouse::ProtocolError("connection closed by peer while reading");
            }
            return n;
        } catch (const clickhouse::ProtocolError&) {
            throw;
        } catch (const rust::Error& err) {
            throw_socket_error(err);
        }
    }

private:
    std::shared_ptr<rust::Box<TlsSession>> session_;
};

class RustlsOutput : public clickhouse::OutputStream {
public:
    explicit RustlsOutput(std::shared_ptr<rust::Box<TlsSession>> session)
        : session_(std::move(session)) {}

    ~RustlsOutput() override = default;

protected:
    void DoFlush() override {
        try {
            tls_flush(**session_);
        } catch (const rust::Error& err) {
            throw_socket_error(err);
        }
    }

    size_t DoWrite(const void* data, size_t len) override {
        if (len == 0) {
            return 0;
        }
        try {
            const size_t n = tls_write(
                **session_,
                rust::Slice<const std::uint8_t>(static_cast<const std::uint8_t*>(data), len));
            if (n == 0) {
                throw std::system_error(
                    std::make_error_code(std::errc::io_error),
                    "rustls write returned no bytes");
            }
            return n;
        } catch (const std::system_error&) {
            throw;
        } catch (const rust::Error& err) {
            throw_socket_error(err);
        }
    }

private:
    std::shared_ptr<rust::Box<TlsSession>> session_;
};

}  // namespace

RustlsSocketFactory::RustlsSocketFactory(Settings settings)
    : settings_(std::move(settings)) {}

RustlsSocketFactory::~RustlsSocketFactory() = default;

std::unique_ptr<clickhouse::SocketBase> RustlsSocketFactory::connect(
    const clickhouse::ClientOptions& opts,
    const clickhouse::Endpoint& endpoint) {
    TlsConnect request;
    request.host = rust::String(endpoint.host);
    request.port = endpoint.port;
    request.secure = settings_.secure;
    request.sni = rust::String(settings_.sni);
    request.use_native_roots = settings_.use_native_roots;
    request.skip_verification = settings_.skip_verification;
    request.connect_timeout_ms = non_negative_ms(opts.connection_connect_timeout);
    request.recv_timeout_ms = non_negative_ms(opts.connection_recv_timeout);
    request.send_timeout_ms = non_negative_ms(opts.connection_send_timeout);
    request.tcp_nodelay = opts.tcp_nodelay;
    request.tcp_keepalive = opts.tcp_keepalive;
    request.keepalive_idle_secs = static_cast<std::uint64_t>(
        std::max<std::int64_t>(opts.tcp_keepalive_idle.count(), 0));
    request.keepalive_interval_secs = static_cast<std::uint64_t>(
        std::max<std::int64_t>(opts.tcp_keepalive_intvl.count(), 0));
    request.keepalive_count = static_cast<std::uint32_t>(opts.tcp_keepalive_cnt);
    request.ca_pem.reserve(settings_.ca_pem.size());
    for (const std::uint8_t byte : settings_.ca_pem) {
        request.ca_pem.push_back(byte);
    }

    try {
        return std::make_unique<RustlsSocket>(tls_connect(request));
    } catch (const rust::Error& err) {
        throw_socket_error(err);
    }
}

RustlsSocket::RustlsSocket(rust::Box<TlsSession> session)
    : session_(std::make_shared<rust::Box<TlsSession>>(std::move(session))) {}

RustlsSocket::~RustlsSocket() = default;

std::unique_ptr<clickhouse::InputStream> RustlsSocket::makeInputStream() const {
    return std::make_unique<RustlsInput>(session_);
}

std::unique_ptr<clickhouse::OutputStream> RustlsSocket::makeOutputStream() const {
    return std::make_unique<RustlsOutput>(session_);
}
