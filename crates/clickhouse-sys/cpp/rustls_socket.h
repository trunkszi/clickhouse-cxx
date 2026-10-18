#pragma once

#include <clickhouse/base/socket.h>
#include <rust/cxx.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct TlsSession;

struct TlsConnect;

/// Socket factory whose streams read and write through a Rust rustls session.
class RustlsSocketFactory : public clickhouse::SocketFactory {
public:
    struct Settings {
        bool secure = false;
        std::string sni;
        std::vector<std::uint8_t> ca_pem;
        bool use_native_roots = true;
        bool skip_verification = false;
    };

    explicit RustlsSocketFactory(Settings settings);
    ~RustlsSocketFactory() override;

    std::unique_ptr<clickhouse::SocketBase> connect(
        const clickhouse::ClientOptions& opts,
        const clickhouse::Endpoint& endpoint) override;

private:
    Settings settings_;
};

class RustlsSocket : public clickhouse::SocketBase {
public:
    explicit RustlsSocket(rust::Box<TlsSession> session);
    ~RustlsSocket() override;

    std::unique_ptr<clickhouse::InputStream> makeInputStream() const override;
    std::unique_ptr<clickhouse::OutputStream> makeOutputStream() const override;

private:
    std::shared_ptr<rust::Box<TlsSession>> session_;
};
