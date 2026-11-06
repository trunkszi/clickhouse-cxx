#pragma once

#include <clickhouse/client.h>
#include "shim.h"
#include <rust/cxx.h>

#include <cstdint>
#include <memory>

struct NativeOpen;
struct EchoOptions;

/// Write `EchoOptions::payload` through `RustlsSocketFactory` and read it back.
rust::Vec<std::uint8_t> factory_echo(const EchoOptions& opts);

namespace chcxx {
class Client;
std::unique_ptr<Client> new_native_client(const NativeOpen& opts);
}
