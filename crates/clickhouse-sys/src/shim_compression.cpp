#include "shim.h"

#include "clickhouse/base/compressed.h"
#include "clickhouse/exceptions.h"

#include <array>
#include <stdexcept>

namespace chcxx {
namespace {
constexpr size_t kMaxDecodedBytes = 256 * 1024 * 1024;

uint32_t ReadU32(const uint8_t* bytes) {
    return static_cast<uint32_t>(bytes[0]) |
           (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) |
           (static_cast<uint32_t>(bytes[3]) << 24);
}
}  // namespace

rust::Vec<uint8_t> decode_compressed_frames(rust::Slice<const uint8_t> encoded) {
    if (encoded.size() > kMaxDecodedBytes) {
        throw clickhouse::CompressionError("compressed input exceeds size limit");
    }
    rust::Vec<uint8_t> output;
    size_t offset = 0;
    while (offset < encoded.size()) {
        const auto remaining = encoded.size() - offset;
        if (remaining < 25) {
            throw clickhouse::CompressionError("truncated compressed frame header");
        }
        const auto* frame = encoded.data() + offset;
        const auto compressed = ReadU32(frame + 17);
        const auto original = ReadU32(frame + 21);
        if (compressed < 9 || compressed > remaining - 16 ||
            original > kMaxDecodedBytes - output.size()) {
            throw clickhouse::CompressionError("invalid compressed frame size");
        }
        clickhouse::ArrayInput input(frame, 16 + compressed);
        clickhouse::CompressedInput decoder(&input);
        std::array<uint8_t, 4096> buffer{};
        size_t decoded = 0;
        for (;;) {
            const auto count = decoder.Read(buffer.data(), buffer.size());
            if (count == 0) break;
            if (count > original - decoded) {
                throw clickhouse::CompressionError("decoded frame exceeds original size");
            }
            for (size_t i = 0; i < count; ++i) output.push_back(buffer[i]);
            decoded += count;
        }
        if (decoded != original || !input.Exhausted()) {
            throw clickhouse::CompressionError("decoded frame size mismatch");
        }
        offset += 16 + compressed;
    }
    return output;
}
}  // namespace chcxx
