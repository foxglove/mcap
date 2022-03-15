#pragma once

#include <fmt/core.h>

#include "types.hpp"

// Do not compile on systems with non-8-bit bytes
static_assert(std::numeric_limits<unsigned char>::digits == 8);

namespace mcap {

namespace internal {

constexpr std::string_view ErrorMsgInvalidOpcode = "invalid opcode, expected {}: 0x{:02x}";
constexpr std::string_view ErrorMsgInvalidLength = "invalid {} length: {}";
constexpr std::string_view ErrorMsgInvalidMagic = "invalid magic bytes in {}: 0x{}";
constexpr std::string_view ErrorOpenFileFailed = "failed to open \"{}\"";

constexpr uint64_t MinHeaderLength = /* magic bytes */ sizeof(Magic) +
                                     /* opcode */ 1 +
                                     /* record length */ 8 +
                                     /* profile length */ 4 +
                                     /* library length */ 4;
constexpr uint64_t FooterLength = /* opcode */ 1 +
                                  /* record length */ 8 +
                                  /* summary start */ 8 +
                                  /* summary offset start */ 8 +
                                  /* summary crc */ 4 +
                                  /* magic bytes */ sizeof(Magic);

/**
 * @brief String formatting compatible with std::format(), used to construct
 * Status messages.
 */
template <typename... T>
[[nodiscard]] inline std::string StrFormat(std::string_view msg, T&&... args) {
  return fmt::format(msg, std::forward<T>(args)...);
}

inline uint32_t KeyValueMapSize(const KeyValueMap& map) {
  uint32_t size = 0;
  for (const auto& [key, value] : map) {
    size += 4 + key.size() + 4 + value.size();
  }
  return size;
}

inline const std::string CompressionString(Compression compression) {
  switch (compression) {
    case Compression::None:
    default:
      return std::string{};
    case Compression::Lz4:
      return "lz4";
    case Compression::Zstd:
      return "zstd";
  }
}

inline uint16_t ParseUint16(const std::byte* data) {
  return uint16_t(data[0]) | (uint16_t(data[1]) << 8);
}

inline uint32_t ParseUint32(const std::byte* data) {
  return uint32_t(data[0]) | (uint32_t(data[1]) << 8) | (uint32_t(data[2]) << 16) |
         (uint32_t(data[3]) << 24);
}

inline Status ParseUint32(const std::byte* data, uint64_t maxSize, uint32_t* output) {
  if (maxSize < 4) {
    const auto msg = StrFormat("cannot read uint32 from {} bytes", maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  *output = ParseUint32(data);
  return StatusCode::Success;
}

inline uint64_t ParseUint64(const std::byte* data) {
  return uint64_t(data[0]) | (uint64_t(data[1]) << 8) | (uint64_t(data[2]) << 16) |
         (uint64_t(data[3]) << 24) | (uint64_t(data[4]) << 32) | (uint64_t(data[5]) << 40) |
         (uint64_t(data[6]) << 48) | (uint64_t(data[7]) << 56);
}

inline Status ParseUint64(const std::byte* data, uint64_t maxSize, uint64_t* output) {
  if (maxSize < 8) {
    const auto msg = StrFormat("cannot read uint64 from {} bytes", maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  *output = ParseUint64(data);
  return StatusCode::Success;
}

inline Status ParseStringView(const std::byte* data, uint64_t maxSize, std::string_view* output) {
  uint32_t size = 0;
  if (auto status = ParseUint32(data, maxSize, &size); !status.ok()) {
    const auto msg = StrFormat("cannot read string size: {}", status.message);
    return Status{StatusCode::InvalidRecord, msg};
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg = StrFormat("string size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }
  *output = std::string_view(reinterpret_cast<const char*>(data + 4), size);
  return StatusCode::Success;
}

inline Status ParseString(const std::byte* data, uint64_t maxSize, std::string* output) {
  uint32_t size = 0;
  if (auto status = ParseUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg = StrFormat("string size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }
  *output = std::string(reinterpret_cast<const char*>(data + 4), size);
  return StatusCode::Success;
}

inline Status ParseByteArray(const std::byte* data, uint64_t maxSize, ByteArray* output) {
  uint32_t size = 0;
  if (auto status = ParseUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg =
      StrFormat("byte array size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }
  output->resize(size);
  std::memcpy(output->data(), data + 4, size);
  return StatusCode::Success;
}

inline Status ParseKeyValueMap(const std::byte* data, uint64_t maxSize, KeyValueMap* output) {
  uint32_t sizeInBytes = 0;
  if (auto status = ParseUint32(data, maxSize, &sizeInBytes); !status.ok()) {
    return status;
  }
  if (sizeInBytes > (maxSize - 4)) {
    const auto msg =
      StrFormat("key-value map size {} exceeds remaining bytes {}", sizeInBytes, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }

  // Account for the byte size prefix in sizeInBytes to make the bounds checking
  // below simpler
  sizeInBytes += 4;

  output->clear();
  uint64_t pos = 4;
  while (pos < sizeInBytes) {
    std::string_view key;
    if (auto status = ParseStringView(data + pos, sizeInBytes - pos, &key); !status.ok()) {
      const auto msg =
        StrFormat("cannot read key-value map key at pos {}: {}", pos, status.message);
      return Status{StatusCode::InvalidRecord, msg};
    }
    pos += 4 + key.size();
    std::string_view value;
    if (auto status = ParseStringView(data + pos, sizeInBytes - pos, &value); !status.ok()) {
      const auto msg = StrFormat("cannot read key-value map value for key \"{}\" at pos {}: {}",
                                 key, pos, status.message);
      return Status{StatusCode::InvalidRecord, msg};
    }
    pos += 4 + value.size();
    output->emplace(key, value);
  }
  return StatusCode::Success;
}

inline std::string MagicToHex(const std::byte* data) {
  return StrFormat("{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", data[0], data[1], data[2],
                   data[3], data[4], data[5], data[6], data[7]);
}

}  // namespace internal

}  // namespace mcap
