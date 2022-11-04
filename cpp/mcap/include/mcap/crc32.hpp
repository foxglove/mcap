#include <array>
#include <cstddef>
#include <cstdint>

namespace mcap::internal {

static constexpr uint32_t CRC32_POLYNOMIAL = 0xedb88320;

/**
 * Initialize a CRC32 to all 1 bits.
 */
static constexpr uint32_t CRC32_INIT = 0xffffffff;

/**
 * Update a streaming CRC32 calculation.
 */
inline uint32_t crc32Update(const uint32_t prev, const std::byte* const data, const size_t length) {
  // Process bits one by one.
  uint32_t r = prev;
  for (size_t i = 0; i < length; i++) {
    uint8_t x = uint8_t(data[i]);
    for (size_t j = 0; j < 8; j++) {
      r = (((x ^ r) & 1) * CRC32_POLYNOMIAL) ^ (r >> 1);
      x >>= 1;
    }
  }
  return r;
}

/** Finalize a CRC32 by inverting the output value. */
inline uint32_t crc32Final(uint32_t crc) {
  return crc ^ 0xffffffff;
}

}  // namespace mcap::internal
