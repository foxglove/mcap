#include <array>
#include <cstddef>
#include <cstdint>

// Hardware-accelerated CRC32 for x86_64 via PCLMULQDQ carryless multiply,
// with runtime CPU detection. The SSE4.2 crc32 instruction is not usable
// here (it hardwires the Castagnoli polynomial); PCLMULQDQ folding works for
// the zlib polynomial MCAP uses. GCC and clang compile the kernel with a
// per-function target attribute; MSVC compiles vector intrinsics without any
// special flags, so it only needs the __cpuid-based dispatch.
#if (defined(__x86_64__) || defined(_M_X64)) && \
  (defined(__GNUC__) || defined(__clang__) || defined(_MSC_VER))
#  define MCAP_CRC32_PCLMUL 1
#  include <immintrin.h>
#  if defined(__GNUC__) || defined(__clang__)
#    define MCAP_CRC32_PCLMUL_TARGET __attribute__((target("pclmul,sse4.1")))
#  else
#    define MCAP_CRC32_PCLMUL_TARGET
#    include <intrin.h>
#  endif
#endif

// Hardware-accelerated CRC32 for AArch64, with runtime CPU detection. Unlike
// x86, AArch64 has dedicated CRC32 instructions for this exact polynomial
// (FEAT_CRC32, mandatory since ARMv8.1), so no folding math is needed.
// Little-endian only: the crc32x instruction consumes its 64-bit operand as
// little-endian data bytes.
#if defined(__aarch64__) && defined(__AARCH64EL__) && (defined(__GNUC__) || defined(__clang__))
#  define MCAP_CRC32_ARM 1
#  include <cstring>
#  if defined(__clang__)
// clang's arm_acle.h only declares the CRC32 intrinsics when the whole
// translation unit targets +crc, so use the always-available builtins.
#    define MCAP_CRC32_ARM_TARGET __attribute__((target("crc")))
#    define MCAP_CRC32_ARM_CRC32B __builtin_arm_crc32b
#    define MCAP_CRC32_ARM_CRC32D __builtin_arm_crc32d
#  else
#    include <arm_acle.h>
#    define MCAP_CRC32_ARM_TARGET __attribute__((target("+crc")))
#    define MCAP_CRC32_ARM_CRC32B __crc32b
#    define MCAP_CRC32_ARM_CRC32D __crc32d
#  endif
#  if defined(__linux__)
#    include <asm/hwcap.h>
#    include <sys/auxv.h>
#    ifndef HWCAP_CRC32
#      define HWCAP_CRC32 (1UL << 7)
#    endif
#  endif
#endif

namespace mcap::internal {

/**
 * Compute CRC32 lookup tables as described at:
 * https://github.com/komrad36/CRC#option-6-1-byte-tabular
 *
 * An iteration of CRC computation can be performed on 8 bits of input at once. By pre-computing a
 * table of the values of CRC(?) for all 2^8 = 256 possible byte values, during the final
 * computation we can replace a loop over 8 bits with a single lookup in the table.
 *
 * For further speedup, we can also pre-compute the values of CRC(?0) for all possible bytes when a
 * zero byte is appended. Then we can process two bytes of input at once by computing CRC(AB) =
 * CRC(A0) ^ CRC(B), using one lookup in the CRC(?0) table and one lookup in the CRC(?) table.
 *
 * The same technique applies for any number of bytes to be processed at once, although the speed
 * improvements diminish.
 *
 * @param Polynomial The binary representation of the polynomial to use (reversed, i.e. most
 * significant bit represents x^0).
 * @param NumTables The number of bytes of input that will be processed at once.
 */
template <size_t Polynomial, size_t NumTables>
struct CRC32Table {
private:
  std::array<uint32_t, 256 * NumTables> table = {};

public:
  constexpr CRC32Table() {
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t r = i;
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      r = ((r & 1) * Polynomial) ^ (r >> 1);
      table[i] = r;
    }
    for (size_t i = 256; i < table.size(); i++) {
      uint32_t value = table[i - 256];
      table[i] = table[value & 0xff] ^ (value >> 8);
    }
  }

  constexpr uint32_t operator[](size_t index) const {
    return table[index];
  }
};

inline uint32_t getUint32LE(const std::byte* data) {
  return (uint32_t(data[0]) << 0) | (uint32_t(data[1]) << 8) | (uint32_t(data[2]) << 16) |
         (uint32_t(data[3]) << 24);
}

static constexpr CRC32Table<0xedb88320, 8> CRC32_TABLE;

/**
 * Initialize a CRC32 to all 1 bits.
 */
static constexpr uint32_t CRC32_INIT = 0xffffffff;

#ifdef MCAP_CRC32_PCLMUL
/**
 * Update a streaming CRC32 calculation using PCLMULQDQ folding, following
 * "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ Instruction"
 * (Gopal et al., Intel, 2009). Fold constants match the zlib/Linux kernel
 * implementations of this CRC. Requires `length >= 64` and a CPU with
 * PCLMULQDQ and SSE4.1 (check cpuSupportsPclmul() first).
 */
MCAP_CRC32_PCLMUL_TARGET inline uint32_t crc32UpdatePclmul(const uint32_t prev,
                                                           const std::byte* const data,
                                                           const size_t length) {
  alignas(16) static const uint64_t K1K2[2] = {0x0154442bd4, 0x01c6e41596};
  alignas(16) static const uint64_t K3K4[2] = {0x01751997d0, 0x00ccaa009e};
  alignas(16) static const uint64_t K5K0[2] = {0x0163cd6124, 0x0000000000};
  alignas(16) static const uint64_t POLY[2] = {0x01db710641, 0x01f7011641};

  const __m128i* buf = reinterpret_cast<const __m128i*>(data);
  size_t remaining = length;

  // Load the initial 64 bytes into four 128-bit accumulators and fold the
  // previous CRC state into the lowest lane.
  __m128i x1 = _mm_loadu_si128(buf + 0);
  __m128i x2 = _mm_loadu_si128(buf + 1);
  __m128i x3 = _mm_loadu_si128(buf + 2);
  __m128i x4 = _mm_loadu_si128(buf + 3);
  x1 = _mm_xor_si128(x1, _mm_cvtsi32_si128(static_cast<int>(prev)));
  buf += 4;
  remaining -= 64;

  // Fold 64 bytes at a time.
  __m128i k = _mm_load_si128(reinterpret_cast<const __m128i*>(K1K2));
  while (remaining >= 64) {
    const __m128i x5 = _mm_clmulepi64_si128(x1, k, 0x00);
    const __m128i x6 = _mm_clmulepi64_si128(x2, k, 0x00);
    const __m128i x7 = _mm_clmulepi64_si128(x3, k, 0x00);
    const __m128i x8 = _mm_clmulepi64_si128(x4, k, 0x00);
    x1 = _mm_clmulepi64_si128(x1, k, 0x11);
    x2 = _mm_clmulepi64_si128(x2, k, 0x11);
    x3 = _mm_clmulepi64_si128(x3, k, 0x11);
    x4 = _mm_clmulepi64_si128(x4, k, 0x11);
    x1 = _mm_xor_si128(_mm_xor_si128(x1, x5), _mm_loadu_si128(buf + 0));
    x2 = _mm_xor_si128(_mm_xor_si128(x2, x6), _mm_loadu_si128(buf + 1));
    x3 = _mm_xor_si128(_mm_xor_si128(x3, x7), _mm_loadu_si128(buf + 2));
    x4 = _mm_xor_si128(_mm_xor_si128(x4, x8), _mm_loadu_si128(buf + 3));
    buf += 4;
    remaining -= 64;
  }

  // Fold the four accumulators into one.
  k = _mm_load_si128(reinterpret_cast<const __m128i*>(K3K4));
  __m128i x5 = _mm_clmulepi64_si128(x1, k, 0x00);
  x1 = _mm_clmulepi64_si128(x1, k, 0x11);
  x1 = _mm_xor_si128(_mm_xor_si128(x1, x5), x2);
  x5 = _mm_clmulepi64_si128(x1, k, 0x00);
  x1 = _mm_clmulepi64_si128(x1, k, 0x11);
  x1 = _mm_xor_si128(_mm_xor_si128(x1, x5), x3);
  x5 = _mm_clmulepi64_si128(x1, k, 0x00);
  x1 = _mm_clmulepi64_si128(x1, k, 0x11);
  x1 = _mm_xor_si128(_mm_xor_si128(x1, x5), x4);

  // Fold remaining 16-byte blocks.
  while (remaining >= 16) {
    x5 = _mm_clmulepi64_si128(x1, k, 0x00);
    x1 = _mm_clmulepi64_si128(x1, k, 0x11);
    x1 = _mm_xor_si128(_mm_xor_si128(x1, x5), _mm_loadu_si128(buf));
    buf += 1;
    remaining -= 16;
  }

  // Fold 128 bits to 64 bits.
  const __m128i mask32 = _mm_setr_epi32(~0, 0, ~0, 0);
  __m128i t = _mm_clmulepi64_si128(x1, k, 0x10);
  x1 = _mm_srli_si128(x1, 8);
  x1 = _mm_xor_si128(x1, t);
  k = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(K5K0));
  t = _mm_srli_si128(x1, 4);
  x1 = _mm_and_si128(x1, mask32);
  x1 = _mm_clmulepi64_si128(x1, k, 0x00);
  x1 = _mm_xor_si128(x1, t);

  // Barrett reduction from 64 bits to the final 32-bit CRC state.
  k = _mm_load_si128(reinterpret_cast<const __m128i*>(POLY));
  t = _mm_and_si128(x1, mask32);
  t = _mm_clmulepi64_si128(t, k, 0x10);
  t = _mm_and_si128(t, mask32);
  t = _mm_clmulepi64_si128(t, k, 0x00);
  x1 = _mm_xor_si128(x1, t);
  uint32_t r = static_cast<uint32_t>(_mm_extract_epi32(x1, 1));

  // Process the tail (< 16 bytes) with the lookup table.
  const std::byte* tail = reinterpret_cast<const std::byte*>(buf);
  for (size_t i = 0; i < remaining; i++) {
    r = CRC32_TABLE[(r ^ uint8_t(tail[i])) & 0xff] ^ (r >> 8);
  }
  return r;
}

inline bool cpuSupportsPclmul() {
#  if defined(_MSC_VER) && !defined(__clang__)
  static const bool supported = [] {
    int info[4] = {0, 0, 0, 0};
    __cpuid(info, 1);
    // CPUID.1:ECX bit 1 = PCLMULQDQ, bit 19 = SSE4.1
    return (info[2] & (1 << 1)) != 0 && (info[2] & (1 << 19)) != 0;
  }();
#  else
  static const bool supported =
    __builtin_cpu_supports("pclmul") && __builtin_cpu_supports("sse4.1");
#  endif
  return supported;
}
#endif

#ifdef MCAP_CRC32_ARM
/**
 * Update a streaming CRC32 calculation using the AArch64 CRC32 instructions,
 * which implement this exact polynomial in hardware, 8 bytes per instruction.
 * Requires FEAT_CRC32 (check cpuSupportsArmCrc() first).
 */
MCAP_CRC32_ARM_TARGET inline uint32_t crc32UpdateArm(const uint32_t prev,
                                                     const std::byte* const data,
                                                     const size_t length) {
  uint32_t r = prev;
  const uint8_t* p = reinterpret_cast<const uint8_t*>(data);
  size_t remaining = length;
  // Unaligned loads are fine on AArch64; std::memcpy compiles to a plain ldr.
  while (remaining >= 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    r = MCAP_CRC32_ARM_CRC32D(r, v);
    p += 8;
    remaining -= 8;
  }
  while (remaining > 0) {
    r = MCAP_CRC32_ARM_CRC32B(r, *p);
    p++;
    remaining--;
  }
  return r;
}

inline bool cpuSupportsArmCrc() {
#  if defined(__ARM_FEATURE_CRC32)
  // The whole build already targets +crc, so support is guaranteed.
  return true;
#  elif defined(__linux__)
  static const bool supported = (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
  return supported;
#  elif defined(__APPLE__)
  // FEAT_CRC32 is present on every Apple AArch64 CPU (Apple A10 and later,
  // including all Apple Silicon Macs).
  return true;
#  else
  return false;
#  endif
}
#endif

/**
 * Update a streaming CRC32 calculation.
 *
 * For performance, this implementation processes the data 8 bytes at a time, using the algorithm
 * presented at: https://github.com/komrad36/CRC#option-9-8-byte-tabular
 */
inline uint32_t crc32Update(const uint32_t prev, const std::byte* const data, const size_t length) {
  uint32_t r = prev;

  // Handle small inputs byte-by-byte, avoiding the 8-byte bulk loop below.
  if (length <= 8) {
    for (size_t i = 0; i < length; i++) {
      r = CRC32_TABLE[(r ^ uint8_t(data[i])) & 0xff] ^ (r >> 8);
    }
    return r;
  }

#ifdef MCAP_CRC32_PCLMUL
  if (length >= 64 && cpuSupportsPclmul()) {
    return crc32UpdatePclmul(prev, data, length);
  }
#endif

#ifdef MCAP_CRC32_ARM
  if (length >= 64 && cpuSupportsArmCrc()) {
    return crc32UpdateArm(prev, data, length);
  }
#endif

  // Process 8 bytes (2 uint32s) at a time.
  size_t offset = 0;
  size_t remainingBytes = length;
  for (; remainingBytes >= 8; offset += 8, remainingBytes -= 8) {
    r ^= getUint32LE(data + offset);
    uint32_t r2 = getUint32LE(data + offset + 4);
    r = CRC32_TABLE[0 * 256 + ((r2 >> 24) & 0xff)] ^ CRC32_TABLE[1 * 256 + ((r2 >> 16) & 0xff)] ^
        CRC32_TABLE[2 * 256 + ((r2 >> 8) & 0xff)] ^ CRC32_TABLE[3 * 256 + ((r2 >> 0) & 0xff)] ^
        CRC32_TABLE[4 * 256 + ((r >> 24) & 0xff)] ^ CRC32_TABLE[5 * 256 + ((r >> 16) & 0xff)] ^
        CRC32_TABLE[6 * 256 + ((r >> 8) & 0xff)] ^ CRC32_TABLE[7 * 256 + ((r >> 0) & 0xff)];
  }

  // Process any remaining bytes one by one.
  for (; offset < length; offset++) {
    r = CRC32_TABLE[(r ^ uint8_t(data[offset])) & 0xff] ^ (r >> 8);
  }
  return r;
}

/** Finalize a CRC32 by inverting the output value. */
inline uint32_t crc32Final(uint32_t crc) {
  return crc ^ 0xffffffff;
}

}  // namespace mcap::internal
