#pragma once
//
// byte_semaphore.hpp
//
// A counting semaphore measured in BYTES, used to cap the total resident
// (decompressed) memory of the parallel reader. A worker takes
// `chunk.uncompressedSize` credits before decompressing a chunk and releases
// them once the chunk is fully drained and unpinned. The total outstanding
// credits stay at or below the configured capacity, EXCEPT for the deliberate
// oversized-chunk case described below.
//
// The cap is SOFT. There are two ways to take credits:
//
//   * tryAcquire()   - non-blocking. Returns false if `n` credits cannot be
//                      granted right now (the speculative-look-ahead path: a
//                      prefetch worker that cannot get credits just stops
//                      prefetching for now).
//   * forceAcquire() - non-blocking; ALWAYS succeeds, even if it drives the pool
//                      over capacity (the counter goes negative). Used for chunks
//                      the merge MUST decompress to emit the next in-order message,
//                      where waiting would deadlock. Exceeding the cap to avoid the
//                      lock is the intended behavior.
//
// The overshoot is bounded: the set of chunks force-acquired at once is the
// temporal overlap depth at the current emit frontier, and it self-corrects,
// because while available_ is negative every tryAcquire() returns false, so no
// new speculative memory is taken until releases bring the pool back above
// water.
//
// C++17.
//
#include <algorithm>
#include <cstdint>
#include <mutex>

namespace mcap::internal {

class ByteSemaphore {
public:
  explicit ByteSemaphore(uint64_t capacityBytes)
      : capacity_(capacityBytes)
      , available_(int64_t(capacityBytes)) {}

  // Non-blocking acquire. Returns false if the request cannot be granted right
  // now (including an oversized request when the pool is not fully free).
  bool tryAcquire(uint64_t n) {
    std::lock_guard<std::mutex> lk(m_);
    const int64_t need = int64_t(n);
    const int64_t threshold = std::min<int64_t>(need, int64_t(capacity_));
    if (available_ < threshold) {
      return false;
    }
    available_ -= need;
    return true;
  }

  void release(uint64_t n) {
    std::lock_guard<std::mutex> lk(m_);
    available_ += int64_t(n);
  }

  // Unconditionally grant `n` credits without waiting, even if this drives the
  // pool over capacity (available_ negative). For chunks the merge MUST have to
  // make forward progress; exceeding the cap here is deliberate and preferable
  // to deadlocking.
  void forceAcquire(uint64_t n) {
    std::lock_guard<std::mutex> lk(m_);
    available_ -= int64_t(n);
  }

  uint64_t capacity() const {
    return capacity_;
  }

  // Test/diagnostic only: current granted (outstanding) bytes. May briefly
  // exceed capacity when an oversized chunk is resident.
  int64_t outstanding() const {
    std::lock_guard<std::mutex> lk(m_);
    return int64_t(capacity_) - available_;
  }

private:
  const uint64_t capacity_;
  int64_t available_;  // signed: goes negative while an oversized chunk is held
  mutable std::mutex m_;
};

}  // namespace mcap::internal
