#pragma once
//
// parallel_budget.hpp
//
// Implements the memory cap for the parallel reader. Two parts:
//
//   1. computeResidencyProfile(): a sweep-line over the chunk index that, BEFORE
//      reading any message, computes the worst-case resident-memory floor for
//      time-ordered reads. For log-time order a chunk is resident for its entire
//      [messageStartTime, messageEndTime] span, so the binding constraint is the
//      maximum byte-sum of chunk intervals overlapping any instant ("stabbing
//      depth in bytes"). This is the number the cap must respect.
//
//   2. resolveBudget(): given a user cap and a read order, decides the effective
//      byte budget for the ByteSemaphore. For LOG-TIME order the effective budget
//      can never drop below the floor without deadlocking the k-way merge (a
//      required chunk that does not fit the budget can't be decompressed). The resolver
//      enforces that, per the chosen policy.
//
// Topic filtering is honored: only chunks containing a selected channel count,
// since unselected chunks are never decompressed. A chunk contributes its FULL
// uncompressedSize when counted (the whole chunk must be decompressed to read
// any selected message inside it).
//
// C++17. Header-only; no implementation macro required (pure inline helpers over
// the public ChunkIndex type).
//
#include <mcap/types.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace mcap {

// Worst-case residency characteristics derived from the chunk index.
struct ResidencyProfile {
  size_t consideredChunks = 0;     // chunks containing a selected channel
  uint64_t maxDepthChunks = 0;     // max overlapping chunk intervals at any instant
  uint64_t maxDepthBytes = 0;      // max sum of uncompressedSize overlapping any instant
  uint64_t uMaxBytes = 0;          // largest single chunk's uncompressedSize
  uint64_t totalUncompressed = 0;  // sum over considered chunks (fully-buffered upper bound)
};

namespace internal {

// A chunk counts if no filter is given, or it carries at least one selected channel.
inline bool chunkSelected(const ChunkIndex& c,
                          const std::unordered_set<ChannelId>& selectedChannels) {
  if (selectedChannels.empty()) {
    return true;
  }
  for (const auto& [channelId, _offset] : c.messageIndexOffsets) {
    if (selectedChannels.count(channelId) != 0) {
      return true;
    }
  }
  return false;
}

}  // namespace internal

inline ResidencyProfile computeResidencyProfile(
  const std::vector<ChunkIndex>& chunkIndexes,
  const std::unordered_set<ChannelId>& selectedChannels = {}) {
  ResidencyProfile p;

  // Sweep-line events. A chunk is live over the CLOSED interval
  // [messageStartTime, messageEndTime]; we expire it at messageEndTime + 1 so
  // that two chunks touching at a shared boundary are treated as overlapping
  // (conservative: never under-counts, so the cap never deadlocks). At equal
  // timestamps, expiries (-) are processed before starts (+).
  struct Event {
    Timestamp t;
    int order;  // 0 = expire (-), 1 = start (+)
    int64_t dChunks;
    int64_t dBytes;
  };
  std::vector<Event> events;
  events.reserve(chunkIndexes.size() * 2);

  for (const auto& c : chunkIndexes) {
    if (!internal::chunkSelected(c, selectedChannels)) {
      continue;
    }
    p.consideredChunks++;
    p.totalUncompressed += c.uncompressedSize;
    p.uMaxBytes = std::max(p.uMaxBytes, c.uncompressedSize);
    const Timestamp start = c.messageStartTime;
    const Timestamp expire = (c.messageEndTime == MaxTime) ? MaxTime : (c.messageEndTime + 1);
    events.push_back({start, 1, +1, int64_t(c.uncompressedSize)});
    events.push_back({expire, 0, -1, -int64_t(c.uncompressedSize)});
  }

  std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
    if (a.t != b.t) return a.t < b.t;
    return a.order < b.order;  // expiries before starts at the same instant
  });

  int64_t curChunks = 0, curBytes = 0;
  for (const auto& e : events) {
    curChunks += e.dChunks;
    curBytes += e.dBytes;
    p.maxDepthChunks = std::max<uint64_t>(p.maxDepthChunks, uint64_t(curChunks));
    p.maxDepthBytes = std::max<uint64_t>(p.maxDepthBytes, uint64_t(curBytes));
  }
  return p;
}

// Policy for what to do when the user's cap is below the order's intrinsic floor.
enum class MemoryCapPolicy {
  Adapt,                 // raise the effective budget up to the floor (never deadlocks;
                         // may exceed the user cap — flagged in BudgetDecision)
  Strict,                // do not exceed the user cap; report infeasible (caller decides)
  EvictAndReDecompress,  // honor a sub-floor cap by evicting + re-decompressing chunks
                         // (bounded redundant CPU; never below one chunk)
  FallBackToSerial,      // if the cap is below the floor, signal the caller to read serially
};

struct BudgetDecision {
  uint64_t effectiveBudgetBytes = 0;      // pass this to ByteSemaphore
  uint64_t floorBytes = 0;                // minimum to progress without eviction, this order
  bool feasibleWithoutEviction = true;    // false only under Strict when cap < floor
  bool exceedsUserCap = false;            // true under Adapt when raised above the cap
  bool requiresEviction = false;          // true under EvictAndReDecompress when cap < floor
  bool fallBackToSerial = false;          // true under FallBackToSerial when cap < floor
  double estReDecompressionFactor = 1.0;  // >1 when eviction is in play (rough)
  std::string note;
};

// order: only the log-time orders carry the overlap floor; file order needs just
// one resident chunk (its largest). userCapBytes == 0 means "no user cap".
// desiredLookaheadBytes is extra budget above the floor for prefetch (clamped by cap).
inline BudgetDecision resolveBudget(const ResidencyProfile& profile,
                                    ReadMessageOptions::ReadOrder order, uint64_t userCapBytes,
                                    MemoryCapPolicy policy = MemoryCapPolicy::Adapt,
                                    uint64_t desiredLookaheadBytes = 0) {
  const bool timeOrdered = (order == ReadMessageOptions::ReadOrder::LogTimeOrder ||
                            order == ReadMessageOptions::ReadOrder::ReverseLogTimeOrder);

  // File order: one chunk drained at a time -> floor is the largest single chunk.
  // Log/Reverse order: floor is the stabbing depth in bytes (which is always >=
  // the largest single chunk, since that chunk alone occupies its own instant).
  const uint64_t oneChunk = std::max<uint64_t>(profile.uMaxBytes, 1);
  const uint64_t floor = timeOrdered ? std::max(profile.maxDepthBytes, oneChunk) : oneChunk;

  BudgetDecision d;
  d.floorBytes = floor;

  const uint64_t unlimited = (userCapBytes == 0);

  if (unlimited) {
    d.effectiveBudgetBytes = floor + desiredLookaheadBytes;
    return d;
  }

  if (userCapBytes >= floor) {
    // Cap is feasible; use it for look-ahead up to floor + desired prefetch.
    d.effectiveBudgetBytes = std::max(floor, std::min(userCapBytes, floor + desiredLookaheadBytes));
    return d;
  }

  // userCapBytes < floor: the interesting case.
  switch (policy) {
    case MemoryCapPolicy::Adapt:
      d.effectiveBudgetBytes = floor;
      d.exceedsUserCap = true;
      d.note =
        "user cap (" + std::to_string(userCapBytes) + " B) is below the " +
        (timeOrdered ? std::string("log-time overlap floor") : std::string("largest-chunk floor")) +
        " (" + std::to_string(floor) + " B); raised to the floor to avoid deadlock";
      return d;

    case MemoryCapPolicy::Strict:
      d.effectiveBudgetBytes = userCapBytes;
      d.feasibleWithoutEviction = false;
      d.note = "user cap is below the floor (" + std::to_string(floor) +
               " B); not feasible without eviction in this order";
      return d;

    case MemoryCapPolicy::EvictAndReDecompress:
      // Must still hold at least one (largest) chunk, or even a single chunk can't fit.
      d.effectiveBudgetBytes = std::max(userCapBytes, oneChunk);
      d.requiresEviction = (d.effectiveBudgetBytes < floor);
      d.estReDecompressionFactor =
        d.requiresEviction ? double(floor) / double(d.effectiveBudgetBytes) : 1.0;
      d.note = "honoring sub-floor cap via eviction; expect ~" +
               std::to_string(d.estReDecompressionFactor) + "x redundant decompression";
      return d;

    case MemoryCapPolicy::FallBackToSerial:
      d.fallBackToSerial = true;
      d.effectiveBudgetBytes = oneChunk;
      d.note = "user cap below floor; falling back to the serial reader";
      return d;
  }
  return d;  // unreachable
}

}  // namespace mcap
