#pragma once
//
// parallel_reader.hpp
//
// Parallel, memory-capped MCAP message reader for TIMESTAMP order (log-time,
// forward and reverse). Decompresses chunks on a thread pool ahead of the merge
// frontier, then emits messages in exactly the same order as the serial
// IndexedMessageReader (verified against it by the parity tests).
//
// Design (see the design doc for the full rationale):
//   * Up front: filter chunks to overlapping + topic-selected, sort by start time
//     (forward) / end time (reverse); compute the residency profile and resolve a
//     byte budget for the ByteSemaphore.
//   * The CONSUMER thread owns budget decisions (so force-vs-block is race-free):
//       - REQUIRED chunks (start <= current frontier) -> forceAcquire (may exceed
//         the cap; never blocks; this is what guarantees progress / no deadlock).
//       - PREFETCH chunks (further ahead) -> tryAcquire (respects the cap; if the
//         budget is full we simply don't prefetch yet -> back-pressure).
//   * Workers only decompress: read compressed bytes from a concurrent
//     IReadable (ConcurrentFileReader), DecompressAll into an owned buffer, parse the
//     trailing MessageIndex records into a sorted, filtered entry list, fulfill a
//     promise. Budget is released when the ReadyChunk is destroyed (drained +
//     unpinned).
//   * A k-way merge over per-chunk cursors emits the global (timestamp,
//     RecordOffset) order, reusing mcap::RecordOffset's operators so tie-breaks
//     match the serial reader exactly.
//
// Header-only inline for now; in the fork, split into .hpp/.inl behind
// MCAP_IMPLEMENTATION like the rest of the library.
//
#include "byte_semaphore.hpp"
#include "concurrent_file_reader.hpp"  // ConcurrentFileReader: the default open(path) source
#include "parallel_budget.hpp"
#include "reader.hpp"  // McapReader, IReadable, RecordReader, etc. (included by mcap.hpp before us)
#include "thread_pool.hpp"
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <new>
#include <optional>
#include <queue>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#ifndef MCAP_COMPRESSION_NO_ZSTD
#  include <zstd.h>
#endif

namespace mcap {

// Back-pressure policy for the parallel reader's resident decompressed memory.
enum class MemoryCapMode {
  ByteBudget,  // default: cap resident decompressed BYTES (precise memory ceiling)
  ChunkCount,  // opt-in: cap the NUMBER of concurrently-live chunks (coarser bound)
};

struct ParallelReadOptions {
  ReadMessageOptions read;   // startTime/endTime/topicFilter/readOrder
  unsigned threadCount = 0;  // 0 -> 4 workers (default); any value is capped at 8
  // Memory back-pressure mode. ByteBudget (default) bounds resident bytes
  // precisely (a hard, portable ceiling -- matters for general use and WASM);
  // ChunkCount bounds the number of live chunks instead (simpler, a touch faster
  // on chunk-dense layouts, but a coarser ~cap*max-chunk-size memory bound).
  MemoryCapMode memoryCap = MemoryCapMode::ByteBudget;
  // ChunkCount mode only: max concurrently-live (decompressed) chunks. 0 -> 2 *
  // topics (channels in the file). REQUIRED frontier chunks bypass the cap via
  // forceAcquire, so it only throttles prefetch and never deadlocks the merge.
  unsigned maxLiveChunks = 0;
  // ByteBudget mode only:
  uint64_t maxBytesInFlight = 0;  // soft cap; 0 -> floor + lookahead (unbounded-ish)
  uint64_t lookaheadBytes = 0;    // prefetch headroom above the floor; 0 -> auto
  MemoryCapPolicy capPolicy =
    MemoryCapPolicy::Adapt;  // sub-floor behavior: exceed the cap rather than deadlock
  // Reject a chunk whose declared uncompressed size exceeds this (corruption /
  // decompression-bomb guard). 0 disables the check. Default 2 GiB: no legitimate
  // MCAP chunk approaches this.
  uint64_t maxChunkUncompressedSize = 2ull << 30;
};

namespace internal {

// Allocator that DEFAULT-initializes elements (no value-init), so resize() on a
// byte buffer does NOT zero-fill memory we're about to overwrite with
// decompressed bytes. (std::vector<std::byte>::resize would memset to 0 first.)
template <class T>
struct NoInitAllocator {
  using value_type = T;
  NoInitAllocator() noexcept = default;
  template <class U>
  NoInitAllocator(const NoInitAllocator<U>&) noexcept {}
  template <class U>
  struct rebind {
    using other = NoInitAllocator<U>;
  };
  T* allocate(std::size_t n) {
    return static_cast<T*>(::operator new(n * sizeof(T)));
  }
  void deallocate(T* p, std::size_t) noexcept {
    ::operator delete(p);
  }
  template <class U, class... Args>
  void construct(U* p, Args&&... args) {
    ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
  }
  template <class U>
  void construct(U* p) noexcept {
    // Guard against accidental rebind to a non-trivial type: default-init
    // would call its default ctor and we'd lose the no-init semantics that
    // exist only to skip zeroing decompression buffers.
    static_assert(std::is_trivial_v<U>, "NoInitAllocator is safe only for trivial element types");
    ::new (static_cast<void*>(p)) U;  // default-init: leaves bytes uninitialized
  }
};
template <class A, class B>
bool operator==(const NoInitAllocator<A>&, const NoInitAllocator<B>&) noexcept {
  return true;
}
template <class A, class B>
bool operator!=(const NoInitAllocator<A>&, const NoInitAllocator<B>&) noexcept {
  return false;
}

// Decompressed-chunk buffer: same layout as ByteArray, but resize() leaves new
// bytes uninitialized (the decompressor overwrites every byte immediately).
using RawByteArray = std::vector<std::byte, NoInitAllocator<std::byte>>;

#ifndef MCAP_COMPRESSION_NO_ZSTD
// One reusable ZSTD_DCtx per worker thread, avoiding a context allocate/free for
// every chunk (the one-shot ZSTD_decompress creates and destroys one each call).
// Thread-local because a DCtx is stateful and not safe to share concurrently.
inline ZSTD_DCtx* threadLocalZstdDCtx() {
  struct Holder {
    ZSTD_DCtx* ctx = ZSTD_createDCtx();
    ~Holder() {
      if (ctx) ZSTD_freeDCtx(ctx);
    }
  };
  thread_local Holder holder;
  return holder.ctx;
}
#endif

// Diagnostic counters for the parallel engine. Shared (shared_ptr) between the
// view and every ReadyChunk so decompressed-byte accounting can be decremented
// from ~ReadyChunk -- the point where that memory is actually freed.
struct ParallelStats {
  std::atomic<uint64_t> chunksDecompressed{0};    // decompress jobs that produced bytes
  std::atomic<uint64_t> chunksScheduled{0};       // schedule attempts that ran a job
  std::atomic<uint64_t> chunksForced{0};          // scheduled via forceAcquire (required path)
  std::atomic<int64_t> liveDecompressedBytes{0};  // currently-resident decompressed bytes
  std::atomic<int64_t> peakDecompressedBytes{0};  // high-water mark of the above

  void addLive(uint64_t n) {
    const int64_t now = liveDecompressedBytes.fetch_add(int64_t(n)) + int64_t(n);
    int64_t prev = peakDecompressedBytes.load(std::memory_order_relaxed);
    while (now > prev && !peakDecompressedBytes.compare_exchange_weak(prev, now)) {
    }
  }
  void subLive(uint64_t n) {
    liveDecompressedBytes.fetch_sub(int64_t(n));
  }
};

// A decompressed chunk plus its filtered, sorted message entries. Releases its
// budget credits back to the semaphore on destruction (RAII), which is how
// "drained + unpinned" frees memory and relieves back-pressure.
struct PMsgEntry {
  Timestamp timestamp;
  ByteOffset offset;       // offset of the Message record within the decompressed chunk
  ByteOffset chunkOffset;  // chunk start offset in the file (for RecordOffset tie-break)
};

struct ReadyChunk {
  RawByteArray bytes;              // decompressed payload (uninitialized resize)
  std::vector<PMsgEntry> entries;  // in emit order (sorted by the active comparator)
  Status status;
  std::shared_ptr<ByteSemaphore> sem;    // shared so the semaphore outlives every chunk
  std::shared_ptr<ParallelStats> stats;  // shared diagnostic accounting
  uint64_t budgetHeld = 0;
  uint64_t liveBytesAccounted = 0;  // decompressed bytes counted into stats->live
  ~ReadyChunk() {
    if (stats && liveBytesAccounted != 0) {
      stats->subLive(liveBytesAccounted);
    }
    if (sem && budgetHeld != 0) {
      sem->release(budgetHeld);
    }
  }
};
using ReadyChunkPtr = std::shared_ptr<ReadyChunk>;

struct ChunkPlan {
  Timestamp startTime = 0;
  Timestamp endTime = 0;
  ByteOffset chunkStartOffset = 0;
  ByteOffset messageIndexEndOffset = 0;
  uint64_t uncompressedSize = 0;
};

// Canonical emit order: true if message A should be emitted before B, comparing by
// timestamp and then RecordOffset (tie-break), flipped for reverse. This is the
// SINGLE source of truth for ordering in the parallel reader -- used by both the
// per-chunk entry sort (entryLess) and the k-way merge heap (makeHeapComparator),
// so the two can never disagree. The RecordOffset tie-break itself is shared with
// the serial reader via RecordOffset's comparison operators.
inline bool messageOrderLess(Timestamp tsA, RecordOffset roA, Timestamp tsB, RecordOffset roB,
                             bool reverse) {
  if (tsA != tsB) {
    return reverse ? (tsA > tsB) : (tsA < tsB);
  }
  return reverse ? (roA > roB) : (roA < roB);
}

inline bool entryLess(const PMsgEntry& a, const PMsgEntry& b, bool reverse) {
  return messageOrderLess(a.timestamp, RecordOffset{a.offset, a.chunkOffset}, b.timestamp,
                          RecordOffset{b.offset, b.chunkOffset}, reverse);
}

}  // namespace internal

class ParallelMessageView {
public:
  ParallelMessageView(McapReader& reader, IReadable* source, const ParallelReadOptions& opts,
                      const ProblemCallback& onProblem)
      : reader_(reader)
      , source_(source)
      , opts_(opts)
      , onProblem_(onProblem)
      , reverse_(opts.read.readOrder == ReadMessageOptions::ReadOrder::ReverseLogTimeOrder)
      , heap_(makeHeapComparator()) {
    init();
  }

  ~ParallelMessageView() {
    cancelled_.store(true);  // make queued workers bail instead of decompressing
    if (pool_) pool_->shutdown();
  }

  ParallelMessageView(const ParallelMessageView&) = delete;
  ParallelMessageView& operator=(const ParallelMessageView&) = delete;

  Status status() const {
    return status_;
  }

  // Diagnostic counters (chunk counts, peak resident decompressed bytes).
  const internal::ParallelStats& stats() const {
    return *stats_;
  }

  // ---- iterator ---------------------------------------------------------
  struct Iterator {
    using iterator_category = std::input_iterator_tag;
    using value_type = MessageView;
    using difference_type = std::ptrdiff_t;
    using pointer = const MessageView*;
    using reference = const MessageView&;

    Iterator() = default;
    explicit Iterator(ParallelMessageView& v)
        : view_(&v) {
      ++(*this);  // prime first message
    }

    reference operator*() const {
      return *view_->curView_;
    }
    pointer operator->() const {
      return &*view_->curView_;
    }

    // Opaque, non-owning handle to the buffer that backs the current message's
    // bytes (`(*it).message.data` points inside it). Advanced/optional: most
    // consumers read `message.data` directly during iteration and never call
    // this. It exists for consumers that want to *defer* reading a message:
    // lock() succeeds while the iterator is positioned on this message (the
    // bytes are alive), and returns empty after operator++ or after the
    // ParallelReader is destroyed (the bytes are gone — re-read from the file).
    //
    // The handle is type-erased to `const void` on purpose: callers get a
    // liveness/ownership token, not access to reader internals. A consumer
    // pairs the locked anchor with the already-public `message.data`/`dataSize`
    // to form a zero-copy view valid for as long as it holds the anchor.
    //
    // Callers MUST NOT persistently store the locked shared_ptr: pinning a
    // chunk past the reader's byte-budget eviction grows memory unboundedly.
    std::weak_ptr<const void> currentBuffer() const {
      if (!view_) {
        return {};
      }
      return view_->pinned_;
    }

    Iterator& operator++() {
      if (view_ && !view_->produceNext()) {
        view_ = nullptr;  // exhausted -> equals end()
      }
      return *this;
    }
    void operator++(int) {
      ++(*this);
    }

    friend bool operator==(const Iterator& a, const Iterator& b) {
      return a.view_ == b.view_;
    }
    friend bool operator!=(const Iterator& a, const Iterator& b) {
      return !(a == b);
    }

  private:
    ParallelMessageView* view_ = nullptr;
  };

  Iterator begin() {
    if (!status_.ok()) return end();
    return Iterator{*this};
  }
  Iterator end() {
    return Iterator{};
  }

private:
  // ---- a live cursor over one ready chunk -------------------------------
  struct Cursor {
    internal::ReadyChunkPtr chunk;
    size_t idx = 0;
    bool live() const {
      return chunk && idx < chunk->entries.size();
    }
    const internal::PMsgEntry& head() const {
      return chunk->entries[idx];
    }
  };

  struct HeapItem {
    Timestamp timestamp;
    RecordOffset ro;
    size_t cursorId;
  };

  std::function<bool(const HeapItem&, const HeapItem&)> makeHeapComparator() {
    const bool reverse = reverse_;
    // priority_queue is a max-heap: "true means a is LOWER priority than b", i.e. a
    // emits AFTER b, i.e. b emits before a. Reuse the one ordering definition.
    return [reverse](const HeapItem& a, const HeapItem& b) {
      return internal::messageOrderLess(b.timestamp, b.ro, a.timestamp, a.ro, reverse);
    };
  }

  void init() {
    // The engine reads the same source from many worker threads at once, so it
    // only works on a source whose read() has no shared mutable cursor/buffer.
    if (source_ == nullptr || !source_->supportsConcurrentRead()) {
      status_ = Status{StatusCode::InvalidMessageReadOptions,
                       "parallel reading requires a source that supports concurrent reads "
                       "(open the ParallelReader on a file path or a concurrent-read source)"};
      onProblem_(status_);
      return;
    }
    // Ensure the summary (chunk index, channels, schemas) is available.
    if (reader_.chunkIndexes().empty()) {
      status_ = reader_.readSummary(ReadSummaryMethod::AllowFallbackScan);
      if (!status_.ok()) {
        onProblem_(status_);
        return;
      }
    }
    const auto& chunkIndexes = reader_.chunkIndexes();
    if (chunkIndexes.empty() ||
        std::all_of(chunkIndexes.begin(), chunkIndexes.end(), [](const ChunkIndex& ci) {
          return ci.messageIndexLength == 0;
        })) {
      status_ = Status{StatusCode::NoMessageIndexesAvailable,
                       "cannot read in time order without message indexes"};
      onProblem_(status_);
      return;
    }

    // Selected channels (topic filter).
    for (const auto& [channelId, channel] : reader_.channels()) {
      if (!opts_.read.topicFilter || opts_.read.topicFilter(channel->topic)) {
        selectedChannels_.insert(channelId);
      }
    }

    // Build the plan list: chunks overlapping the time range that carry a
    // selected channel, sorted by start time (forward) / end time (reverse).
    for (const auto& ci : chunkIndexes) {
      if (ci.messageStartTime >= opts_.read.endTime) continue;
      if (ci.messageEndTime < opts_.read.startTime) continue;
      bool hasSelected = false;
      for (const auto& [channelId, _off] : ci.messageIndexOffsets) {
        if (selectedChannels_.count(channelId) != 0) {
          hasSelected = true;
          break;
        }
      }
      if (!hasSelected) continue;
      if (opts_.maxChunkUncompressedSize != 0 &&
          ci.uncompressedSize > opts_.maxChunkUncompressedSize) {
        status_ = Status{StatusCode::DecompressionSizeMismatch,
                         "chunk uncompressedSize (" + std::to_string(ci.uncompressedSize) +
                           " B) exceeds maxChunkUncompressedSize; possible corruption"};
        onProblem_(status_);
        return;
      }
      internal::ChunkPlan plan;
      plan.startTime = ci.messageStartTime;
      plan.endTime = ci.messageEndTime;
      plan.chunkStartOffset = ci.chunkStartOffset;
      plan.messageIndexEndOffset = ci.chunkStartOffset + ci.chunkLength + ci.messageIndexLength;
      plan.uncompressedSize = ci.uncompressedSize;
      plans_.push_back(plan);
    }
    std::sort(plans_.begin(), plans_.end(),
              [this](const internal::ChunkPlan& a, const internal::ChunkPlan& b) {
                return reverse_ ? (a.endTime > b.endTime) : (a.startTime < b.startTime);
              });

    scheduled_.assign(plans_.size(), false);
    futures_.resize(plans_.size());

    // Snapshot channels/schemas by id once for const-ref lookup in produceNext.
    // reader_.channels()/schemas() return maps BY VALUE, so copy them a single time
    // here rather than paying a copy (and shared_ptr refcount) for every message.
    chanById_ = reader_.channels();
    schemaById_ = reader_.schemas();

    // ---- Memory back-pressure: ByteBudget (default) or ChunkCount (opt-in) ----
    // Both reuse the same ByteSemaphore; the UNIT differs (bytes vs chunks, see
    // scheduleChunk). REQUIRED frontier chunks use forceAcquire in either mode, so
    // the k-way merge never deadlocks regardless of the cap.
    if (opts_.memoryCap == MemoryCapMode::ChunkCount) {
      // Cap the number of concurrently-live chunks. Use the TOTAL topic count (not
      // the selected subset): a single-topic filtered read must still get ample
      // prefetch depth, so the cap can't collapse to 2 just because one topic is
      // selected. Default 2 * topics; overridable via opts.maxLiveChunks.
      const unsigned numTopics = static_cast<unsigned>(reader_.channels().size());
      const unsigned liveCap =
        opts_.maxLiveChunks != 0 ? opts_.maxLiveChunks : std::max(2u * std::max(numTopics, 1u), 2u);
      sem_ = std::make_shared<internal::ByteSemaphore>(static_cast<uint64_t>(liveCap));
    } else {
      // Precise byte budget: profile worst-case residency and resolve an effective
      // byte cap (floor + lookahead). This is the portable hard memory ceiling.
      const auto profile = computeResidencyProfile(chunkIndexes, selectedChannels_);
      uint64_t lookahead = opts_.lookaheadBytes;
      if (lookahead == 0) {
        const unsigned t =
          opts_.threadCount ? opts_.threadCount : std::thread::hardware_concurrency();
        lookahead = uint64_t(std::max(1u, t)) * (profile.uMaxBytes ? profile.uMaxBytes : 1);
      }
      budget_ = resolveBudget(profile, opts_.read.readOrder, opts_.maxBytesInFlight,
                              opts_.capPolicy, lookahead);
      if (budget_.fallBackToSerial || !budget_.feasibleWithoutEviction) {
        // Caller asked for a regime this engine won't honor silently. Surface it;
        // the caller can fall back to McapReader::readMessages.
        status_ = Status{StatusCode::InvalidMessageReadOptions, budget_.note};
        onProblem_(status_);
        return;
      }
      sem_ = std::make_shared<internal::ByteSemaphore>(
        std::max<uint64_t>(budget_.effectiveBudgetBytes, 1));
    }
    // Worker count: default 4 (most reads are consumer/merge-bound, where ~4
    // decompress workers keep the single consumer fed); HARD CAP 8 (decompression
    // saturates memory bandwidth around there, so >8 only adds contention and
    // regresses -- confirmed on both small-message and point-cloud workloads);
    // never exceed the core count. An explicit threadCount overrides the default
    // but is still capped at 8.
    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) {
      hw = 8;
    }
    const unsigned cap = std::min(8u, hw);
    unsigned workers = opts_.threadCount == 0 ? 4u : opts_.threadCount;
    workers = std::min(workers, cap);
    if (workers == 0) {
      workers = 1;
    }
    pool_ = std::make_unique<internal::ThreadPool>(workers);
  }

  // Decompress + parse one chunk on a worker. `budgetHeld` was already acquired
  // by the consumer (force or try), so workers never touch the semaphore.
  void runChunkJob(size_t planIdx, uint64_t budgetHeld,
                   std::shared_ptr<std::promise<internal::ReadyChunkPtr>> prom) {
    auto rc = std::make_shared<internal::ReadyChunk>();
    rc->sem = sem_;  // shared ownership: semaphore outlives this chunk
    rc->stats = stats_;
    rc->budgetHeld = budgetHeld;

    // Fast-bail for queued jobs once the view is being torn down, so early exit
    // (e.g. interactive scrubbing) doesn't wait for the whole prefetch window.
    if (cancelled_.load(std::memory_order_relaxed)) {
      rc->status = Status{StatusCode::ReadFailed, "cancelled"};
      prom->set_value(std::move(rc));
      return;
    }

    // Any throw in decompression/parsing/allocation is converted to a Status; the
    // promise is ALWAYS fulfilled so the consumer can never hang on get().
    try {
      const auto& plan = plans_[planIdx];
      RecordReader rr(*source_, plan.chunkStartOffset, plan.messageIndexEndOffset);
      bool gotChunk = false;
      // Each MessageIndex contributes one contiguous, per-channel run of entries.
      // We record the run boundaries so orderEntries() can k-way MERGE the already-
      // sorted runs (O(N log C)) instead of a full O(N log N) std::sort.
      std::vector<std::pair<size_t, size_t>> runs;
      for (auto rec = rr.next(); rec.has_value(); rec = rr.next()) {
        if (!rr.status().ok()) {
          rc->status = rr.status();
          break;
        }
        if (rec->opcode == OpCode::Chunk) {
          Chunk chunk;
          rc->status = McapReader::ParseChunk(*rec, &chunk);
          if (!rc->status.ok()) break;
          rc->status = decompressInto(chunk, rc->bytes);
          if (!rc->status.ok()) break;
          gotChunk = true;
        } else if (rec->opcode == OpCode::MessageIndex) {
          MessageIndex mi;
          rc->status = McapReader::ParseMessageIndex(*rec, &mi);
          if (!rc->status.ok()) break;
          if (selectedChannels_.count(mi.channelId) == 0) continue;
          const size_t runStart = rc->entries.size();
          for (const auto& [ts, off] : mi.records) {
            if (ts >= opts_.read.startTime && ts < opts_.read.endTime) {
              rc->entries.push_back({ts, off, plan.chunkStartOffset});
            }
          }
          if (rc->entries.size() > runStart) {
            runs.emplace_back(runStart, rc->entries.size());
          }
        }
      }
      if (rc->status.ok() && !gotChunk) {
        rc->status = Status{StatusCode::InvalidChunkOffset, "no chunk record at planned offset"};
      }
      if (rc->status.ok()) {
        orderEntries(rc->entries, runs);
        // Account the decompressed payload as resident until ~ReadyChunk frees it.
        rc->liveBytesAccounted = rc->bytes.size();
        stats_->addLive(rc->liveBytesAccounted);
        stats_->chunksDecompressed.fetch_add(1, std::memory_order_relaxed);
        // Source-file pages backing the compressed bytes are released
        // automatically by the source: ConcurrentFileReader keeps only small
        // per-thread read buffers, so there is no source-side RSS to bound.
      }
    } catch (const std::exception& e) {
      rc->bytes.clear();
      rc->entries.clear();
      rc->status = Status{StatusCode::DecompressionFailed,
                          std::string("exception decompressing chunk: ") + e.what()};
    } catch (...) {
      rc->bytes.clear();
      rc->entries.clear();
      rc->status = Status{StatusCode::DecompressionFailed, "unknown exception decompressing chunk"};
    }
    prom->set_value(std::move(rc));
  }

  // Put a chunk's entries into emit order. Each per-channel MessageIndex run is
  // already monotonic in (timestamp, offset) for any well-formed file, so a k-way
  // MERGE of the runs is O(N log C) -- cheaper than a full O(N log N) std::sort
  // (profiling showed the per-chunk sort was a leading consumer-side cost). Falls
  // back to a full sort for reverse reads, a single/zero run, or if any run turns
  // out not to be monotonic (so a misbehaving writer can never break ordering).
  void orderEntries(std::vector<internal::PMsgEntry>& entries,
                    const std::vector<std::pair<size_t, size_t>>& runs) const {
    const bool reverse = reverse_;
    auto less = [reverse](const internal::PMsgEntry& a, const internal::PMsgEntry& b) {
      return internal::entryLess(a, b, reverse);
    };
    if (reverse || runs.size() <= 1) {
      if (!std::is_sorted(entries.begin(), entries.end(), less)) {
        std::sort(entries.begin(), entries.end(), less);
      }
      return;
    }
    for (const auto& r : runs) {
      if (!std::is_sorted(entries.begin() + static_cast<std::ptrdiff_t>(r.first),
                          entries.begin() + static_cast<std::ptrdiff_t>(r.second), less)) {
        std::sort(entries.begin(), entries.end(), less);
        return;
      }
    }
    // All runs sorted -> k-way merge them (offsets are unique within a chunk, so the
    // order is a strict total order: the merge result is identical to std::sort).
    std::vector<internal::PMsgEntry> merged;
    merged.reserve(entries.size());
    struct Node {
      size_t idx;
      size_t end;
    };
    auto worse = [&](const Node& a, const Node& b) {
      // Max-heap: the run whose head sorts EARLIER must surface first, so a node is
      // "lower priority" when its head sorts after the other's.
      return less(entries[b.idx], entries[a.idx]);
    };
    std::priority_queue<Node, std::vector<Node>, decltype(worse)> pq(worse);
    for (const auto& r : runs) {
      if (r.first < r.second) pq.push(Node{r.first, r.second});
    }
    while (!pq.empty()) {
      const Node n = pq.top();
      pq.pop();
      merged.push_back(entries[n.idx]);
      if (n.idx + 1 < n.end) pq.push(Node{n.idx + 1, n.end});
    }
    entries.swap(merged);
  }

  Status decompressInto(const Chunk& chunk, internal::RawByteArray& out) {
    const auto comp = McapReader::ParseCompression(chunk.compression);
    if (!comp.has_value()) {
      return Status{StatusCode::UnrecognizedCompression, "unrecognized: " + chunk.compression};
    }
    if (*comp == Compression::None) {
      out.assign(chunk.records, chunk.records + chunk.uncompressedSize);
      return StatusCode::Success;
    }
#ifndef MCAP_COMPRESSION_NO_ZSTD
    if (*comp == Compression::Zstd) {
      // Reuse a per-thread DCtx and decompress into an uninitialized buffer (the
      // resize below does not zero-fill, since zstd overwrites every byte).
      out.resize(chunk.uncompressedSize);
      ZSTD_DCtx* decompressCtx = internal::threadLocalZstdDCtx();
      if (decompressCtx == nullptr) {
        out.clear();
        return Status{StatusCode::DecompressionFailed, "ZSTD_createDCtx failed"};
      }
      const size_t n = ZSTD_decompressDCtx(decompressCtx, out.data(), out.size(), chunk.records,
                                           chunk.compressedSize);
      if (ZSTD_isError(n)) {
        out.clear();
        return Status{StatusCode::DecompressionFailed,
                      std::string("zstd decompression failed: ") + ZSTD_getErrorName(n)};
      }
      if (n != chunk.uncompressedSize) {
        out.clear();
        return Status{StatusCode::DecompressionSizeMismatch,
                      "zstd: decompressed size does not match declared uncompressedSize"};
      }
      return StatusCode::Success;
    }
#endif
#ifndef MCAP_COMPRESSION_NO_LZ4
    if (*comp == Compression::Lz4) {
      // LZ4Reader writes into a std::vector<std::byte>; decompress into a temp and
      // copy across. LZ4 is not the optimized path here (correctness over speed).
      ByteArray tmp;
      LZ4Reader lz4;  // per-job instance: LZ4Reader is not thread-safe to share
      const Status s =
        lz4.decompressAll(chunk.records, chunk.compressedSize, chunk.uncompressedSize, &tmp);
      if (!s.ok()) return s;
      out.assign(tmp.begin(), tmp.end());
      return StatusCode::Success;
    }
#endif
    return Status{StatusCode::UnsupportedCompression, "unsupported: " + chunk.compression};
  }

  // Schedule chunk `planIdx` for decompression if not already scheduled.
  // `force` -> forceAcquire (required chunk, may exceed cap). Otherwise tryAcquire
  // (prefetch) and return false if the budget is full.
  bool scheduleChunk(size_t planIdx, bool force) {
    if (scheduled_[planIdx]) return true;
    const uint64_t need = opts_.memoryCap == MemoryCapMode::ChunkCount
                            ? uint64_t{1}                        // one credit per chunk
                            : plans_[planIdx].uncompressedSize;  // bytes for the byte budget
    if (force) {
      sem_->forceAcquire(need);
    } else if (!sem_->tryAcquire(need)) {
      return false;  // budget full -> back-pressure, try again later
    }
    auto prom = std::make_shared<std::promise<internal::ReadyChunkPtr>>();
    futures_[planIdx] = prom->get_future();
    scheduled_[planIdx] = true;
    stats_->chunksScheduled.fetch_add(1, std::memory_order_relaxed);
    if (force) stats_->chunksForced.fetch_add(1, std::memory_order_relaxed);
    pool_->submit([this, planIdx, need, prom] {
      runChunkJob(planIdx, need, prom);
    });
    return true;
  }

  // Opportunistically prefetch chunks ahead of the cursor frontier, within budget.
  // Resumes from a monotonic cursor so it never re-walks the already-scheduled
  // prefix: the old version restarted at nextPlanToCursor_ on every message,
  // re-scanning the whole ~budget-wide scheduled window (O(window) per message),
  // which profiling showed was the dominant consumer-thread cost.
  void prefetch() {
    while (nextPlanToPrefetch_ < plans_.size()) {
      if (scheduled_[nextPlanToPrefetch_]) {
        ++nextPlanToPrefetch_;  // already scheduled (here or force-scheduled); skip for good
        continue;
      }
      if (!scheduleChunk(nextPlanToPrefetch_, /*force=*/false)) {
        break;  // budget full -> retry this same index next time
      }
      ++nextPlanToPrefetch_;
    }
  }

  void addCursor(size_t planIdx) {
    // Block until this (already-scheduled) chunk is decompressed.
    internal::ReadyChunkPtr rc = futures_[planIdx].get();
    if (!rc->status.ok()) {
      status_ = rc->status;
      onProblem_(status_);
      return;
    }
    cursors_.push_back(Cursor{rc, 0});
    const size_t cursorId = cursors_.size() - 1;
    if (cursors_[cursorId].live()) {
      const auto& e = cursors_[cursorId].head();
      heap_.push(HeapItem{e.timestamp, RecordOffset{e.offset, e.chunkOffset}, cursorId});
    }
  }

  // Produce the next message in order. Returns false when exhausted.
  bool produceNext() {
    if (!status_.ok()) return false;
    curView_.reset();

    for (;;) {
      prefetch();

      // Admission: ensure every chunk that could contain a message at or before
      // (forward) / at or after (reverse) the current heap frontier has a cursor.
      // Required chunks are force-scheduled so they never block on the budget.
      while (nextPlanToCursor_ < plans_.size()) {
        const auto& plan = plans_[nextPlanToCursor_];
        bool required;
        if (heap_.empty()) {
          required = true;  // need at least the next chunk to know the frontier
        } else {
          const Timestamp frontier = heap_.top().timestamp;
          required = reverse_ ? (plan.endTime >= frontier) : (plan.startTime <= frontier);
        }
        if (!required) break;
        scheduleChunk(nextPlanToCursor_, /*force=*/true);
        addCursor(nextPlanToCursor_);
        if (!status_.ok()) return false;
        nextPlanToCursor_++;
      }

      if (heap_.empty()) {
        pinned_.reset();  // exhausted: drop the last pinned chunk so budget/memory return to 0
        return false;     // nothing left
      }

      const HeapItem top = heap_.top();
      heap_.pop();
      Cursor& cur = cursors_[top.cursorId];
      const internal::PMsgEntry entry = cur.head();

      // Pin the backing chunk for the message we're about to emit so its bytes
      // (curMessage_.data points into them) stay valid until we advance to a
      // DIFFERENT chunk. Re-pinning only on a chunk change avoids an atomic
      // shared_ptr inc/dec for every message within a chunk -- profiling showed
      // that per-message refcount churn was a leading consumer-side cost. Dropping
      // the cursor's own reference when it drains (below) stays safe: pinned_ holds
      // this chunk until the next chunk's first message rotates it out.
      if (pinned_.get() != cur.chunk.get()) {
        pinned_ = cur.chunk;
      }

      // Read the message out of the decompressed chunk (zero-copy into rc->bytes).
      BufferReader br;
      br.reset(cur.chunk->bytes.data(), cur.chunk->bytes.size(), cur.chunk->bytes.size());
      RecordReader rr(br, entry.offset, cur.chunk->bytes.size());
      auto rec = rr.next();
      if (!rr.status().ok() || !rec.has_value() || rec->opcode != OpCode::Message) {
        status_ = Status{StatusCode::InvalidRecord, "expected a message record in chunk"};
        onProblem_(status_);
        return false;
      }
      status_ = McapReader::ParseMessage(*rec, &curMessage_);
      if (!status_.ok()) {
        onProblem_(status_);
        return false;
      }

      // Advance the cursor; re-push its next head if any. When a cursor is
      // exhausted, release the chunk it held: pinned_ still owns the chunk backing
      // the message we're emitting, so dropping cursors_'s reference here makes the
      // ReadyChunk destruct once pinned_ rotates on the next ++ -- which returns
      // its bytes to the ByteSemaphore so prefetch can keep filling the window.
      // Without this, cursors_ retains every chunk for the whole iteration: the
      // budget is never reclaimed, prefetch dies, and resident memory grows to the
      // entire uncompressed dataset.
      cur.idx++;
      if (cur.live()) {
        const auto& e = cur.head();
        heap_.push(HeapItem{e.timestamp, RecordOffset{e.offset, e.chunkOffset}, top.cursorId});
      } else {
        cur.chunk.reset();
      }

      // Resolve channel/schema from one-time snapshots by const-ref, so emitting a
      // message does NOT copy a shared_ptr just to look them up. (reader_.channel()/
      // schema() return shared_ptr BY VALUE -- profiling showed that per-message
      // atomic refcount churn was a leading consumer-side cost.) The only refcount
      // left is the unavoidable copy into the MessageView itself.
      auto cit = chanById_.find(curMessage_.channelId);
      if (cit == chanById_.end()) {
        onProblem_(Status{StatusCode::InvalidChannelId, "message references missing channel"});
        continue;  // skip, keep going
      }
      const ChannelPtr& channel = cit->second;
      const SchemaPtr* schema = &emptySchema_;
      if (channel->schemaId != 0) {
        auto sit = schemaById_.find(channel->schemaId);
        if (sit != schemaById_.end()) schema = &sit->second;
      }
      curView_.emplace(curMessage_, channel, *schema,
                       RecordOffset{entry.offset, entry.chunkOffset});
      return true;
    }
  }

  // ---- state ------------------------------------------------------------
  McapReader& reader_;
  IReadable* source_ = nullptr;
  ParallelReadOptions opts_;
  ProblemCallback onProblem_;
  bool reverse_ = false;

  std::unordered_set<ChannelId> selectedChannels_;
  std::vector<internal::ChunkPlan> plans_;
  std::vector<bool> scheduled_;
  std::vector<std::future<internal::ReadyChunkPtr>> futures_;
  size_t nextPlanToCursor_ = 0;
  size_t nextPlanToPrefetch_ = 0;  // monotonic prefetch cursor (never re-scans scheduled prefix)

  std::shared_ptr<internal::ByteSemaphore> sem_;
  std::unique_ptr<internal::ThreadPool> pool_;
  std::shared_ptr<internal::ParallelStats> stats_ = std::make_shared<internal::ParallelStats>();
  BudgetDecision budget_;

  std::vector<Cursor> cursors_;
  std::priority_queue<HeapItem, std::vector<HeapItem>,
                      std::function<bool(const HeapItem&, const HeapItem&)>>
    heap_;

  Message curMessage_;
  std::optional<MessageView> curView_;
  internal::ReadyChunkPtr pinned_;  // keeps curMessage_.data alive until next ++

  // One-time snapshots for const-ref channel/schema resolution in produceNext (no
  // per-message shared_ptr copy on lookup). Populated once in init().
  std::unordered_map<ChannelId, ChannelPtr> chanById_;
  std::unordered_map<SchemaId, SchemaPtr> schemaById_;
  SchemaPtr emptySchema_;  // null sentinel so a missing schema returns by const-ref
  std::atomic<bool> cancelled_{false};
  Status status_;
};

// A self-contained multithreaded MCAP reader. It COMPOSES a McapReader (used via
// its public API for the summary, channels, schemas, and chunk index) and OWNS the
// concurrent-safe source it reads from. All parallel-specific surface lives here;
// the core McapReader / reader.hpp are untouched apart from the general-purpose
// IReadable::supportsConcurrentRead() capability bit.
//
//   ParallelReader pr;
//   if (!pr.open("file.mcap").ok()) { ... }
//   ParallelReadOptions opts; opts.read.readOrder = ReadOrder::LogTimeOrder;
//   for (const auto& mv : pr.readMessages(onProblem, opts)) { decode(mv); }
//
class ParallelReader {
public:
  ParallelReader() = default;
  ~ParallelReader() = default;
  ParallelReader(const ParallelReader&) = delete;
  ParallelReader& operator=(const ParallelReader&) = delete;

  // Open a file via an internally-owned, concurrent-safe positioned-read source
  // (pread/ReadFile) and parse its summary. This is the common entry point. It
  // uses ConcurrentFileReader rather than mmap: same concurrency, but it reads
  // through the page cache, avoiding mmap's major-fault storms on files near RAM
  // size. To use a different source, call
  // open(IReadable&).
  Status open(std::string_view path) {
    close();
    auto src = std::make_unique<ConcurrentFileReader>();
    Status status = src->open(path);
    if (!status.ok()) {
      return status;
    }
    status = reader_.open(*src);
    if (!status.ok()) {
      return status;
    }
    ownedSource_ = std::move(src);
    source_ = ownedSource_.get();
    return reader_.readSummary(ReadSummaryMethod::AllowFallbackScan);
  }

  // Open against a caller-provided source. It MUST support concurrent reads
  // (source.supportsConcurrentRead() == true), e.g. a ConcurrentFileReader or an in-memory
  // BufferReader; otherwise readMessages() reports an error and yields nothing.
  // The caller retains ownership of `concurrentSource` (it must outlive this).
  Status open(IReadable& concurrentSource) {
    close();
    const Status status = reader_.open(concurrentSource);
    if (!status.ok()) {
      return status;
    }
    source_ = &concurrentSource;
    return reader_.readSummary(ReadSummaryMethod::AllowFallbackScan);
  }

  void close() {
    reader_.close();
    ownedSource_.reset();
    source_ = nullptr;
  }

  // Iterate messages in the requested order, decompressing chunks on a thread pool
  // ahead of the merge frontier. Output matches the serial reader exactly. If the
  // reader is not open on a concurrent-safe source, the view yields nothing and
  // reports an error via `onProblem`.
  ParallelMessageView readMessages(const ProblemCallback& onProblem,
                                   const ParallelReadOptions& options) {
    return ParallelMessageView(reader_, source_, options, onProblem);
  }

  // Access to the composed reader's parsed summary data (valid after open()).
  McapReader& reader() {
    return reader_;
  }
  const std::optional<Statistics>& statistics() const {
    return reader_.statistics();
  }
  std::unordered_map<ChannelId, ChannelPtr> channels() const {
    return reader_.channels();
  }
  std::unordered_map<SchemaId, SchemaPtr> schemas() const {
    return reader_.schemas();
  }
  const std::vector<ChunkIndex>& chunkIndexes() const {
    return reader_.chunkIndexes();
  }

private:
  McapReader reader_;
  std::unique_ptr<IReadable> ownedSource_;  // owns a ConcurrentFileReader when open(path) is used
  IReadable* source_ = nullptr;             // the concurrent source reader_ reads from
};

}  // namespace mcap
