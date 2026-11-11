#pragma once
//
// parallel_reader.hpp
//
// Parallel, memory-capped MCAP message reader for TIMESTAMP order (log-time,
// forward and reverse). Decompresses chunks on a thread pool ahead of the merge
// frontier, then emits messages in exactly the same order as the serial
// IndexedMessageReader (verified against it by the parity tests).
//
// Design:
//   * Up front: filter chunks to overlapping + topic-selected, sort by start time
//     (forward) / end time (reverse); compute the residency profile and resolve a
//     byte budget for the ByteSemaphore.
//   * The CONSUMER thread owns budget decisions (so force-vs-block is race-free):
//       - REQUIRED chunks (start <= current frontier) -> forceAcquire (may exceed
//         the cap; never blocks; this is what guarantees progress / no deadlock).
//       - PREFETCH chunks (further ahead) -> tryAcquire (respects the cap; if the
//         budget is full we simply don't prefetch yet -> back-pressure).
//   * Workers only decompress: read compressed bytes from a concurrent
//     IReadable (ConcurrentFileReader), DecompressAll into an owned buffer, parse
//     the trailing MessageIndex records into a sorted, filtered entry list, fulfill
//     a promise. Budget is released when the ReadyChunk is destroyed.
//   * A k-way merge over per-chunk cursors emits the global (timestamp,
//     RecordOffset) order, reusing mcap::RecordOffset's operators so tie-breaks
//     match the serial reader exactly.
//
// Declare MCAP_NO_PARALLEL to omit the parallel reader entirely.
// Follow the library convention: #define MCAP_IMPLEMENTATION exactly once
// (before including mcap.hpp) to compile the function bodies in parallel_reader.inl.
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

struct ParallelReadOptions {
  ReadMessageOptions read;   // startTime/endTime/topicFilter/readOrder
  unsigned threadCount = 0;  // 0 -> 4 workers (default); any value is capped at 8
  // Byte budget: cap resident decompressed bytes (hard portable ceiling).
  // maxBytesInFlight == 0 means "no user cap" (effective budget = floor + lookahead).
  uint64_t maxBytesInFlight = 0;
  uint64_t lookaheadBytes = 0;  // prefetch headroom above the floor; 0 -> auto
  MemoryCapPolicy capPolicy =
    MemoryCapPolicy::Adapt;  // sub-floor behavior: exceed the cap rather than deadlock
  // Reject a chunk whose declared uncompressed size exceeds this (corruption /
  // decompression-bomb guard). 0 disables the check. Default 2 GiB.
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

struct PMsgEntry {
  Timestamp timestamp;
  ByteOffset offset;       // offset of the Message record within the decompressed chunk
  ByteOffset chunkOffset;  // chunk start offset in the file (for RecordOffset tie-break)
};

// A decompressed chunk plus its filtered, sorted message entries. Releases its
// budget credits back to the semaphore on destruction (RAII).
struct ReadyChunk {
  RawByteArray bytes;
  std::vector<PMsgEntry> entries;
  Status status;
  std::shared_ptr<ByteSemaphore> sem;
  std::shared_ptr<ParallelStats> stats;
  uint64_t budgetHeld = 0;
  uint64_t liveBytesAccounted = 0;
  ~ReadyChunk();
};
using ReadyChunkPtr = std::shared_ptr<ReadyChunk>;

struct ChunkPlan {
  Timestamp startTime = 0;
  Timestamp endTime = 0;
  ByteOffset chunkStartOffset = 0;
  ByteOffset messageIndexEndOffset = 0;
  uint64_t uncompressedSize = 0;
};

// Canonical emit order: true if message A should be emitted before B.
// Single source of truth for ordering — used by both per-chunk entry sort and
// the k-way merge heap so the two can never disagree.
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
                      const ProblemCallback& onProblem);
  ~ParallelMessageView();

  ParallelMessageView(const ParallelMessageView&) = delete;
  ParallelMessageView& operator=(const ParallelMessageView&) = delete;

  Status status() const;

  // Diagnostic counters (chunk counts, peak resident decompressed bytes).
  const internal::ParallelStats& stats() const;

  // ---- iterator ---------------------------------------------------------
  struct Iterator {
    using iterator_category = std::input_iterator_tag;
    using value_type = MessageView;
    using difference_type = std::ptrdiff_t;
    using pointer = const MessageView*;
    using reference = const MessageView&;

    Iterator() = default;
    explicit Iterator(ParallelMessageView& v);

    reference operator*() const;
    pointer operator->() const;

    // Opaque, non-owning handle to the buffer that backs the current message's
    // bytes. Advanced/optional: lock() succeeds while the iterator is positioned
    // on this message (the bytes are alive), and returns empty after operator++
    // or after the ParallelReader is destroyed. Callers MUST NOT persistently
    // store the locked shared_ptr: pinning a chunk past the reader's byte-budget
    // eviction grows memory unboundedly.
    std::weak_ptr<const void> currentBuffer() const;

    Iterator& operator++();
    void operator++(int);

    friend bool operator==(const Iterator& a, const Iterator& b);
    friend bool operator!=(const Iterator& a, const Iterator& b);

  private:
    ParallelMessageView* view_ = nullptr;
  };

  Iterator begin();
  Iterator end();

private:
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

  std::function<bool(const HeapItem&, const HeapItem&)> makeHeapComparator();
  void init();
  void runChunkJob(size_t planIdx, uint64_t budgetHeld,
                   std::shared_ptr<std::promise<internal::ReadyChunkPtr>> prom);
  void orderEntries(std::vector<internal::PMsgEntry>& entries,
                    const std::vector<std::pair<size_t, size_t>>& runs) const;
  Status decompressInto(const Chunk& chunk, internal::RawByteArray& out);
  bool scheduleChunk(size_t planIdx, bool force);
  void prefetch();
  void addCursor(size_t planIdx);
  bool produceNext();

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
  size_t nextPlanToPrefetch_ = 0;

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
  internal::ReadyChunkPtr pinned_;

  std::unordered_map<ChannelId, ChannelPtr> chanById_;
  std::unordered_map<SchemaId, SchemaPtr> schemaById_;
  SchemaPtr emptySchema_;
  std::atomic<bool> cancelled_{false};
  Status status_;
};

// A self-contained multithreaded MCAP reader. It COMPOSES a McapReader (used via
// its public API for the summary, channels, schemas, and chunk index) and OWNS the
// concurrent-safe source it reads from.
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
  // (pread/ReadFile) and parse its summary. This is the common entry point.
  Status open(std::string_view path);

  // Open against a caller-provided source. It MUST support concurrent reads
  // (source.supportsConcurrentRead() == true). The caller retains ownership
  // of `concurrentSource` (it must outlive this reader).
  Status open(IReadable& concurrentSource);

  void close();

  // Iterate messages in the requested order, decompressing chunks on a thread
  // pool ahead of the merge frontier. Output matches the serial reader exactly.
  ParallelMessageView readMessages(const ProblemCallback& onProblem,
                                   const ParallelReadOptions& options);

  McapReader& reader();
  const std::optional<Statistics>& statistics() const;
  std::unordered_map<ChannelId, ChannelPtr> channels() const;
  std::unordered_map<SchemaId, SchemaPtr> schemas() const;
  const std::vector<ChunkIndex>& chunkIndexes() const;

private:
  McapReader reader_;
  std::unique_ptr<IReadable> ownedSource_;
  IReadable* source_ = nullptr;
};

}  // namespace mcap

#ifdef MCAP_IMPLEMENTATION
#  include "parallel_reader.inl"
#endif
