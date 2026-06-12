// message_byte_store.hpp — deferred ("lazy") per-message byte access on top of
// the parallel reader.
//
// This is an OPTIONAL layer, separate from the core ParallelReader. The common
// case — "decode each message as I iterate" — needs none of this: just read
// `mv.message.data` inside the `readMessages()` loop. This header exists for the
// niche where a consumer must read a message's bytes LATER, possibly long after
// iteration has moved on or the reader is gone (e.g. a host that ingests some
// topics eagerly and others lazily on demand).
//
// It serves two regimes from one `MessageByteFetcher` callable:
//   * HOT  — the fetcher is invoked while the iterator is still positioned on
//            the message (its chunk is still pinned). Copies the message's bytes
//            out of the worker-decompressed chunk. No re-decompression, and the
//            returned anchor pins only the message — never the whole chunk, so a
//            retained view can't keep a chunk (and its ~100 siblings) resident.
//   * COLD — the fetcher is invoked after the chunk is gone. Re-decompresses the
//            containing chunk on demand (byte-bounded, O(1) seek), under a
//            byte-budgeted LRU, and returns a MESSAGE-SIZED COPY so a retained
//            view never pins a whole decompressed chunk.
//
// The fetcher distinguishes the two by a non-owning liveness handle obtained
// from `Iterator::currentBuffer()`. This is what avoids decompressing every
// eager message twice (worker + consumer): the hot path copies out the bytes the
// worker already produced instead of re-decompressing the chunk.
//
// std-only public surface: `ByteView` carries only `{const std::byte*, size_t,
// std::shared_ptr<const void>}`, so this layer never imposes a non-std
// dependency on the core reader. The consumer adapts `ByteView`
// to its own owned-span type in one line.
//
// LIFETIME CONTRACT: a `ByteView`'s `data` pointer is valid for exactly as long
// as a copy of its `anchor` is alive. Move/return the `ByteView` whole; never
// keep `data` after dropping `anchor`.

#pragma once

#include "parallel_reader.hpp"  // ParallelMessageView::Iterator, McapReader, LinearMessageView, ChunkIndex, MessageView, Status
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mcap {

// Non-owning view of one message's payload, plus an ownership anchor that keeps
// the bytes alive. See LIFETIME CONTRACT at the top of this file. An empty
// ByteView (data == nullptr) means "could not resolve" — the caller renders no
// data for that message.
struct ByteView {
  const std::byte* data = nullptr;
  std::size_t size = 0;
  std::shared_ptr<const void> anchor;
};

namespace detail {

// Saturating add for MCAP timestamps. MaxTime is the type's upper bound; +1
// would wrap to 0 and make a half-open range degenerate.
inline Timestamp incrementEndTimeSaturating(Timestamp ts) {
  return (ts == MaxTime) ? MaxTime : ts + 1;
}

// Cold-path machinery, shared (via shared_ptr) by every fetcher the store
// produces so it outlives the store/import. Owns a lazily-opened,
// FileReader-backed McapReader and a byte-bounded LRU of decompressed chunks.
// All cold work is serialized behind mu_ because the FileReader-backed reader is
// NOT concurrent-safe (single shared cursor) and lazy pulls may arrive from
// several consumer threads. It is intentionally retained by fetchers after the
// MessageByteStore owner is destroyed: PJ4 destroys the DataSource instance at
// the end of import, but ObjectStore lazy pulls happen later.
class ColdChunkStore {
public:
  struct ChunkMeta {
    uint64_t length = 0;
    Timestamp ts_start = 0;
    Timestamp ts_end = 0;
  };

  std::string filepath_;
  std::size_t capacity_bytes_ = 0;
  std::unordered_map<uint64_t, ChunkMeta> meta_;  // chunkStartOffset -> bounds

  // Resolve one message to a message-sized owned copy. Empty on any failure;
  // failures are surfaced via stderr (NOT a callback) because this can run after
  // the owning data source is gone — a captured callback would dangle.
  ByteView fetch(uint64_t chunk_start, ByteOffset within_chunk, ChannelId channel_id,
                 Timestamp log_time) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!ensureOpenLocked()) {
      return {};
    }
    auto entry = getChunkLocked(chunk_start);
    if (!entry) {
      return {};
    }
    auto it = entry->index.find(Key{channel_id, log_time, within_chunk});
    if (it == entry->index.end()) {
      std::fprintf(stderr,
                   "[data_load_mcap] cold-path lookup miss after chunk loaded — chunk@%llu ch=%u "
                   "ts=%llu wc=%llu "
                   "(possible corruption or offset mismatch)\n",
                   static_cast<unsigned long long>(chunk_start), static_cast<unsigned>(channel_id),
                   static_cast<unsigned long long>(log_time),
                   static_cast<unsigned long long>(within_chunk));
      return {};
    }
    const auto [offset, size] = it->second;
    // Message-sized copy: a retained ByteView then pins only this message's
    // bytes, never the whole decompressed chunk (which the LRU still bounds).
    auto copy = std::make_shared<std::vector<uint8_t>>(
      entry->bytes->begin() + static_cast<std::ptrdiff_t>(offset),
      entry->bytes->begin() + static_cast<std::ptrdiff_t>(offset + size));
    const auto* base = reinterpret_cast<const std::byte*>(copy->data());
    return ByteView{base, size, std::move(copy)};
  }

private:
  using Key = std::tuple<ChannelId, Timestamp, ByteOffset>;

  struct Entry {
    std::shared_ptr<const std::vector<uint8_t>> bytes;  // whole decompressed chunk
    std::map<Key, std::pair<size_t /*offset*/, size_t /*size*/>> index;
  };
  struct MapValue {
    std::shared_ptr<const Entry> entry;
    std::list<uint64_t>::iterator lru_it;
  };

  // Lazily open + summarize the cold reader on first miss. A hard failure is
  // latched in open_error_ so we don't re-attempt a doomed open on every pull
  // (retry storm). Channel/schema resolution for the byte-bounded view below
  // requires the parsed summary, hence readSummary here.
  bool ensureOpenLocked() {
    if (open_error_) {
      return false;
    }
    if (reader_) {
      return true;
    }
    reader_ = std::make_unique<McapReader>();
    if (Status st = reader_->open(filepath_); !st.ok()) {
      std::fprintf(stderr, "[data_load_mcap] cold-path open failed for '%s' (code=%d): %s\n",
                   filepath_.c_str(), static_cast<int>(st.code), st.message.c_str());
      open_error_ = st;
      reader_.reset();
      return false;
    }
    if (Status st = reader_->readSummary(ReadSummaryMethod::AllowFallbackScan); !st.ok()) {
      std::fprintf(stderr, "[data_load_mcap] cold-path summary failed for '%s' (code=%d): %s\n",
                   filepath_.c_str(), static_cast<int>(st.code), st.message.c_str());
      open_error_ = st;
      reader_.reset();
      return false;
    }
    return true;
  }

  std::shared_ptr<const Entry> getChunkLocked(uint64_t chunk_start) {
    if (auto it = map_.find(chunk_start); it != map_.end()) {
      lru_.splice(lru_.begin(), lru_, it->second.lru_it);
      return it->second.entry;
    }
    auto m = meta_.find(chunk_start);
    if (m == meta_.end()) {
      std::fprintf(stderr, "[data_load_mcap] cold-path missing chunk metadata for chunk@%llu\n",
                   static_cast<unsigned long long>(chunk_start));
      return nullptr;
    }
    auto entry = loadChunkLocked(chunk_start, m->second);
    if (!entry) {
      return nullptr;
    }
    lru_.push_front(chunk_start);
    map_.emplace(chunk_start, MapValue{entry, lru_.begin()});
    total_bytes_ += entry->bytes->size();
    // Keep at least one entry so a chunk larger than the budget still survives
    // until the next insert displaces it.
    while (map_.size() > 1 && total_bytes_ > capacity_bytes_) {
      uint64_t victim = lru_.back();
      auto victim_it = map_.find(victim);
      total_bytes_ -= victim_it->second.entry->bytes->size();
      lru_.pop_back();
      map_.erase(victim_it);
    }
    return entry;
  }

  // Byte-bounded decompression of a single chunk: hands LinearMessageView the
  // exact [start, start+length) byte range so mcap decompresses that one chunk
  // directly — no O(chunks_before_target) linear scan. Indexed by
  // (channel_id, log_time, within_chunk_record_offset); the third element
  // distinguishes messages sharing a (channel, logTime), which MCAP permits.
  std::shared_ptr<const Entry> loadChunkLocked(uint64_t chunk_start, const ChunkMeta& meta) {
    auto buffer = std::make_shared<std::vector<uint8_t>>();
    auto entry = std::make_shared<Entry>();
    auto on_problem = [chunk_start](const Status& s) {
      std::fprintf(stderr, "[data_load_mcap] cold-path chunk@%llu mcap problem (code=%d): %s\n",
                   static_cast<unsigned long long>(chunk_start), static_cast<int>(s.code),
                   s.message.c_str());
    };
    LinearMessageView view(*reader_, chunk_start, chunk_start + meta.length, meta.ts_start,
                           incrementEndTimeSaturating(meta.ts_end), on_problem);
    for (const auto& v : view) {
      if (v.channel == nullptr || v.message.data == nullptr) {
        continue;
      }
      const auto* data = reinterpret_cast<const uint8_t*>(v.message.data);
      const size_t size = v.message.dataSize;
      const size_t offset = buffer->size();
      buffer->insert(buffer->end(), data, data + size);
      // Within-chunk record offset: the SERIAL LinearMessageView reports it in
      // messageOffset.chunkOffset (per reader.inl conventions) — note this is
      // the parallel reader's messageOffset.offset, the value the fetcher keys
      // its lookup with. value_or(0) covers the unlikely "not set" case.
      const ByteOffset within_chunk = v.messageOffset.chunkOffset.value_or(0);
      entry->index.emplace(Key{v.channel->id, v.message.logTime, within_chunk},
                           std::pair<size_t, size_t>{offset, size});
    }
    entry->bytes = std::shared_ptr<const std::vector<uint8_t>>(std::move(buffer));
    return entry;
  }

  std::mutex mu_;
  std::unique_ptr<McapReader> reader_;  // FileReader-backed; opened lazily under mu_
  std::optional<Status> open_error_;    // latched hard failure (no retry storm)
  std::list<uint64_t> lru_;             // front = MRU, keyed by chunkStartOffset
  std::unordered_map<uint64_t, MapValue> map_;
  size_t total_bytes_ = 0;
};

}  // namespace detail

// One message's deferred byte accessor. A plain callable (NOT std::function) to
// keep per-message binding cheap. The hot path copies the message out of the
// still-decompressed chunk (a message-sized alloc); the cold path re-decompresses.
// Safe to invoke after the ParallelReader is destroyed: the hot handle simply
// expires and it falls back to the cold store it shares.
class MessageByteFetcher {
public:
  ByteView operator()() const {
    // Hot: the worker-decompressed chunk is still alive. Copy THIS message out
    // of it rather than aliasing it — a retained anchor must pin one message,
    // not the whole chunk (one chunk backs ~100 messages, so a per-message
    // object topic present in every chunk, e.g. /tf, would otherwise pin the
    // entire decompressed file). The chunk is still decompressed only once, so
    // consecutive messages never re-decompress; this only adds a small memcpy.
    if (auto anchor = hot_buffer_.lock()) {
      if (hot_data_ == nullptr || size_ == 0) {
        return {};  // empty/degenerate message: nothing to copy, and don't pin the chunk
      }
      const auto* src = reinterpret_cast<const uint8_t*>(hot_data_);
      auto copy = std::make_shared<std::vector<uint8_t>>(src, src + size_);
      const auto* base = reinterpret_cast<const std::byte*>(copy->data());
      return ByteView{base, size_, std::move(copy)};
    }
    // Cold: chunk gone — re-decompress on demand (or fast-fail if torn down).
    if (!cold_) {
      return {};
    }
    return cold_->fetch(chunk_start_, within_chunk_, channel_id_, log_time_);
  }

private:
  friend class MessageByteStore;
  std::weak_ptr<const void> hot_buffer_;  // type-erased pinned chunk; empty -> cold
  const std::byte* hot_data_ = nullptr;   // valid iff hot_buffer_.lock() succeeds
  std::size_t size_ = 0;
  uint64_t chunk_start_ = 0;     // chunk's file offset (cold locator)
  ByteOffset within_chunk_ = 0;  // within-chunk record offset (cold key)
  ChannelId channel_id_ = 0;
  Timestamp log_time_ = 0;
  std::shared_ptr<detail::ColdChunkStore> cold_;  // shared; outlives this store
};

// Tuning for MessageByteStore. Namespace-scope (not nested) so it can be a
// defaulted argument of MessageByteStore::init without tripping the "default
// member initializer required before the end of its enclosing class" rule.
struct MessageByteStoreOptions {
  std::size_t cacheCapacityBytes = 128ULL * 1024 * 1024;
};

// Produces MessageByteFetchers for a ParallelMessageView iteration and owns the
// cold-read machinery. Construct from the parallel reader's already-parsed chunk
// index (no second summary parse). The cold FileReader is opened lazily on the
// first cold miss, so fully-eager imports never open the file a second time.
class MessageByteStore {
public:
  using Options = MessageByteStoreOptions;

  MessageByteStore() = default;
  MessageByteStore(const MessageByteStore&) = delete;
  MessageByteStore& operator=(const MessageByteStore&) = delete;
  MessageByteStore(MessageByteStore&&) = default;
  MessageByteStore& operator=(MessageByteStore&&) = default;
  ~MessageByteStore() = default;

  // I/O-free: only consumes the passed-in chunk index. `onProblem`, if given, is
  // used ONLY here as a cheap early-warning (a non-readable file is reported now
  // rather than silently at the first lazy pull). It is NOT retained — cold-path
  // problems go to stderr because they may fire after the caller is gone.
  void init(std::string filepath, const std::vector<ChunkIndex>& chunkIndexes,
            MessageByteStoreOptions options = {}, const ProblemCallback& onProblem = {}) {
    cold_ = std::make_shared<detail::ColdChunkStore>();
    cold_->filepath_ = std::move(filepath);
    cold_->capacity_bytes_ = options.cacheCapacityBytes;
    for (const auto& ci : chunkIndexes) {
      cold_->meta_.emplace(
        ci.chunkStartOffset,
        detail::ColdChunkStore::ChunkMeta{ci.chunkLength, ci.messageStartTime, ci.messageEndTime});
    }
    if (onProblem) {
      std::ifstream probe(cold_->filepath_, std::ios::binary);
      if (!probe.is_open()) {
        onProblem(Status{StatusCode::OpenFailed,
                         "MCAP file not readable for lazy reads: " + cold_->filepath_});
      }
    }
  }

  // Bind a fetcher to the message the iterator is currently positioned on.
  // Captures the opaque buffer handle + message.data NOW and retains no
  // reference to the iterator, so the fetcher safely outlives both the iterator
  // and the ParallelReader.
  MessageByteFetcher makeFetcher(const ParallelMessageView::Iterator& it,
                                 const MessageView& view) const {
    MessageByteFetcher f;
    f.cold_ = cold_;
    f.size_ = view.message.dataSize;
    f.hot_buffer_ = it.currentBuffer();
    f.hot_data_ = view.message.data;
    // Cold locator. RecordOffset in the parallel reader: .chunkOffset = chunk's
    // FILE offset (== ChunkIndex::chunkStartOffset), .offset = within-chunk
    // record offset (the value the cold index is keyed by).
    f.chunk_start_ = view.messageOffset.chunkOffset.value_or(view.messageOffset.offset);
    f.within_chunk_ = view.messageOffset.offset;
    f.channel_id_ = (view.channel != nullptr) ? view.channel->id : 0;
    f.log_time_ = view.message.logTime;
    return f;
  }

private:
  std::shared_ptr<detail::ColdChunkStore> cold_;
};

}  // namespace mcap
