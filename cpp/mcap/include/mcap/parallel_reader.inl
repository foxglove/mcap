// parallel_reader.inl — included only when MCAP_IMPLEMENTATION is defined.
// Contains implementations for classes declared in parallel_reader.hpp.

namespace mcap {

// internal::ReadyChunk ////////////////////////////////////////////////////////

internal::ReadyChunk::~ReadyChunk() {
  if (stats && liveBytesAccounted != 0) {
    stats->subLive(liveBytesAccounted);
  }
  if (sem && budgetHeld != 0) {
    sem->release(budgetHeld);
  }
}

// ParallelMessageView /////////////////////////////////////////////////////////

ParallelMessageView::ParallelMessageView(McapReader& reader, IReadable* source,
                                         const ParallelReadOptions& opts,
                                         const ProblemCallback& onProblem)
    : reader_(reader)
    , source_(source)
    , opts_(opts)
    , onProblem_(onProblem)
    , reverse_(opts.read.readOrder == ReadMessageOptions::ReadOrder::ReverseLogTimeOrder)
    , heap_(makeHeapComparator()) {
  init();
}

ParallelMessageView::~ParallelMessageView() {
  cancelled_.store(true);  // make queued workers bail instead of decompressing
  if (pool_) pool_->shutdown();
}

Status ParallelMessageView::status() const {
  return status_;
}

const internal::ParallelStats& ParallelMessageView::stats() const {
  return *stats_;
}

ParallelMessageView::Iterator ParallelMessageView::begin() {
  if (!status_.ok()) return end();
  return Iterator{*this};
}

ParallelMessageView::Iterator ParallelMessageView::end() {
  return Iterator{};
}

// ParallelMessageView::Iterator ///////////////////////////////////////////////

ParallelMessageView::Iterator::Iterator(ParallelMessageView& v)
    : view_(&v) {
  ++(*this);  // prime first message
}

ParallelMessageView::Iterator::reference ParallelMessageView::Iterator::operator*() const {
  return *view_->curView_;
}

ParallelMessageView::Iterator::pointer ParallelMessageView::Iterator::operator->() const {
  return &*view_->curView_;
}

std::weak_ptr<const void> ParallelMessageView::Iterator::currentBuffer() const {
  if (!view_) {
    return {};
  }
  return view_->pinned_;
}

ParallelMessageView::Iterator& ParallelMessageView::Iterator::operator++() {
  if (view_ && !view_->produceNext()) {
    view_ = nullptr;  // exhausted -> equals end()
  }
  return *this;
}

void ParallelMessageView::Iterator::operator++(int) {
  ++(*this);
}

bool operator==(const ParallelMessageView::Iterator& a, const ParallelMessageView::Iterator& b) {
  return a.view_ == b.view_;
}

bool operator!=(const ParallelMessageView::Iterator& a, const ParallelMessageView::Iterator& b) {
  return !(a == b);
}

// ParallelMessageView private methods /////////////////////////////////////////

std::function<bool(const ParallelMessageView::HeapItem&, const ParallelMessageView::HeapItem&)>
ParallelMessageView::makeHeapComparator() {
  const bool reverse = reverse_;
  // priority_queue is a max-heap: "true means a is LOWER priority than b", i.e.
  // a emits AFTER b. Reuse the one ordering definition.
  return [reverse](const HeapItem& a, const HeapItem& b) {
    return internal::messageOrderLess(b.timestamp, b.ro, a.timestamp, a.ro, reverse);
  };
}

void ParallelMessageView::init() {
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
  chanById_ = reader_.channels();
  schemaById_ = reader_.schemas();

  // Precise byte budget: profile worst-case residency and resolve an effective
  // byte cap (floor + lookahead).
  const auto profile = computeResidencyProfile(chunkIndexes, selectedChannels_);
  uint64_t lookahead = opts_.lookaheadBytes;
  if (lookahead == 0) {
    const unsigned t = opts_.threadCount ? opts_.threadCount : std::thread::hardware_concurrency();
    lookahead = uint64_t(std::max(1u, t)) * (profile.uMaxBytes ? profile.uMaxBytes : 1);
  }
  budget_ = resolveBudget(profile, opts_.read.readOrder, opts_.maxBytesInFlight, opts_.capPolicy,
                          lookahead);
  if (budget_.fallBackToSerial || !budget_.feasibleWithoutEviction) {
    status_ = Status{StatusCode::InvalidMessageReadOptions, budget_.note};
    onProblem_(status_);
    return;
  }
  sem_ =
    std::make_shared<internal::ByteSemaphore>(std::max<uint64_t>(budget_.effectiveBudgetBytes, 1));

  // Worker count: default 4 (most reads are consumer/merge-bound, where ~4
  // decompress workers keep the single consumer fed); HARD CAP 8 (decompression
  // saturates memory bandwidth around there, so >8 only adds contention);
  // never exceed the core count.
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

void ParallelMessageView::runChunkJob(size_t planIdx, uint64_t budgetHeld,
                                      std::shared_ptr<std::promise<internal::ReadyChunkPtr>> prom) {
  auto rc = std::make_shared<internal::ReadyChunk>();
  rc->sem = sem_;
  rc->stats = stats_;
  rc->budgetHeld = budgetHeld;

  // Fast-bail for queued jobs once the view is being torn down.
  if (cancelled_.load(std::memory_order_relaxed)) {
    rc->status = Status{StatusCode::ReadFailed, "cancelled"};
    prom->set_value(std::move(rc));
    return;
  }

  // Any throw in decompression/parsing/allocation is converted to a Status;
  // the promise is ALWAYS fulfilled so the consumer can never hang on get().
  try {
    const auto& plan = plans_[planIdx];
    RecordReader rr(*source_, plan.chunkStartOffset, plan.messageIndexEndOffset);
    bool gotChunk = false;
    // Each MessageIndex contributes one contiguous, per-channel run of entries.
    // We record the run boundaries so orderEntries() can k-way MERGE the
    // already-sorted runs (O(N log C)) instead of a full O(N log N) std::sort.
    std::vector<std::pair<size_t, size_t>> runs;
    for (auto rec = rr.next(); rec.has_value(); rec = rr.next()) {
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
    // The for-loop exits when rr.next() returns nullopt, which can mean either
    // a clean end-of-range OR a record-read failure (truncated/corrupt input).
    // Surface that failure here so a successfully-decompressed chunk does not
    // mask a subsequent read error.
    if (rc->status.ok() && !rr.status().ok()) {
      rc->status = rr.status();
    }
    if (rc->status.ok() && !gotChunk) {
      rc->status = Status{StatusCode::InvalidChunkOffset, "no chunk record at planned offset"};
    }
    if (rc->status.ok()) {
      orderEntries(rc->entries, runs);
      rc->liveBytesAccounted = rc->bytes.size();
      stats_->addLive(rc->liveBytesAccounted);
      stats_->chunksDecompressed.fetch_add(1, std::memory_order_relaxed);
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

void ParallelMessageView::orderEntries(std::vector<internal::PMsgEntry>& entries,
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
  // All runs sorted -> k-way merge them (O(N log C)).
  std::vector<internal::PMsgEntry> merged;
  merged.reserve(entries.size());
  struct Node {
    size_t idx;
    size_t end;
  };
  auto worse = [&](const Node& a, const Node& b) {
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

Status ParallelMessageView::decompressInto(const Chunk& chunk, internal::RawByteArray& out) {
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
    // Reuse a per-thread DCtx and decompress into an uninitialized buffer.
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

bool ParallelMessageView::scheduleChunk(size_t planIdx, bool force) {
  if (scheduled_[planIdx]) return true;
  const uint64_t need = plans_[planIdx].uncompressedSize;
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

void ParallelMessageView::prefetch() {
  // Resumes from a monotonic cursor so it never re-walks the already-scheduled
  // prefix: avoids O(window) re-scan per message (was the dominant consumer cost).
  while (nextPlanToPrefetch_ < plans_.size()) {
    if (scheduled_[nextPlanToPrefetch_]) {
      ++nextPlanToPrefetch_;
      continue;
    }
    if (!scheduleChunk(nextPlanToPrefetch_, /*force=*/false)) {
      break;  // budget full -> retry this same index next time
    }
    ++nextPlanToPrefetch_;
  }
}

void ParallelMessageView::addCursor(size_t planIdx) {
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

bool ParallelMessageView::produceNext() {
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
      pinned_.reset();
      return false;
    }

    const HeapItem top = heap_.top();
    heap_.pop();
    Cursor& cur = cursors_[top.cursorId];
    const internal::PMsgEntry entry = cur.head();

    // Pin the backing chunk for the message we're about to emit so its bytes
    // stay valid until we advance to a DIFFERENT chunk. Re-pinning only on a
    // chunk change avoids an atomic shared_ptr inc/dec for every message within
    // a chunk (profiling showed per-message refcount churn was a leading cost).
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
    // the current message, so dropping cursors_'s reference here makes the
    // ReadyChunk destruct once pinned_ rotates on the next ++ -- which returns
    // its bytes to the ByteSemaphore so prefetch can keep filling the window.
    cur.idx++;
    if (cur.live()) {
      const auto& e = cur.head();
      heap_.push(HeapItem{e.timestamp, RecordOffset{e.offset, e.chunkOffset}, top.cursorId});
    } else {
      cur.chunk.reset();
    }

    // Resolve channel/schema from one-time snapshots by const-ref, so emitting
    // a message does NOT copy a shared_ptr (profiling showed per-message atomic
    // refcount churn was a leading consumer cost).
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
    curView_.emplace(curMessage_, channel, *schema, RecordOffset{entry.offset, entry.chunkOffset});
    return true;
  }
}

// ParallelReader //////////////////////////////////////////////////////////////

Status ParallelReader::open(std::string_view path) {
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

Status ParallelReader::open(IReadable& concurrentSource) {
  close();
  const Status status = reader_.open(concurrentSource);
  if (!status.ok()) {
    return status;
  }
  source_ = &concurrentSource;
  return reader_.readSummary(ReadSummaryMethod::AllowFallbackScan);
}

void ParallelReader::close() {
  reader_.close();
  ownedSource_.reset();
  source_ = nullptr;
}

ParallelMessageView ParallelReader::readMessages(const ProblemCallback& onProblem,
                                                 const ParallelReadOptions& options) {
  return ParallelMessageView(reader_, source_, options, onProblem);
}

McapReader& ParallelReader::reader() {
  return reader_;
}

const std::optional<Statistics>& ParallelReader::statistics() const {
  return reader_.statistics();
}

std::unordered_map<ChannelId, ChannelPtr> ParallelReader::channels() const {
  return reader_.channels();
}

std::unordered_map<SchemaId, SchemaPtr> ParallelReader::schemas() const {
  return reader_.schemas();
}

const std::vector<ChunkIndex>& ParallelReader::chunkIndexes() const {
  return reader_.chunkIndexes();
}

}  // namespace mcap
