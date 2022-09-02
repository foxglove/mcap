#pragma once

#include "types.hpp"
#include <algorithm>

namespace mcap {

/**
 * @brief A job to read a specific message at offset `offset` from the decompressed chunk
 * stored in `chunkReaderIndex`. A timestamp is provided to order this job relative to other jobs.
 */
struct ReadMessageJob {
  Timestamp timestamp;
  ByteOffset offset;
  size_t chunkReaderIndex;
};

/**
 * @brief A job to decompress the chunk starting at `chunkStartOffset`. The message indices
 * starting directly after the chunk record and ending at `messageIndexEndOffset` will be used to
 * find specific messages within the chunk.
 */
struct DecompressChunkJob {
  Timestamp messageStartTime;
  Timestamp messageEndTime;
  ByteOffset chunkStartOffset;
  ByteOffset messageIndexEndOffset;
};

/**
 * @brief A tagged union of jobs that an indexed MCAP reader executes.
 */
struct ReadJob {
  enum struct Tag {
    ReadMessage,
    DecompressChunk,
  };
  Tag tag;
  union {
    ReadMessageJob readMessage;
    DecompressChunkJob decompressChunk;
  };

  explicit ReadJob(ReadMessageJob&& readMessageJob) {
    tag = Tag::ReadMessage;
    readMessage = readMessageJob;
  }

  explicit ReadJob(DecompressChunkJob&& decompressChunkJob) {
    tag = Tag::DecompressChunk;
    decompressChunk = decompressChunkJob;
  }

  /**
   * @brief returns the comparison key that should be used when ordering these jobs forward in time.
   */
  Timestamp forwardComparisonKey() const {
    switch (tag) {
      case ReadJob::Tag::ReadMessage:
        return readMessage.timestamp;
      case ReadJob::Tag::DecompressChunk:
        return decompressChunk.messageStartTime;
      default:
        assert(false && "unreachable");
    }
  }
  /**
   * @brief returns the comparison key that should be used when ordering these jobs in reverse
   * time order.
   */
  Timestamp reverseComparisonKey() const {
    switch (tag) {
      case ReadJob::Tag::ReadMessage:
        return readMessage.timestamp;
      case ReadJob::Tag::DecompressChunk:
        return decompressChunk.messageEndTime;
      default:
        assert(false && "unreachable");
    }
  }
};

/**
 * @brief A priority queue of jobs for an indexed MCAP reader to execute.
 */
struct ReadJobQueue {
private:
  bool reverse_ = false;
  std::vector<ReadJob> heap_;

  static bool compareForward(const ReadJob& a, const ReadJob& b) {
    return a.forwardComparisonKey() > b.forwardComparisonKey();
  }

  static bool compareReverse(const ReadJob& a, const ReadJob& b) {
    return a.reverseComparisonKey() < b.reverseComparisonKey();
  }

public:
  explicit ReadJobQueue(bool reverse)
      : reverse_(reverse) {}
  void push(DecompressChunkJob&& decompressChunkJob) {
    heap_.emplace_back(std::move(decompressChunkJob));
    if (!reverse_) {
      std::push_heap(heap_.begin(), heap_.end(), compareForward);
    } else {
      std::push_heap(heap_.begin(), heap_.end(), compareReverse);
    }
  }

  void push(ReadMessageJob&& readMessageJob) {
    heap_.emplace_back(std::move(readMessageJob));
    if (!reverse_) {
      std::push_heap(heap_.begin(), heap_.end(), compareForward);
    } else {
      std::push_heap(heap_.begin(), heap_.end(), compareReverse);
    }
  }

  ReadJob pop() {
    if (!reverse_) {
      std::pop_heap(heap_.begin(), heap_.end(), compareForward);
    } else {
      std::pop_heap(heap_.begin(), heap_.end(), compareReverse);
    }
    auto popped = heap_.back();
    heap_.pop_back();
    return popped;
  }

  size_t len() {
    return heap_.size();
  };
};

}  // namespace mcap
