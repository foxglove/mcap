#pragma once

#include "types.hpp"
#include <algorithm>
#include <variant>

namespace mcap {

struct ReadMessageJob {
  Timestamp timestamp;
  ByteOffset offset;
  size_t chunkReaderIndex;
};

struct DecompressChunkJob {
  Timestamp messageStartTime;
  Timestamp messageEndTime;
  ByteOffset chunkStartOffset;
  ByteOffset messageIndexEndOffset;
};

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
};

struct ReadJobQueue {
private:
  bool reverse_ = false;
  std::vector<ReadJob> heap_;

  static Timestamp forwardComparisonKey(const ReadJob& entry) {
    switch (entry.tag) {
      case ReadJob::Tag::ReadMessage:
        return entry.readMessage.timestamp;
      case ReadJob::Tag::DecompressChunk:
        return entry.decompressChunk.messageStartTime;
      default:
        assert(false && "unreachable");
    }
  }

  static Timestamp reverseComparisonKey(const ReadJob& entry) {
    switch (entry.tag) {
      case ReadJob::Tag::ReadMessage:
        return entry.readMessage.timestamp;
      case ReadJob::Tag::DecompressChunk:
        return entry.decompressChunk.messageEndTime;
      default:
        assert(false && "unreachable");
    }
  }

  static bool compareForward(const ReadJob& a, const ReadJob& b) {
    return forwardComparisonKey(a) > forwardComparisonKey(b);
  }

  static bool compareReverse(const ReadJob& a, const ReadJob& b) {
    return reverseComparisonKey(a) < reverseComparisonKey(b);
  }

public:
  explicit ReadJobQueue(bool reverse)
      : reverse_(reverse) {}
  void push(DecompressChunkJob&& decompressChunkJob) {
    ReadJob job;
    job.tag = ReadJob::Tag::DecompressChunk;
    job.decompressChunk = decompressChunkJob;
    heap_.emplace_back(std::move(job));
    if (!reverse_) {
      std::push_heap(heap_.begin(), heap_.end(), compareForward);
    } else {
      std::push_heap(heap_.begin(), heap_.end(), compareReverse);
    }
  }

  void push(ReadMessageJob&& readMessageJob) {
    ReadJob job;
    job.tag = ReadJob::Tag::ReadMessage;
    job.readMessage = readMessageJob;
    heap_.emplace_back(std::move(job));
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
