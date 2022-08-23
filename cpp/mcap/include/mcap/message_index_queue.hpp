#pragma once

#include "types.hpp"
#include <algorithm>
#include <variant>

namespace mcap {

struct MessageIndexEntry {
  Timestamp timestamp;
  ByteOffset offset;
  size_t chunkReaderIndex;
};

using MessageIndexQueueItem = std::variant<ChunkIndex, MessageIndexEntry>;

struct MessageIndexQueue {
private:
  bool reverse_ = false;
  std::vector<MessageIndexQueueItem> heap_;

  static Timestamp forwardComparisonKey(const MessageIndexQueueItem& entry) {
    if (std::holds_alternative<MessageIndexEntry>(entry)) {
      return std::get<MessageIndexEntry>(entry).timestamp;
    }
    return std::get<ChunkIndex>(entry).messageStartTime;
  }

  static Timestamp reverseComparisonKey(const MessageIndexQueueItem& entry) {
    if (std::holds_alternative<MessageIndexEntry>(entry)) {
      return std::get<MessageIndexEntry>(entry).timestamp;
    }
    return std::get<ChunkIndex>(entry).messageEndTime;
  }

  static bool compareForward(const MessageIndexQueueItem& a, const MessageIndexQueueItem& b) {
    return forwardComparisonKey(a) > forwardComparisonKey(b);
  }

  static bool compareReverse(const MessageIndexQueueItem& a, const MessageIndexQueueItem& b) {
    return reverseComparisonKey(a) < reverseComparisonKey(b);
  }

public:
  explicit MessageIndexQueue(bool reverse)
      : reverse_(reverse) {}
  void push(const ChunkIndex& chunkIndex) {
    heap_.push_back(chunkIndex);
    if (!reverse_) {
      std::push_heap(heap_.begin(), heap_.end(), compareForward);
    } else {
      std::push_heap(heap_.begin(), heap_.end(), compareReverse);
    }
  }

  void push(Timestamp messageTimestamp, ByteOffset messageByteOffset, size_t chunkReaderIndex) {
    heap_.push_back(MessageIndexEntry{
      chunkReaderIndex,
      messageByteOffset,
      messageTimestamp,
    });
    if (!reverse_) {
      std::push_heap(heap_.begin(), heap_.end(), compareForward);
    } else {
      std::push_heap(heap_.begin(), heap_.end(), compareReverse);
    }
  }

  std::variant<ChunkIndex, MessageIndexEntry> pop() {
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
