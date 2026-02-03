#include "internal.hpp"

namespace mcap {

MCAP_PUBLIC_INLINE std::string_view OpCodeString(OpCode opcode) {
  using namespace std::literals;
  switch (opcode) {
    case OpCode::Header:
      return "Header"sv;
    case OpCode::Footer:
      return "Footer"sv;
    case OpCode::Schema:
      return "Schema"sv;
    case OpCode::Channel:
      return "Channel"sv;
    case OpCode::Message:
      return "Message"sv;
    case OpCode::Chunk:
      return "Chunk"sv;
    case OpCode::MessageIndex:
      return "MessageIndex"sv;
    case OpCode::ChunkIndex:
      return "ChunkIndex"sv;
    case OpCode::Attachment:
      return "Attachment"sv;
    case OpCode::AttachmentIndex:
      return "AttachmentIndex"sv;
    case OpCode::Statistics:
      return "Statistics"sv;
    case OpCode::Metadata:
      return "Metadata"sv;
    case OpCode::MetadataIndex:
      return "MetadataIndex"sv;
    case OpCode::SummaryOffset:
      return "SummaryOffset"sv;
    case OpCode::DataEnd:
      return "DataEnd"sv;
    default:
      return "Unknown"sv;
  }
}

MCAP_PUBLIC_INLINE MetadataIndex::MetadataIndex(const Metadata& metadata, ByteOffset fileOffset)
    : offset(fileOffset)
    , length(9 + 4 + metadata.name.size() + 4 + internal::KeyValueMapSize(metadata.metadata))
    , name(metadata.name) {}

MCAP_PUBLIC_INLINE bool RecordOffset::operator==(const RecordOffset& other) const {
  if (chunkOffset != std::nullopt && other.chunkOffset != std::nullopt) {
    if (*chunkOffset != *other.chunkOffset) {
      // messages are in separate chunks, cannot be equal.
      return false;
    }
    // messages are in the same chunk, compare chunk-level offsets.
    return (offset == other.offset);
  }
  if (chunkOffset != std::nullopt || other.chunkOffset != std::nullopt) {
    // one message is in a chunk and one is not, cannot be equal.
    return false;
  }
  // neither message is in a chunk, compare file-level offsets.
  return (offset == other.offset);
}

MCAP_PUBLIC_INLINE bool RecordOffset::operator>(const RecordOffset& other) const {
  if (chunkOffset != std::nullopt) {
    if (other.chunkOffset != std::nullopt) {
      if (*chunkOffset == *other.chunkOffset) {
        // messages are in the same chunk, compare chunk-level offsets.
        return (offset > other.offset);
      }
      // messages are in separate chunks, compare file-level offsets
      return (*chunkOffset > *other.chunkOffset);
    } else {
      // this message is in a chunk, other is not, compare file-level offsets.
      return (*chunkOffset > other.offset);
    }
  }
  if (other.chunkOffset != std::nullopt) {
    // other message is in a chunk, this is not, compare file-level offsets.
    return (offset > *other.chunkOffset);
  }
  // neither message is in a chunk, compare file-level offsets.
  return (offset > other.offset);
}

}  // namespace mcap
