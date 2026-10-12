#include "internal.hpp"

namespace mcap {

constexpr std::string_view OpCodeString(OpCode opcode) {
  switch (opcode) {
    case OpCode::Header:
      return "Header";
    case OpCode::Footer:
      return "Footer";
    case OpCode::Schema:
      return "Schema";
    case OpCode::Channel:
      return "Channel";
    case OpCode::Message:
      return "Message";
    case OpCode::Chunk:
      return "Chunk";
    case OpCode::MessageIndex:
      return "MessageIndex";
    case OpCode::ChunkIndex:
      return "ChunkIndex";
    case OpCode::Attachment:
      return "Attachment";
    case OpCode::AttachmentIndex:
      return "AttachmentIndex";
    case OpCode::Statistics:
      return "Statistics";
    case OpCode::Metadata:
      return "Metadata";
    case OpCode::MetadataIndex:
      return "MetadataIndex";
    case OpCode::SummaryOffset:
      return "SummaryOffset";
    case OpCode::DataEnd:
      return "DataEnd";
    default:
      return "Unknown";
  }
}

MetadataIndex::MetadataIndex(const Metadata& metadata, ByteOffset fileOffset)
    : offset(fileOffset)
    , length(9 + 4 + metadata.name.size() + 4 + internal::KeyValueMapSize(metadata.metadata))
    , name(metadata.name) {}

int RecordOffset::compare(const RecordOffset& other) const {
  // Order first by position in the file: the chunk record's offset for a chunked record, or the
  // record's own offset otherwise.
  const ByteOffset filePosition = chunkOffset.has_value() ? *chunkOffset : offset;
  const ByteOffset otherFilePosition =
    other.chunkOffset.has_value() ? *other.chunkOffset : other.offset;
  if (filePosition != otherFilePosition) {
    return filePosition < otherFilePosition ? -1 : 1;
  }
  // Same file position. A plain file offset naming the start of a chunk precedes every record
  // inside that chunk, since the chunk record's header comes before its contents.
  if (chunkOffset.has_value() != other.chunkOffset.has_value()) {
    return chunkOffset.has_value() ? 1 : -1;
  }
  // Both plain (and therefore equal), or both in the same chunk: order by offset within it.
  if (offset != other.offset) {
    return offset < other.offset ? -1 : 1;
  }
  return 0;
}

}  // namespace mcap
