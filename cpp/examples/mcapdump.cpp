#include <mcap/mcap.hpp>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

std::string ToString(const mcap::KeyValueMap& map) {
  std::stringstream ss;
  ss << "{";
  for (const auto& [key, value] : map) {
    if (ss.tellg() > 1) {
      ss << ", ";
    }
    ss << "\"" << key << "\": \"" << value << "\"";
  }
  ss << "}";
  return ss.str();
}

std::string ToString(const std::unordered_map<uint16_t, uint64_t>& map) {
  if (map.size() > 8) {
    return mcap::internal::StrFormat("<{} entries>", map.size());
  }

  std::stringstream ss;
  ss << "{";
  for (const auto& [key, value] : map) {
    if (ss.tellg() > 1) {
      ss << ", ";
    }
    ss << key << ": " << value;
  }
  ss << "}";
  return ss.str();
}

std::string ToString(const std::vector<std::pair<mcap::Timestamp, mcap::ByteOffset>>& pairs) {
  if (pairs.size() > 8) {
    return mcap::internal::StrFormat("<{} entries>", pairs.size());
  }

  std::stringstream ss;
  ss << "[";
  for (const auto& [timestamp, offset] : pairs) {
    if (ss.tellg() > 1) {
      ss << ", ";
    }
    ss << "{" << timestamp << ", " << offset << "}";
  }
  ss << "]";
  return ss.str();
}

std::string ToString(const mcap::Header& header) {
  return mcap::internal::StrFormat("[Header] profile={}, library={}", header.profile,
                                   header.library);
}

std::string ToString(const mcap::Footer& footer) {
  return mcap::internal::StrFormat(
    "[Footer] summary_start={}, summary_offset_start={}, summary_crc={}", footer.summaryStart,
    footer.summaryOffsetStart, footer.summaryCrc);
}

std::string ToString(const mcap::Schema& schema) {
  return mcap::internal::StrFormat("[Schema] id={}, name={}, encoding={}, data=<{} bytes>",
                                   schema.id, schema.name, schema.encoding, schema.data.size());
}

std::string ToString(const mcap::Channel& channel) {
  return mcap::internal::StrFormat(
    "[Channel] id={}, topic={}, message_encoding={}, schema_id={}, metadata={}", channel.id,
    channel.topic, channel.messageEncoding, channel.schemaId, ToString(channel.metadata));
}

std::string ToString(const mcap::Message& message) {
  return mcap::internal::StrFormat(
    "[Message] channel_id={}, sequence={}, publish_time={}, record_time={}, data=<{} bytes>",
    message.channelId, message.sequence, message.publishTime, message.recordTime, message.dataSize);
}

std::string ToString(const mcap::Chunk& chunk) {
  return mcap::internal::StrFormat(
    "[Chunk] uncompressed_size={}, uncompressed_crc={}, compression={}, data=<{} bytes>",
    chunk.uncompressedSize, chunk.uncompressedCrc, chunk.compression, chunk.compressedSize);
}

std::string ToString(const mcap::MessageIndex& messageIndex) {
  return mcap::internal::StrFormat("[MessageIndex] channel_id={}, records={}",
                                   messageIndex.channelId, ToString(messageIndex.records));
}

std::string ToString(const mcap::ChunkIndex& chunkIndex) {
  return mcap::internal::StrFormat(
    "[ChunkIndex] start_time={}, end_time={}, chunk_start_offset={}, chunk_length={}, "
    "message_index_offsets={}, message_index_length={}, compression={}, "
    "compressed_size={}, uncompressed_size={}",
    chunkIndex.startTime, chunkIndex.endTime, chunkIndex.chunkStartOffset, chunkIndex.chunkLength,
    ToString(chunkIndex.messageIndexOffsets), chunkIndex.messageIndexLength, chunkIndex.compression,
    chunkIndex.compressedSize, chunkIndex.uncompressedSize);
}

std::string ToString(const mcap::Attachment& attachment) {
  return mcap::internal::StrFormat(
    "[Attachment] name={}, created_at={}, log_time={}, content_type={}, data=<{} bytes>, crc={}",
    attachment.name, attachment.createdAt, attachment.logTime, attachment.contentType,
    attachment.dataSize, attachment.crc);
}

std::string ToString(const mcap::AttachmentIndex& attachmentIndex) {
  return mcap::internal::StrFormat(
    "[Attachment] offset={}, length={}, log_time={}, data_size={}, name={}, content_type={}",
    attachmentIndex.offset, attachmentIndex.length, attachmentIndex.logTime,
    attachmentIndex.dataSize, attachmentIndex.name, attachmentIndex.contentType);
}

std::string ToString(const mcap::Statistics& statistics) {
  return mcap::internal::StrFormat(
    "[Statistics] message_count={}, channel_count={}, attachment_count={}, metadata_count={}, "
    "chunk_count={}, channel_message_counts={}",
    statistics.messageCount, statistics.channelCount, statistics.attachmentCount,
    statistics.metadataCount, statistics.chunkCount, ToString(statistics.channelMessageCounts));
}

std::string ToString(const mcap::Metadata& metadata) {
  return mcap::internal::StrFormat("[Metadata] name={}, metadata={}", metadata.name,
                                   ToString(metadata.metadata));
}

std::string ToString(const mcap::MetadataIndex& metadataIndex) {
  return mcap::internal::StrFormat("[MetadataIndex] offset={}, length={}, name={}",
                                   metadataIndex.offset, metadataIndex.length, metadataIndex.name);
}

std::string ToString(const mcap::SummaryOffset& summaryOffset) {
  return mcap::internal::StrFormat(
    "[SummaryOffset] group_opcode={} (0x{:02x}), group_start={}, group_length={}",
    mcap::OpCodeString(summaryOffset.groupOpCode), uint8_t(summaryOffset.groupOpCode),
    summaryOffset.groupStart, summaryOffset.groupLength);
}

std::string ToString(const mcap::DataEnd& dataEnd) {
  return mcap::internal::StrFormat("[DataEnd] data_section_crc={}", dataEnd.dataSectionCrc);
}

std::string ToString(const mcap::Record& record) {
  return mcap::internal::StrFormat("[Unknown] opcode=0x{:02x}, data=<{} bytes>",
                                   uint8_t(record.opcode), record.dataSize);
}

std::string ToStringRaw(const mcap::Record& record) {
  return mcap::internal::StrFormat("[{}] opcode=0x{:02x}, data=<{} bytes>",
                                   mcap::OpCodeString(record.opcode), uint8_t(record.opcode),
                                   record.dataSize);
}

void DumpRaw(mcap::IReadable& dataSource) {
  // Iterate all of the raw records in the data source, ignoring the magic bytes
  // at the beginning and end of the file. This will not print the contents of
  // chunk records since chunks are not parsed or decompressed
  mcap::RecordReader reader{dataSource, 8, dataSource.size() - 8};

  bool running = true;
  while (running) {
    const auto record = reader.next();
    if (record.has_value()) {
      std::cout << ToStringRaw(record.value()) << "\n";
    } else {
      running = false;
    }

    if (!reader.status().ok()) {
      std::cout << "! " << reader.status().message << "\n";
    }
  }
}

void Dump(mcap::IReadable& dataSource) {
  // Iterate and parse all of the records in the data source, ignoring the magic
  // bytes in the header. This will print the contents of chunk records as well
  mcap::TypedRecordReader reader{dataSource, 8};
  bool inChunk = false;

  reader.onHeader = [](const mcap::Header& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onFooter = [](const mcap::Footer& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onSchema = [&](const mcap::Schema& record) {
    std::cout << (inChunk ? "  " : "") << ToString(record) << "\n";
  };
  reader.onChannel = [&](const mcap::Channel& record) {
    std::cout << (inChunk ? "  " : "") << ToString(record) << "\n";
  };
  reader.onMessage = [&](const mcap::Message& record) {
    std::cout << (inChunk ? "  " : "") << ToString(record) << "\n";
  };
  reader.onChunk = [&](const mcap::Chunk& record) {
    std::cout << ToString(record) << "\n";
    inChunk = true;
  };
  reader.onMessageIndex = [](const mcap::MessageIndex& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onChunkIndex = [](const mcap::ChunkIndex& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onAttachment = [](const mcap::Attachment& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onAttachmentIndex = [](const mcap::AttachmentIndex& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onStatistics = [](const mcap::Statistics& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onMetadata = [](const mcap::Metadata& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onMetadataIndex = [](const mcap::MetadataIndex& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onSummaryOffset = [](const mcap::SummaryOffset& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onDataEnd = [](const mcap::DataEnd& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onUnknownRecord = [](const mcap::Record& record) {
    std::cout << ToString(record) << "\n";
  };
  reader.onChunkEnd = [&]() {
    inChunk = false;
  };

  bool running = true;
  while (running) {
    running = reader.next();
    if (!reader.status().ok()) {
      std::cerr << "! " << reader.status().message << "\n";
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  const std::string inputFile = argv[1];
  std::ifstream input(inputFile, std::ios::binary);
  mcap::FileStreamReader dataSource{input};

  Dump(dataSource);

  return 0;
}
