#include <mcap/mcap.hpp>

#include <nlohmann/json.hpp>

#include <iostream>

using json = nlohmann::ordered_json;

json ToJson(const std::byte* data, uint64_t size) {
  json output = json::array();
  for (uint64_t i = 0; i < size; ++i) {
    output.push_back(std::to_string(uint8_t(data[i])));
  }
  return output;
}

json ToJson(const std::unordered_map<uint16_t, uint64_t>& map) {
  json output = json::object();
  for (const auto& [key, value] : map) {
    output[std::to_string(key)] = std::to_string(value);
  }
  return output;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  json recordsJson = json::array();

  const std::string inputFile = argv[1];
  std::ifstream input(inputFile, std::ios::binary);
  mcap::FileStreamReader dataSource{input};
  mcap::TypedRecordReader reader{dataSource, 8};

  reader.onHeader = [&](const mcap::Header& header) {
    recordsJson.push_back(json::object({
      {"type", "Header"},
      {"fields", json::array({
                   json::array({"library", header.library}),
                   json::array({"profile", header.profile}),
                 })},
    }));
  };

  reader.onFooter = [&](const mcap::Footer& footer) {
    recordsJson.push_back(json::object({
      {"type", "Footer"},
      {"fields", json::array({
                   json::array({"summary_crc", std::to_string(footer.summaryCrc)}),
                   json::array({"summary_offset_start", std::to_string(footer.summaryOffsetStart)}),
                   json::array({"summary_start", std::to_string(footer.summaryStart)}),
                 })},
    }));
  };

  reader.onSchema = [&](const mcap::SchemaPtr schemaPtr) {
    const auto& schema = *schemaPtr;
    recordsJson.push_back(json::object({
      {"type", "Schema"},
      {"fields", json::array({
                   json::array({"data", ToJson(schema.data.data(), schema.data.size())}),
                   json::array({"encoding", schema.encoding}),
                   json::array({"id", std::to_string(schema.id)}),
                   json::array({"name", schema.name}),
                 })},
    }));
  };

  reader.onChannel = [&](const mcap::ChannelPtr channelPtr) {
    const auto& channel = *channelPtr;
    recordsJson.push_back(json::object({
      {"type", "Channel"},
      {"fields", json::array({
                   json::array({"id", std::to_string(channel.id)}),
                   json::array({"message_encoding", channel.messageEncoding}),
                   json::array({"metadata", channel.metadata}),
                   json::array({"schema_id", std::to_string(channel.schemaId)}),
                   json::array({"topic", channel.topic}),
                 })},
    }));
  };

  reader.onMessage = [&](const mcap::Message& message) {
    recordsJson.push_back(json::object({
      {"type", "Message"},
      {"fields", json::array({
                   json::array({"channel_id", std::to_string(message.channelId)}),
                   json::array({"data", ToJson(message.data, message.dataSize)}),
                   json::array({"log_time", std::to_string(message.logTime)}),
                   json::array({"publish_time", std::to_string(message.publishTime)}),
                   json::array({"sequence", std::to_string(message.sequence)}),
                 })},
    }));
  };

  // reader.onChunk = [&](const mcap::Chunk& chunk) {
  //   recordsJson.push_back(json::object({
  //     {"type", "Chunk"},
  //     {"fields", json::array({
  //                  json::array({"compressed_size", std::to_string(chunk.compressedSize)}),
  //                  json::array({"compression", chunk.compression}),
  //                  json::array({"end_time", std::to_string(chunk.endTime)}),
  //                  json::array({"start_time", std::to_string(chunk.startTime)}),
  //                  json::array({"uncompressed_crc", std::to_string(chunk.uncompressedCrc)}),
  //                  json::array({"uncompressed_size", std::to_string(chunk.uncompressedSize)}),
  //                })},
  //   }));
  // };

  // reader.onMessageIndex = [&](const mcap::MessageIndex& messageIndex) {
  //   recordsJson.push_back(json::object({
  //     {"type", "MessageIndex"},
  //     {"fields", json::array({
  //                  json::array({"channel_id", std::to_string(messageIndex.channelId)}),
  //                  json::array({"records", messageIndex.records}),
  //                })},
  //   }));
  // };

  reader.onChunkIndex = [&](const mcap::ChunkIndex& chunkIndex) {
    recordsJson.push_back(json::object({
      {"type", "ChunkIndex"},
      {"fields",
       json::array({
         json::array({"chunk_length", std::to_string(chunkIndex.chunkLength)}),
         json::array({"chunk_start_offset", std::to_string(chunkIndex.chunkStartOffset)}),
         json::array({"compressed_size", std::to_string(chunkIndex.compressedSize)}),
         json::array({"compression", chunkIndex.compression}),
         json::array({"end_time", std::to_string(chunkIndex.endTime)}),
         json::array({"message_index_length", std::to_string(chunkIndex.messageIndexLength)}),
         json::array({"message_index_offsets", ToJson(chunkIndex.messageIndexOffsets)}),
         json::array({"start_time", std::to_string(chunkIndex.startTime)}),
         json::array({"uncompressed_size", std::to_string(chunkIndex.uncompressedSize)}),
       })},
    }));
  };

  reader.onAttachment = [&](const mcap::Attachment& attachment) {
    recordsJson.push_back(json::object({
      {"type", "Attachment"},
      {"fields", json::array({
                   json::array({"content_type", attachment.contentType}),
                   json::array({"created_at", std::to_string(attachment.createdAt)}),
                   json::array({"data", ToJson(attachment.data, attachment.dataSize)}),
                   json::array({"log_time", std::to_string(attachment.logTime)}),
                   json::array({"name", attachment.name}),
                 })},
    }));
  };

  reader.onAttachmentIndex = [&](const mcap::AttachmentIndex& attachmentIndex) {
    recordsJson.push_back(json::object({
      {"type", "AttachmentIndex"},
      {"fields", json::array({
                   json::array({"content_type", attachmentIndex.contentType}),
                   json::array({"data_size", std::to_string(attachmentIndex.dataSize)}),
                   json::array({"length", std::to_string(attachmentIndex.length)}),
                   json::array({"log_time", std::to_string(attachmentIndex.logTime)}),
                   json::array({"name", attachmentIndex.name}),
                   json::array({"offset", std::to_string(attachmentIndex.offset)}),
                 })},
    }));
  };

  reader.onStatistics = [&](const mcap::Statistics& statistics) {
    recordsJson.push_back(json::object({
      {"type", "Statistics"},
      {"fields", json::array({
                   json::array({"attachment_count", std::to_string(statistics.attachmentCount)}),
                   json::array({"channel_count", std::to_string(statistics.channelCount)}),
                   json::array({"channel_message_counts", ToJson(statistics.channelMessageCounts)}),
                   json::array({"chunk_count", std::to_string(statistics.chunkCount)}),
                   json::array({"message_count", std::to_string(statistics.messageCount)}),
                   json::array({"metadata_count", std::to_string(statistics.metadataCount)}),
                 })},
    }));
  };

  reader.onMetadata = [&](const mcap::Metadata& metadata) {
    recordsJson.push_back(json::object({
      {"type", "Metadata"},
      {"fields", json::array({
                   json::array({"metadata", metadata.metadata}),
                   json::array({"name", metadata.name}),
                 })},
    }));
  };

  reader.onMetadataIndex = [&](const mcap::MetadataIndex& metadataIndex) {
    recordsJson.push_back(json::object({
      {"type", "MetadataIndex"},
      {"fields", json::array({
                   json::array({"length", std::to_string(metadataIndex.length)}),
                   json::array({"name", metadataIndex.name}),
                   json::array({"offset", std::to_string(metadataIndex.offset)}),
                 })},
    }));
  };

  reader.onSummaryOffset = [&](const mcap::SummaryOffset& summaryOffset) {
    recordsJson.push_back(json::object({
      {"type", "SummaryOffset"},
      {"fields",
       json::array({
         json::array({"group_length", std::to_string(summaryOffset.groupLength)}),
         json::array({"group_opcode", std::to_string(uint8_t(summaryOffset.groupOpCode))}),
         json::array({"group_start", std::to_string(summaryOffset.groupStart)}),
       })},
    }));
  };

  // reader.onDataEnd = [&](const mcap::DataEnd& dataEnd) {
  //   recordsJson.push_back(json::object({
  //     {"type", "DataEnd"},
  //     {"fields", json::array({
  //                  json::array({"data_section_crc", std::to_string(dataEnd.dataSectionCrc)}),
  //                })},
  //   }));
  // };

  while (reader.next()) {
    if (!reader.status().ok()) {
      json output = {{"error", reader.status().message}};
      std::cout << output.dump() << "\n";
      return 1;
    }
  }

  json output = {{"records", recordsJson}};
  std::cout << output.dump() << "\n";
  return 0;
}
