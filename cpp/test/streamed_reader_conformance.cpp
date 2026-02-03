#include <mcap/reader.hpp>

#include <nlohmann/json.hpp>

#include <iostream>

using json = nlohmann::ordered_json;
using mcap::ByteOffset;

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

  reader.onHeader = [&](const mcap::Header& header, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "Header"},
      {"fields", json::array({
                   {"library", header.library},
                   {"profile", header.profile},
                 })},
    }));
  };

  reader.onFooter = [&](const mcap::Footer& footer, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "Footer"},
      {"fields", json::array({
                   {"summary_crc", std::to_string(footer.summaryCrc)},
                   {"summary_offset_start", std::to_string(footer.summaryOffsetStart)},
                   {"summary_start", std::to_string(footer.summaryStart)},
                 })},
    }));
  };

  reader.onSchema = [&](const mcap::SchemaPtr schemaPtr, ByteOffset, std::optional<ByteOffset>) {
    const auto& schema = *schemaPtr;
    recordsJson.push_back(json::object({
      {"type", "Schema"},
      {"fields", json::array({
                   {"data", ToJson(schema.data.data(), schema.data.size())},
                   {"encoding", schema.encoding},
                   {"id", std::to_string(schema.id)},
                   {"name", schema.name},
                 })},
    }));
  };

  reader.onChannel = [&](const mcap::ChannelPtr channelPtr, ByteOffset, std::optional<ByteOffset>) {
    const auto& channel = *channelPtr;
    recordsJson.push_back(json::object({
      {"type", "Channel"},
      {"fields", json::array({
                   {"id", std::to_string(channel.id)},
                   {"message_encoding", channel.messageEncoding},
                   {"metadata", channel.metadata},
                   {"schema_id", std::to_string(channel.schemaId)},
                   {"topic", channel.topic},
                 })},
    }));
  };

  reader.onMessage = [&](const mcap::Message& message, ByteOffset, std::optional<ByteOffset>) {
    recordsJson.push_back(json::object({
      {"type", "Message"},
      {"fields", json::array({
                   {"channel_id", std::to_string(message.channelId)},
                   {"data", ToJson(message.data, message.dataSize)},
                   {"log_time", std::to_string(message.logTime)},
                   {"publish_time", std::to_string(message.publishTime)},
                   {"sequence", std::to_string(message.sequence)},
                 })},
    }));
  };

  // reader.onChunk = [&](const mcap::Chunk& chunk, ByteOffset) {
  //   recordsJson.push_back(json::object({
  //     {"type", "Chunk"},
  //     {"fields", json::array({
  //                  {"compressed_size", std::to_string(chunk.compressedSize)},
  //                  {"compression", chunk.compression},
  //                  {"message_end_time", std::to_string(chunk.messageEndTime)},
  //                  {"message_start_time", std::to_string(chunk.messageStartTime)},
  //                  {"uncompressed_crc", std::to_string(chunk.uncompressedCrc)},
  //                  {"uncompressed_size", std::to_string(chunk.uncompressedSize)},
  //                })},
  //   }));
  // };

  // reader.onMessageIndex = [&](const mcap::MessageIndex& messageIndex, ByteOffset) {
  //   recordsJson.push_back(json::object({
  //     {"type", "MessageIndex"},
  //     {"fields", json::array({
  //                  {"channel_id", std::to_string(messageIndex.channelId)},
  //                  {"records", ToJson(messageIndex.records)},
  //                })},
  //   }));
  // };

  reader.onChunkIndex = [&](const mcap::ChunkIndex& chunkIndex, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "ChunkIndex"},
      {"fields", json::array({
                   {"chunk_length", std::to_string(chunkIndex.chunkLength)},
                   {"chunk_start_offset", std::to_string(chunkIndex.chunkStartOffset)},
                   {"compressed_size", std::to_string(chunkIndex.compressedSize)},
                   {"compression", chunkIndex.compression},
                   {"message_end_time", std::to_string(chunkIndex.messageEndTime)},
                   {"message_index_length", std::to_string(chunkIndex.messageIndexLength)},
                   {"message_index_offsets", ToJson(chunkIndex.messageIndexOffsets)},
                   {"message_start_time", std::to_string(chunkIndex.messageStartTime)},
                   {"uncompressed_size", std::to_string(chunkIndex.uncompressedSize)},
                 })},
    }));
  };

  reader.onAttachment = [&](const mcap::Attachment& attachment, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "Attachment"},
      {"fields", json::array({
                   {"create_time", std::to_string(attachment.createTime)},
                   {"data", ToJson(attachment.data, attachment.dataSize)},
                   {"log_time", std::to_string(attachment.logTime)},
                   {"media_type", attachment.mediaType},
                   {"name", attachment.name},
                 })},
    }));
  };

  reader.onAttachmentIndex = [&](const mcap::AttachmentIndex& attachmentIndex, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "AttachmentIndex"},
      {"fields", json::array({
                   {"create_time", std::to_string(attachmentIndex.createTime)},
                   {"data_size", std::to_string(attachmentIndex.dataSize)},
                   {"length", std::to_string(attachmentIndex.length)},
                   {"log_time", std::to_string(attachmentIndex.logTime)},
                   {"media_type", attachmentIndex.mediaType},
                   {"name", attachmentIndex.name},
                   {"offset", std::to_string(attachmentIndex.offset)},
                 })},
    }));
  };

  reader.onStatistics = [&](const mcap::Statistics& statistics, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "Statistics"},
      {"fields", json::array({
                   {"attachment_count", std::to_string(statistics.attachmentCount)},
                   {"channel_count", std::to_string(statistics.channelCount)},
                   {"channel_message_counts", ToJson(statistics.channelMessageCounts)},
                   {"chunk_count", std::to_string(statistics.chunkCount)},
                   {"message_count", std::to_string(statistics.messageCount)},
                   {"message_end_time", std::to_string(statistics.messageEndTime)},
                   {"message_start_time", std::to_string(statistics.messageStartTime)},
                   {"metadata_count", std::to_string(statistics.metadataCount)},
                   {"schema_count", std::to_string(statistics.schemaCount)},
                 })},
    }));
  };

  reader.onMetadata = [&](const mcap::Metadata& metadata, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "Metadata"},
      {"fields", json::array({
                   {"metadata", metadata.metadata},
                   {"name", metadata.name},
                 })},
    }));
  };

  reader.onMetadataIndex = [&](const mcap::MetadataIndex& metadataIndex, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "MetadataIndex"},
      {"fields", json::array({
                   {"length", std::to_string(metadataIndex.length)},
                   {"name", metadataIndex.name},
                   {"offset", std::to_string(metadataIndex.offset)},
                 })},
    }));
  };

  reader.onSummaryOffset = [&](const mcap::SummaryOffset& summaryOffset, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "SummaryOffset"},
      {"fields", json::array({
                   {"group_length", std::to_string(summaryOffset.groupLength)},
                   {"group_opcode", std::to_string(uint8_t(summaryOffset.groupOpCode))},
                   {"group_start", std::to_string(summaryOffset.groupStart)},
                 })},
    }));
  };

  reader.onDataEnd = [&](const mcap::DataEnd& dataEnd, ByteOffset) {
    recordsJson.push_back(json::object({
      {"type", "DataEnd"},
      {"fields", json::array({
                   {"data_section_crc", std::to_string(dataEnd.dataSectionCrc)},
                 })},
    }));
  };

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
