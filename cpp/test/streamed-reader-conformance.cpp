#include <mcap/mcap.hpp>

#include <nlohmann/json.hpp>

#include <iostream>

using json = nlohmann::json;

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
      {"fields", {{"library", header.library}, {"profile", header.profile}}},
    }));
  };

  reader.onFooter = [&](const mcap::Footer& footer) {
    recordsJson.push_back(json::object({
      {"type", "Footer"},
      {"fields",
       {{"summary_start", footer.summaryStart},
        {"summary_offset_start", footer.summaryOffsetStart},
        {"summary_crc", footer.summaryCrc}}},
    }));
  };

  reader.onSchema = [&](const mcap::Schema& schema) {
    recordsJson.push_back(json::object({
      {"type", "Schema"},
      {"fields", {{"id", schema.id}, {"name", schema.name}, {"encoding", schema.encoding}}},
    }));
  };

  reader.onChannel = [&](const mcap::Channel& channel) {
    recordsJson.push_back(json::object({
      {"type", "Channel"},
      {"fields",
       {{"id", channel.id},
        {"topic", channel.topic},
        {"message_encoding", channel.messageEncoding},
        {"schema_id", channel.schemaId},
        {"metadata", channel.metadata}}},
    }));
  };

  reader.onMessage = [&](const mcap::Message& message) {
    recordsJson.push_back(json::object({
      {"type", "Message"},
      {"fields",
       {{"channel_id", message.channelId},
        {"sequence", message.sequence},
        {"publish_time", message.publishTime},
        {"log_time", message.logTime}}},
    }));
  };

  reader.onChunk = [&](const mcap::Chunk& chunk) {
    recordsJson.push_back(json::object({
      {"type", "Chunk"},
      {"fields",
       {{"start_time", chunk.startTime},
        {"end_time", chunk.endTime},
        {"uncompressed_size", chunk.uncompressedSize},
        {"uncompressed_crc", chunk.uncompressedCrc},
        {"compression", chunk.compression},
        {"compressed_size", chunk.compressedSize}}},
    }));
  };

  reader.onMessageIndex = [&](const mcap::MessageIndex& messageIndex) {
    recordsJson.push_back(json::object({
      {"type", "MessageIndex"},
      {"fields", {{"channel_id", messageIndex.channelId}, {"records", messageIndex.records}}},
    }));
  };

  reader.onChunkIndex = [&](const mcap::ChunkIndex& chunkIndex) {
    recordsJson.push_back(json::object({
      {"type", "ChunkIndex"},
      {"fields",
       {{"start_time", chunkIndex.startTime},
        {"end_time", chunkIndex.endTime},
        {"chunk_start_offset", chunkIndex.chunkStartOffset}}},
    }));
  };

  reader.onAttachment = [&](const mcap::Attachment& attachment) {
    recordsJson.push_back(json::object({
      {"type", "Attachment"},
      {"fields",
       {{"name", attachment.name},
        {"created_at", attachment.createdAt},
        {"log_time", attachment.logTime},
        {"content_type", attachment.contentType}}},
    }));
  };

  reader.onAttachmentIndex = [&](const mcap::AttachmentIndex& attachmentIndex) {
    recordsJson.push_back(json::object({
      {"type", "AttachmentIndex"},
      {"fields",
       {{"offset", attachmentIndex.offset},
        {"length", attachmentIndex.length},
        {"log_time", attachmentIndex.logTime},
        {"data_size", attachmentIndex.dataSize},
        {"name", attachmentIndex.name},
        {"content_type", attachmentIndex.contentType}}},
    }));
  };

  reader.onStatistics = [&](const mcap::Statistics& statistics) {
    recordsJson.push_back(json::object({
      {"type", "Statistics"},
      {"fields",
       {{"message_count", statistics.messageCount},
        {"channel_count", statistics.channelCount},
        {"attachment_count", statistics.attachmentCount},
        {"metadata_count", statistics.metadataCount},
        {"chunk_count", statistics.chunkCount}}},
    }));
  };

  reader.onMetadata = [&](const mcap::Metadata& metadata) {
    recordsJson.push_back(json::object({
      {"type", "Metadata"},
      {"fields", {{"name", metadata.name}, {"metadata", metadata.metadata}}},
    }));
  };

  reader.onMetadataIndex = [&](const mcap::MetadataIndex& metadataIndex) {
    recordsJson.push_back(json::object({
      {"type", "MetadataIndex"},
      {"fields",
       {{"offset", metadataIndex.offset},
        {"length", metadataIndex.length},
        {"name", metadataIndex.name}}},
    }));
  };

  reader.onSummaryOffset = [&](const mcap::SummaryOffset& summaryOffset) {
    recordsJson.push_back(json::object({
      {"type", "SummaryOffset"},
      {"fields",
       {{"group_opcode", summaryOffset.groupOpCode},
        {"group_start", summaryOffset.groupStart},
        {"group_length", summaryOffset.groupLength}}},
    }));
  };

  reader.onDataEnd = [&](const mcap::DataEnd& dataEnd) {
    recordsJson.push_back(json::object({
      {"type", "DataEnd"},
      {"fields", {{"data_section_crc", dataEnd.dataSectionCrc}}},
    }));
  };

  while (reader.next()) {
  }

  json output = {{"records", recordsJson}};
  std::cout << output.dump(2) << "\n";
  return 0;
}
