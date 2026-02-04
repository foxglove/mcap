#define MCAP_IMPLEMENTATION
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

static void onProblem(const mcap::Status& problem) {
  std::cerr << "failed to read: " << problem.message << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  json messages = json::array();
  json schemas = json::array();
  json channels = json::array();
  json statistics = json::array();

  std::unordered_set<uint16_t> knownSchemas;
  std::unordered_set<uint16_t> knownChannels;

  const std::string inputFile = argv[1];
  std::ifstream input(inputFile, std::ios::binary);
  mcap::McapReader reader;
  auto status = reader.open(input);
  if (!status.ok()) {
    std::cerr << "Could not read input file " << inputFile << ": " << status.message << std::endl;
  }
  status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
  if (!status.ok()) {
    std::cerr << "Could not read summary: " << status.message << std::endl;
  }
  mcap::ReadMessageOptions options;
  options.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
  for (const auto& msgView : reader.readMessages(onProblem, options)) {
    if (knownSchemas.find(msgView.schema->id) == knownSchemas.end()) {
      schemas.push_back(json::object({
        {"type", "Schema"},
        {"fields", json::array({
                     {"data", ToJson(msgView.schema->data.data(), msgView.schema->data.size())},
                     {"encoding", msgView.schema->encoding},
                     {"id", std::to_string(msgView.schema->id)},
                     {"name", msgView.schema->name},
                   })},
      }));
      knownSchemas.emplace(msgView.schema->id);
    }
    if (knownChannels.find(msgView.channel->id) == knownChannels.end()) {
      channels.push_back(json::object({
        {"type", "Channel"},
        {"fields", json::array({
                     {"id", std::to_string(msgView.channel->id)},
                     {"message_encoding", msgView.channel->messageEncoding},
                     {"metadata", msgView.channel->metadata},
                     {"schema_id", std::to_string(msgView.channel->schemaId)},
                     {"topic", msgView.channel->topic},
                   })},
      }));
      knownChannels.emplace(msgView.channel->id);
    }
    messages.push_back(json::object({
      {"type", "Message"},
      {"fields", json::array({
                   {"channel_id", std::to_string(msgView.message.channelId)},
                   {"data", ToJson(msgView.message.data, msgView.message.dataSize)},
                   {"log_time", std::to_string(msgView.message.logTime)},
                   {"publish_time", std::to_string(msgView.message.publishTime)},
                   {"sequence", std::to_string(msgView.message.sequence)},
                 })},
    }));
  }
  auto statsRecord = reader.statistics();
  if (statsRecord != std::nullopt) {
    statistics.push_back(json::object({
      {"type", "Statistics"},
      {"fields", json::array({
                   {"attachment_count", std::to_string(statsRecord->attachmentCount)},
                   {"channel_count", std::to_string(statsRecord->channelCount)},
                   {"channel_message_counts", ToJson(statsRecord->channelMessageCounts)},
                   {"chunk_count", std::to_string(statsRecord->chunkCount)},
                   {"message_count", std::to_string(statsRecord->messageCount)},
                   {"message_end_time", std::to_string(statsRecord->messageEndTime)},
                   {"message_start_time", std::to_string(statsRecord->messageStartTime)},
                   {"metadata_count", std::to_string(statsRecord->metadataCount)},
                   {"schema_count", std::to_string(statsRecord->schemaCount)},
                 })},
    }));
  }
  json output = {
    {"messages", messages},
    {"schemas", schemas},
    {"channels", channels},
    {"statistics", statistics},
  };
  std::cout << output.dump() << "\n";
  return 0;
}
