#include <mcap/writer.hpp>

#include <nlohmann/json.hpp>

#include <fstream>
#include <iostream>

using json = nlohmann::json;

constexpr std::string_view UseChunks = "ch";
constexpr std::string_view UseMessageIndex = "mx";
constexpr std::string_view UseStatistics = "st";
constexpr std::string_view UseRepeatedSchemas = "rsh";
constexpr std::string_view UseRepeatedChannelInfos = "rch";
constexpr std::string_view UseAttachmentIndex = "ax";
constexpr std::string_view UseMetadataIndex = "mdx";
constexpr std::string_view UseChunkIndex = "chx";
constexpr std::string_view UseSummaryOffset = "sum";
constexpr std::string_view AddExtraDataToRecords = "pad";

mcap::McapWriterOptions ReadOptions(const json& featuresJson) {
  // ["ch", "mx", "st", "rsh", "rch", "chx", "sum", "pad"]
  mcap::McapWriterOptions options{""};
  options.compression = mcap::Compression::None;
  options.enableDataCRC = true;
  options.noChunking = true;
  options.noMessageIndex = true;
  options.noSummary = true;
  options.noRepeatedSchemas = true;
  options.noRepeatedChannels = true;
  options.noAttachmentIndex = true;
  options.noMetadataIndex = true;
  options.noChunkIndex = true;
  options.noStatistics = true;
  options.noSummaryOffsets = true;

  for (const auto& feature : featuresJson) {
    const auto featureString = feature.get<std::string>();
    if (featureString == UseChunks) {
      options.noChunking = false;
    } else if (featureString == UseMessageIndex) {
      options.noChunking = false;
      options.noSummary = false;
      options.noMessageIndex = false;
    } else if (featureString == UseStatistics) {
      options.noSummary = false;
      options.noStatistics = false;
    } else if (featureString == UseRepeatedSchemas) {
      options.noSummary = false;
      options.noRepeatedSchemas = false;
    } else if (featureString == UseRepeatedChannelInfos) {
      options.noSummary = false;
      options.noRepeatedChannels = false;
    } else if (featureString == UseAttachmentIndex) {
      options.noSummary = false;
      options.noAttachmentIndex = false;
    } else if (featureString == UseMetadataIndex) {
      options.noSummary = false;
      options.noMetadataIndex = false;
    } else if (featureString == UseChunkIndex) {
      options.noChunking = false;
      options.noSummary = false;
      options.noChunkIndex = false;
    } else if (featureString == UseSummaryOffset) {
      options.noSummary = false;
      options.noSummaryOffsets = false;
    } else if (featureString == AddExtraDataToRecords) {
      std::cerr << "AddExtraDataToRecords not supported\n";
      std::abort();
    } else {
      std::cerr << "Unknown feature: " << featureString << "\n";
      std::abort();
    }
  }

  return options;
}

template <typename T>
T ReadUInt(const json& obj) {
  return T(std::stoull(obj.get<std::string>()));
}

void ReadBytes(const json& byteArrayJson, mcap::ByteArray& output) {
  // ["1", "2", "3"]
  output.clear();
  for (const auto& byteJson : byteArrayJson) {
    output.push_back(std::byte(ReadUInt<uint8_t>(byteJson)));
  }
}

mcap::ByteArray ReadBytes(const json& byteArrayJson) {
  mcap::ByteArray buffer;
  ReadBytes(byteArrayJson, buffer);
  return buffer;
}

mcap::Header ReadHeader(const json& headerJson) {
  // {"type": "Header", "fields": [["library", ""], ["profile", ""]]},
  mcap::Header header;
  header.library = headerJson["fields"][0][1];
  header.profile = headerJson["fields"][1][1];
  return header;
}

mcap::Schema ReadSchema(const json& schemaJson) {
  // std::cout << "Reading schema: " << schemaJson << "\n";
  // {
  //   "type": "Schema",
  //   "fields": [
  //     ["data", ["4", "5", "6"]],
  //     ["encoding", "c"],
  //     ["id", "1"],
  //     ["name", "Example"]
  //   ]
  // },
  mcap::Schema schema;
  schema.data = ReadBytes(schemaJson["fields"][0][1]);
  schema.encoding = schemaJson["fields"][1][1];
  schema.id = ReadUInt<uint16_t>(schemaJson["fields"][2][1]);
  schema.name = schemaJson["fields"][3][1];
  return schema;
}

mcap::Channel ReadChannel(const json& channelJson) {
  // std::cout << "Reading channel: " << channelJson << "\n";
  // {
  //   "type": "Channel",
  //   "fields": [
  //     ["id", "1"],
  //     ["message_encoding", "a"],
  //     ["metadata", {"foo": "bar"}],
  //     ["schema_id", "1"],
  //     ["topic", "example"]
  //   ]
  // },
  mcap::Channel channel;
  channel.id = ReadUInt<uint16_t>(channelJson["fields"][0][1]);
  channel.messageEncoding = channelJson["fields"][1][1];
  channel.metadata = channelJson["fields"][2][1];
  channel.schemaId = ReadUInt<uint16_t>(channelJson["fields"][3][1]);
  channel.topic = channelJson["fields"][4][1];
  return channel;
}

mcap::Message ReadMessage(const json& messageJson, mcap::ByteArray& buffer) {
  // std::cout << "Reading message: " << messageJson << "\n";
  // {
  //   "type": "Message",
  //   "fields": [
  //     ["channel_id", "1"],
  //     ["data", ["1", "2", "3"]],
  //     ["log_time", "2"],
  //     ["publish_time", "1"],
  //     ["sequence", "10"]
  //   ]
  // },
  mcap::Message message;
  message.channelId = ReadUInt<uint16_t>(messageJson["fields"][0][1]);
  ReadBytes(messageJson["fields"][1][1], buffer);
  message.data = buffer.data();
  message.dataSize = buffer.size();
  message.logTime = ReadUInt<uint64_t>(messageJson["fields"][2][1]);
  message.publishTime = ReadUInt<uint64_t>(messageJson["fields"][3][1]);
  message.sequence = ReadUInt<uint32_t>(messageJson["fields"][4][1]);
  return message;
}

mcap::Attachment ReadAttachment(const json& attachmentJson, mcap::ByteArray& buffer) {
  // {
  //   "type": "Attachment",
  //   "fields": [
  //     ["create_time", "1"],
  //     ["data", ["1", "2", "3"]],
  //     ["log_time", "2"],
  //     ["media_type", "application/octet-stream"],
  //     ["name", "myFile"]
  //   ]
  // },
  mcap::Attachment attachment;
  attachment.createTime = ReadUInt<uint64_t>(attachmentJson["fields"][0][1]);
  ReadBytes(attachmentJson["fields"][1][1], buffer);
  attachment.data = buffer.data();
  attachment.dataSize = buffer.size();
  attachment.logTime = ReadUInt<uint64_t>(attachmentJson["fields"][2][1]);
  attachment.mediaType = attachmentJson["fields"][3][1];
  attachment.name = attachmentJson["fields"][4][1];
  return attachment;
}

mcap::Metadata ReadMetadata(const json& metadataJson) {
  // {
  //   "type": "Metadata",
  //   "fields": [["metadata", {"foo": "bar"}], ["name", "myMetadata"]]
  // },
  mcap::Metadata metadata;
  metadata.metadata = metadataJson["fields"][0][1];
  metadata.name = metadataJson["fields"][1][1];
  return metadata;
}

class StdoutWriter final : public mcap::IWritable {
public:
  void handleWrite(const std::byte* data, uint64_t size) override {
    std::cout.write(reinterpret_cast<const char*>(data), size);
    size_ += size;
  }
  void end() override {
    std::cout << std::flush;
  }
  uint64_t size() const override {
    return size_;
  }

private:
  uint64_t size_ = 0;
};

static void assertOk(const mcap::Status& status) {
  if (!status.ok()) {
    throw std::runtime_error(status.message);
  }
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  // Read and parse the input file
  const std::string inputFile = argv[1];
  std::ifstream inputStream(inputFile);
  json input;
  inputStream >> input;

  auto options = ReadOptions(input["meta"]["variant"]["features"]);

  StdoutWriter stdoutWriter;
  mcap::McapWriter mcapWriter;

  for (const json& record : input["records"]) {
    // Read the record type
    const std::string recordType = record["type"];

    if (recordType == "Header") {
      const auto header = ReadHeader(record);
      options.profile = header.profile;
      options.library = header.library;
      mcapWriter.open(stdoutWriter, options);
    } else if (recordType == "Schema") {
      auto schema = ReadSchema(record);
      mcapWriter.addSchema(schema);
    } else if (recordType == "Channel") {
      auto channel = ReadChannel(record);
      mcapWriter.addChannel(channel);
    } else if (recordType == "Message") {
      mcap::ByteArray buffer;
      const auto message = ReadMessage(record, buffer);
      assertOk(mcapWriter.write(message));
    } else if (recordType == "Attachment") {
      mcap::ByteArray buffer;
      auto attachment = ReadAttachment(record, buffer);
      assertOk(mcapWriter.write(attachment));
    } else if (recordType == "Metadata") {
      auto metadata = ReadMetadata(record);
      assertOk(mcapWriter.write(metadata));
    } else if (recordType == "DataEnd") {
      mcapWriter.close();
      return 0;
    } else {
      std::cerr << "Unknown record type: " << recordType << "\n";
      std::abort();
    }
  }
}
