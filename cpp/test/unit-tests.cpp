#define MCAP_IMPLEMENTATION
#include <mcap/mcap.hpp>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <array>
#include <numeric>

std::string_view StringView(const std::byte* data, size_t size) {
  return std::string_view{reinterpret_cast<const char*>(data), size};
}

struct Buffer : mcap::IReadable, mcap::IWritable {
  std::vector<std::byte> buffer;

  virtual uint64_t size() const {
    return buffer.size();
  }

  // IWritable
  virtual void end() {}
  virtual void handleWrite(const std::byte* data, uint64_t size) {
    buffer.insert(buffer.end(), data, data + size);
  }

  // IReadable
  virtual uint64_t read(std::byte** output, uint64_t offset, uint64_t size) {
    if (offset + size > buffer.size()) {
      return 0;
    }
    *output = buffer.data() + offset;
    return size;
  }
};

void requireOk(const mcap::Status& status) {
  CAPTURE(status.code);
  CAPTURE(status.message);
  REQUIRE(status.ok());
}

static void writeMsg(mcap::McapWriter& writer, mcap::ChannelId channelId, uint32_t sequence,
                     mcap::Timestamp logTime, mcap::Timestamp publishTime,
                     const std::vector<std::byte>& data) {
  mcap::Message msg;
  msg.channelId = channelId;
  msg.sequence = sequence;
  msg.logTime = logTime;
  msg.publishTime = publishTime;
  msg.data = data.data();
  msg.dataSize = data.size();
  requireOk(writer.write(msg));
}

static void writeExampleFile(Buffer& buffer) {
  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("");
  opts.library = "";
  opts.noRepeatedChannels = true;
  opts.noRepeatedSchemas = true;
  opts.noStatistics = true;
  opts.noSummaryOffsets = true;
  opts.compression = mcap::Compression::None;
  writer.open(buffer, opts);
  mcap::Schema schema("Example", "c", "\x04\x05\x06");
  writer.addSchema(schema);
  mcap::Channel channel("example", "a", schema.id, {{"foo", "bar"}});
  writer.addChannel(channel);
  std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
  writeMsg(writer, channel.id, 10, 2, 1, data);
  writer.close();
}

TEST_CASE("internal::crc32", "[writer]") {
  const auto crc32 = [](const uint8_t* data, size_t len) {
    return mcap::internal::crc32Final(mcap::internal::crc32Update(
      mcap::internal::CRC32_INIT, reinterpret_cast<const std::byte*>(data), len));
  };

  std::array<uint8_t, 32> data;
  std::iota(data.begin(), data.end(), 1);

  REQUIRE(crc32(data.data(), 0) == 0);
  REQUIRE(crc32(data.data(), 1) == 2768625435);

  for (size_t split = 0; split <= data.size(); split++) {
    CAPTURE(split);
    uint32_t crc = mcap::internal::CRC32_INIT;
    crc = mcap::internal::crc32Update(crc, reinterpret_cast<const std::byte*>(data.data()), split);
    crc = mcap::internal::crc32Update(crc, reinterpret_cast<const std::byte*>(data.data() + split),
                                      data.size() - split);
    REQUIRE(mcap::internal::crc32Final(crc) == 2280057893);
  }
}

TEST_CASE("internal::Parse*()", "[reader]") {
  SECTION("uint64_t") {
    const std::array<std::byte, 8> input = {std::byte(0xef), std::byte(0xcd), std::byte(0xab),
                                            std::byte(0x90), std::byte(0x78), std::byte(0x56),
                                            std::byte(0x34), std::byte(0x12)};
    REQUIRE(mcap::internal::ParseUint64(input.data()) == 0x1234567890abcdefull);
  }
}

TEST_CASE("McapWriter::write()", "[writer]") {
  SECTION("uint8_t") {
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, mcap::OpCode::DataEnd);
    REQUIRE(output.size() == 1);
    REQUIRE(uint8_t(output.data()[0]) == uint8_t(mcap::OpCode::DataEnd));
  }

  SECTION("uint16_t") {
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, uint16_t(0x1234));
    REQUIRE(output.size() == 2);
    REQUIRE(uint8_t(output.data()[0]) == 0x34);
    REQUIRE(uint8_t(output.data()[1]) == 0x12);
  }

  SECTION("uint32_t") {
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, uint32_t(0x12345678));
    REQUIRE(output.size() == 4);
    REQUIRE(uint8_t(output.data()[0]) == 0x78);
    REQUIRE(uint8_t(output.data()[1]) == 0x56);
    REQUIRE(uint8_t(output.data()[2]) == 0x34);
    REQUIRE(uint8_t(output.data()[3]) == 0x12);
  }

  SECTION("uint64_t") {
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, uint64_t(0x1234567890abcdef));
    REQUIRE(output.size() == 8);
    REQUIRE(uint8_t(output.data()[0]) == 0xef);
    REQUIRE(uint8_t(output.data()[1]) == 0xcd);
    REQUIRE(uint8_t(output.data()[2]) == 0xab);
    REQUIRE(uint8_t(output.data()[3]) == 0x90);
    REQUIRE(uint8_t(output.data()[4]) == 0x78);
    REQUIRE(uint8_t(output.data()[5]) == 0x56);
    REQUIRE(uint8_t(output.data()[6]) == 0x34);
    REQUIRE(uint8_t(output.data()[7]) == 0x12);
  }

  SECTION("byte*") {
    std::array<std::byte, 5> input = {std::byte(0x12), std::byte(0x34), std::byte(0x56),
                                      std::byte(0x78), std::byte(0x9a)};
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, input.data(), input.size());
    REQUIRE(output.size() == 5);
    REQUIRE(uint8_t(output.data()[0]) == 0x12);
    REQUIRE(uint8_t(output.data()[1]) == 0x34);
    REQUIRE(uint8_t(output.data()[2]) == 0x56);
    REQUIRE(uint8_t(output.data()[3]) == 0x78);
    REQUIRE(uint8_t(output.data()[4]) == 0x9a);
  }

  SECTION("string_view") {
    std::string_view input = "Hello, world!";
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, input);
    REQUIRE(output.size() == 17);
    REQUIRE(uint8_t(output.data()[0]) == 0x0d);
    REQUIRE(uint8_t(output.data()[1]) == 0x00);
    REQUIRE(uint8_t(output.data()[2]) == 0x00);
    REQUIRE(uint8_t(output.data()[3]) == 0x00);

    const std::string_view outputString =
      std::string_view{reinterpret_cast<const char*>(output.data() + 4), 13};
    REQUIRE(outputString.size() == input.size());
    REQUIRE(outputString == input);
  }

  SECTION("ByteArray") {
    mcap::ByteArray input = {std::byte(0x12), std::byte(0x34), std::byte(0x56), std::byte(0x78),
                             std::byte(0x9a)};
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, input);
    REQUIRE(output.size() == 9);
    REQUIRE(uint8_t(output.data()[0]) == 0x05);
    REQUIRE(uint8_t(output.data()[1]) == 0x00);
    REQUIRE(uint8_t(output.data()[2]) == 0x00);
    REQUIRE(uint8_t(output.data()[3]) == 0x00);
    REQUIRE(uint8_t(output.data()[4]) == 0x12);
    REQUIRE(uint8_t(output.data()[5]) == 0x34);
    REQUIRE(uint8_t(output.data()[6]) == 0x56);
    REQUIRE(uint8_t(output.data()[7]) == 0x78);
    REQUIRE(uint8_t(output.data()[8]) == 0x9a);
  }

  SECTION("KeyValueMap") {
    mcap::KeyValueMap input = {{"key", "value"}, {"key2", "value2"}};
    mcap::BufferWriter output;
    mcap::McapWriter::write(output, input);
    REQUIRE(output.size() == 4 + 4 + 3 + 4 + 5 + 4 + 4 + 4 + 6);
    // Total byte length of the map
    REQUIRE(uint8_t(output.data()[0]) == 34);
    REQUIRE(uint8_t(output.data()[1]) == 0x00);
    REQUIRE(uint8_t(output.data()[2]) == 0x00);
    REQUIRE(uint8_t(output.data()[3]) == 0x00);
    // Length of "key"
    REQUIRE(uint8_t(output.data()[4]) == 0x03);
    REQUIRE(uint8_t(output.data()[5]) == 0x00);
    REQUIRE(uint8_t(output.data()[6]) == 0x00);
    REQUIRE(uint8_t(output.data()[7]) == 0x00);
    // "key"
    REQUIRE(StringView(output.data() + 8, 3) == "key");
    // Length of "value"
    REQUIRE(uint8_t(output.data()[11]) == 0x05);
    REQUIRE(uint8_t(output.data()[12]) == 0x00);
    REQUIRE(uint8_t(output.data()[13]) == 0x00);
    REQUIRE(uint8_t(output.data()[14]) == 0x00);
    // "value"
    REQUIRE(StringView(output.data() + 15, 5) == "value");
    // Length of "key2"
    REQUIRE(uint8_t(output.data()[20]) == 0x04);
    REQUIRE(uint8_t(output.data()[21]) == 0x00);
    REQUIRE(uint8_t(output.data()[22]) == 0x00);
    REQUIRE(uint8_t(output.data()[23]) == 0x00);
    // "key2"
    REQUIRE(StringView(output.data() + 24, 4) == "key2");
    // Length of "value2"
    REQUIRE(uint8_t(output.data()[28]) == 0x06);
    REQUIRE(uint8_t(output.data()[29]) == 0x00);
    REQUIRE(uint8_t(output.data()[30]) == 0x00);
    REQUIRE(uint8_t(output.data()[31]) == 0x00);
    // "value2"
    REQUIRE(StringView(output.data() + 32, 6) == "value2");
  }
}

TEST_CASE("McapReader::readSummary()", "[reader]") {
  SECTION("NoFallbackScan") {
    mcap::McapReader reader;
    Buffer buffer;
    writeExampleFile(buffer);
    auto status = reader.open(buffer);
    requireOk(status);

    status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
    REQUIRE(status.code == mcap::StatusCode::MissingStatistics);

    const auto& chunkIndexes = reader.chunkIndexes();
    REQUIRE(chunkIndexes.size() == 1);
    const auto& chunkIndex = chunkIndexes.front();
    REQUIRE(chunkIndex.messageStartTime == 2);
    REQUIRE(chunkIndex.messageEndTime == 2);
    REQUIRE(chunkIndex.chunkStartOffset == 25);
    REQUIRE(chunkIndex.chunkLength == 164);
    REQUIRE(chunkIndex.messageIndexOffsets.size() == 1);
    REQUIRE(chunkIndex.messageIndexOffsets.at(1) == 189);
    REQUIRE(chunkIndex.messageIndexLength == 31);
    REQUIRE(chunkIndex.compression == "");
    REQUIRE(chunkIndex.compressedSize == 115);
    REQUIRE(chunkIndex.uncompressedSize == 115);

    REQUIRE(!reader.statistics().has_value());
  }

  SECTION("AllowFallbackScan") {
    mcap::McapReader reader;
    Buffer buffer;
    writeExampleFile(buffer);
    auto status = reader.open(buffer);
    requireOk(status);

    status = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    requireOk(status);

    const auto& chunkIndexes = reader.chunkIndexes();
    REQUIRE(chunkIndexes.size() == 1);
    const auto& chunkIndex = chunkIndexes.front();
    REQUIRE(chunkIndex.messageStartTime == 2);
    REQUIRE(chunkIndex.messageEndTime == 2);
    REQUIRE(chunkIndex.chunkStartOffset == 25);
    REQUIRE(chunkIndex.chunkLength == 164);
    REQUIRE(chunkIndex.messageIndexOffsets.size() == 0);
    REQUIRE(chunkIndex.messageIndexLength == 0);
    REQUIRE(chunkIndex.compression == "");
    REQUIRE(chunkIndex.compressedSize == 115);
    REQUIRE(chunkIndex.uncompressedSize == 115);

    const auto maybeStats = reader.statistics();
    REQUIRE(maybeStats.has_value());
    const auto& stats = *maybeStats;
    REQUIRE(stats.messageCount == 1);
    REQUIRE(stats.schemaCount == 1);
    REQUIRE(stats.channelCount == 1);
    REQUIRE(stats.attachmentCount == 0);
    REQUIRE(stats.metadataCount == 0);
    REQUIRE(stats.chunkCount == 1);
    REQUIRE(stats.messageStartTime == 2);
    REQUIRE(stats.messageEndTime == 2);
    REQUIRE(stats.channelMessageCounts.size() == 1);
    REQUIRE(stats.channelMessageCounts.at(1) == 1);
  }

  SECTION("ForceScan") {
    mcap::McapReader reader;
    Buffer buffer;
    writeExampleFile(buffer);
    auto status = reader.open(buffer);
    requireOk(status);

    status = reader.readSummary(mcap::ReadSummaryMethod::ForceScan);
    requireOk(status);

    const auto& chunkIndexes = reader.chunkIndexes();
    REQUIRE(chunkIndexes.size() == 1);
    const auto& chunkIndex = chunkIndexes.front();
    REQUIRE(chunkIndex.messageStartTime == 2);
    REQUIRE(chunkIndex.messageEndTime == 2);
    REQUIRE(chunkIndex.chunkStartOffset == 25);
    REQUIRE(chunkIndex.chunkLength == 164);
    REQUIRE(chunkIndex.messageIndexOffsets.size() == 0);
    REQUIRE(chunkIndex.messageIndexLength == 0);
    REQUIRE(chunkIndex.compression == "");
    REQUIRE(chunkIndex.compressedSize == 115);
    REQUIRE(chunkIndex.uncompressedSize == 115);

    const auto maybeStats = reader.statistics();
    REQUIRE(maybeStats.has_value());
    const auto& stats = *maybeStats;
    REQUIRE(stats.messageCount == 1);
    REQUIRE(stats.schemaCount == 1);
    REQUIRE(stats.channelCount == 1);
    REQUIRE(stats.attachmentCount == 0);
    REQUIRE(stats.metadataCount == 0);
    REQUIRE(stats.chunkCount == 1);
    REQUIRE(stats.messageStartTime == 2);
    REQUIRE(stats.messageEndTime == 2);
    REQUIRE(stats.channelMessageCounts.size() == 1);
    REQUIRE(stats.channelMessageCounts.at(1) == 1);
  }
}

TEST_CASE("McapReader::byteRange()", "[reader]") {
  SECTION("After open()") {
    mcap::McapReader reader;
    Buffer buffer;
    writeExampleFile(buffer);
    auto status = reader.open(buffer);
    requireOk(status);

    auto [startOffset, endOffset] = reader.byteRange(0);
    REQUIRE(startOffset == 25);
    REQUIRE(endOffset == 316);

    auto [startOffset2, endOffset2] = reader.byteRange(0, 0);
    REQUIRE(startOffset2 == 25);
    REQUIRE(endOffset2 == 316);

    reader.close();
  }

  SECTION("After readSummary()") {
    mcap::McapReader reader;
    Buffer buffer;
    writeExampleFile(buffer);
    auto status = reader.open(buffer);
    requireOk(status);

    status = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    requireOk(status);

    auto [startOffset, endOffset] = reader.byteRange(0);
    REQUIRE(startOffset == 25);
    REQUIRE(endOffset == 189);

    auto [startOffset2, endOffset2] = reader.byteRange(0, 0);
    REQUIRE(startOffset2 == 0);
    REQUIRE(endOffset2 == 0);

    auto [startOffset3, endOffset3] = reader.byteRange(1, 2);
    REQUIRE(startOffset3 == 25);
    REQUIRE(endOffset3 == 189);

    auto [startOffset4, endOffset4] = reader.byteRange(2, 3);
    REQUIRE(startOffset4 == 25);
    REQUIRE(endOffset4 == 189);

    auto [startOffset5, endOffset5] = reader.byteRange(3, 4);
    REQUIRE(startOffset5 == 0);
    REQUIRE(endOffset5 == 0);

    reader.close();
  }
}

TEST_CASE("McapReader::readMessages()", "[reader]") {
  SECTION("Empty file") {
    Buffer buffer;

    mcap::McapWriter writer;
    writer.open(buffer, mcap::McapWriterOptions("test"));
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);
    writer.close();

    mcap::McapReader reader;
    auto status = reader.open(buffer);
    requireOk(status);

    for (const auto& msg : reader.readMessages()) {
      FAIL("Shouldn't have gotten a message: topic " + msg.channel->topic + ", schema " +
           msg.schema->name);
    }

    reader.close();
  }

  SECTION("MovableIterators") {
    Buffer buffer;

    mcap::McapWriter writer;
    writer.open(buffer, mcap::McapWriterOptions("test"));
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    writeMsg(writer, channel.id, 0, 2, 1, data);
    writeMsg(writer, channel.id, 1, 4, 3, data);
    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

    auto view = reader.readMessages();
    {
      auto it = view.begin();
      REQUIRE(it != view.end());
      auto* originalMsg = &it->message;
      REQUIRE(it->message.sequence == 0);
      REQUIRE(it->message.channelId == channel.id);
      REQUIRE(it->message.logTime == 2);
      REQUIRE(it->message.publishTime == 1);
      REQUIRE(it->message.dataSize == data.size());
      REQUIRE(std::vector(it->message.data, it->message.data + it->message.dataSize) == data);

      // ensure iterator still works after move
      mcap::LinearMessageView::Iterator other = std::move(it);
      REQUIRE(&other->message == originalMsg);

      REQUIRE(other->message.sequence == 0);
      REQUIRE(other->message.channelId == channel.id);
      REQUIRE(other->message.logTime == 2);
      REQUIRE(other->message.publishTime == 1);
      REQUIRE(other->message.dataSize == data.size());
      REQUIRE(std::vector(other->message.data, other->message.data + other->message.dataSize) ==
              data);
    }

    {
      auto it = view.begin();
      ++it;
      REQUIRE(it->message.sequence == 1);
      REQUIRE(it->message.channelId == channel.id);
      REQUIRE(it->message.logTime == 4);
      REQUIRE(it->message.publishTime == 3);
      REQUIRE(it->message.dataSize == data.size());
      REQUIRE(std::vector(it->message.data, it->message.data + it->message.dataSize) == data);
    }

    for (const auto& msg : view) {
      REQUIRE((msg.message.sequence == 0 || msg.message.sequence == 1));
      REQUIRE(msg.message.channelId == channel.id);
      REQUIRE((msg.message.logTime == 2 || msg.message.logTime == 4));
      REQUIRE((msg.message.publishTime == 1 || msg.message.publishTime == 3));
      REQUIRE(msg.message.dataSize == msg.message.dataSize);
      REQUIRE(std::vector(msg.message.data, msg.message.data + msg.message.dataSize) == data);
    }

    reader.close();
  }
}

TEST_CASE("LZ4 compression", "[reader][writer]") {
  SECTION("Roundtrip") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::Lz4;
    opts.forceCompression = true;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);

    mcap::Message msg;
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    writeMsg(writer, channel.id, 0, 2, 1, data);

    writer.close();

    mcap::McapReader reader;
    auto status = reader.open(buffer);
    requireOk(status);

    size_t messageCount = 0;
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };
    for (const auto& msgView : reader.readMessages(onProblem)) {
      ++messageCount;
      REQUIRE(msgView.message.sequence == 0);
      REQUIRE(msgView.message.channelId == channel.id);
      REQUIRE(msgView.message.logTime == 2);
      REQUIRE(msgView.message.publishTime == 1);
      REQUIRE(msgView.message.dataSize == data.size());
      REQUIRE(std::vector(msgView.message.data, msgView.message.data + msgView.message.dataSize) ==
              data);
    }
    REQUIRE(messageCount == 1);

    reader.close();
  }

  SECTION("Roundtrip two topics") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::Lz4;
    opts.forceCompression = true;
    writer.open(buffer, opts);
    mcap::Schema schema1("schema1", "schemaEncoding", "ab");
    writer.addSchema(schema1);
    mcap::Channel channel1("topic1", "messageEncoding", schema1.id);
    writer.addChannel(channel1);
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    writeMsg(writer, channel1.id, 0, 2, 1, data);

    mcap::Schema schema2("schema2", "schemaEncoding", "ab");
    writer.addSchema(schema2);
    mcap::Channel channel2("topic1", "messageEncoding", schema2.id);
    writer.addChannel(channel2);
    writeMsg(writer, channel2.id, 1, 2, 1, data);

    writer.close();

    mcap::McapReader reader;
    auto status = reader.open(buffer);
    requireOk(status);

    size_t messageCount = 0;
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };
    for (const auto& msgView : reader.readMessages(onProblem)) {
      REQUIRE(msgView.message.sequence == messageCount);
      if (msgView.message.sequence == 0) {
        REQUIRE(msgView.message.channelId == channel1.id);
      } else {
        REQUIRE(msgView.message.channelId == channel2.id);
      }
      REQUIRE(msgView.message.logTime == 2);
      REQUIRE(msgView.message.publishTime == 1);
      REQUIRE(msgView.message.dataSize == data.size());
      REQUIRE(std::vector(msgView.message.data, msgView.message.data + msgView.message.dataSize) ==
              data);
      ++messageCount;
    }
    REQUIRE(messageCount == 2);

    reader.close();
  }
}

TEST_CASE("Read Order", "[reader][writer]") {
  SECTION("Roundtrip unordered") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::Lz4;
    opts.forceCompression = true;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);

    mcap::Message msg;
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    writeMsg(writer, channel.id, 0, 0, 0, data);
    writeMsg(writer, channel.id, 2, 2, 2, data);
    writeMsg(writer, channel.id, 1, 1, 1, data);

    writer.close();

    mcap::McapReader reader;
    auto status = reader.open(buffer);
    requireOk(status);

    size_t messageCount = 0;
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };

    mcap::ReadMessageOptions options;
    options.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      REQUIRE(msgView.message.sequence == messageCount);
      REQUIRE(msgView.message.logTime == messageCount);
      REQUIRE(msgView.message.publishTime == messageCount);
      ++messageCount;
    }
    REQUIRE(messageCount == 3);

    options.readOrder = mcap::ReadMessageOptions::ReadOrder::ReverseLogTimeOrder;
    messageCount = 0;
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      REQUIRE(msgView.message.sequence == (2 - messageCount));
      REQUIRE(msgView.message.logTime == (2 - messageCount));
      REQUIRE(msgView.message.publishTime == (2 - messageCount));
      ++messageCount;
    }
    REQUIRE(messageCount == 3);

    reader.close();
  }
}
