#define MCAP_IMPLEMENTATION
#include <mcap/mcap.hpp>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <array>
#include <cstdio>
#include <numeric>

#if defined _WIN32 || defined __CYGWIN__
#  include <io.h>
#  include <ioapiset.h>
#  include <winioctl.h>
#endif

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

static void WriteMsg(mcap::McapWriter& writer, mcap::ChannelId channelId, uint32_t sequence,
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
  WriteMsg(writer, channel.id, 10, 2, 1, data);
  writer.close();
}

TEST_CASE("internal::crc32", "[writer]") {
  const auto crc32 = [](const uint8_t* data, size_t len) {
    return mcap::internal::crc32Final(mcap::internal::crc32Update(
      mcap::internal::CRC32_INIT, reinterpret_cast<const std::byte*>(data), len));
  };

  std::array<uint8_t, 32> data;
  std::iota(data.begin(), data.end(), (uint8_t)(1));

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
    requireOk(reader.open(buffer));

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
    requireOk(reader.open(buffer));

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
    WriteMsg(writer, channel.id, 0, 2, 1, data);
    WriteMsg(writer, channel.id, 1, 4, 3, data);
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

  SECTION("IteratorComparison") {
    Buffer buffer;

    mcap::McapWriter writer;
    writer.open(buffer, mcap::McapWriterOptions("test"));
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    WriteMsg(writer, channel.id, 0, 2, 1, data);
    WriteMsg(writer, channel.id, 1, 4, 3, data);
    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

    auto view = reader.readMessages();
    auto it = view.begin();
    REQUIRE(it != view.end());
    REQUIRE(it == view.begin());
    REQUIRE(it == it);
    ++it;
    REQUIRE(it != view.end());
    REQUIRE(it != view.begin());
    REQUIRE(it == it);
    ++it;
    REQUIRE(it == view.end());
    REQUIRE(it != view.begin());
    REQUIRE(it == it);

    reader.close();
  }
  SECTION("IteratorComparisonEmpty") {
    Buffer buffer;

    mcap::McapWriter writer;
    writer.open(buffer, mcap::McapWriterOptions("test"));
    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

    auto view = reader.readMessages();
    auto it = view.begin();
    REQUIRE(it == view.begin());
    REQUIRE(it == view.end());
    reader.close();
  }
}

/**
 * @brief ensures that message index records are only written for the channels present in the
 * previous chunk. This test writes two chunks with one message each in separate channels, with
 * the second message being large enough to guarantee the current chunk will be written out.
 * If the writer is working correctly, there will be one message index record after each chunk,
 * one for each message.
 */
TEST_CASE("Message index records", "[writer]") {
  Buffer buffer;

  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("test");
  opts.chunkSize = 200;
  opts.compression = mcap::Compression::None;

  writer.open(buffer, opts);

  mcap::Schema schema("schema", "schemaEncoding", "ab");
  writer.addSchema(schema);
  mcap::Channel channel1("topic", "messageEncoding", schema.id);
  writer.addChannel(channel1);
  mcap::Channel channel2("topic", "messageEncoding", schema.id);
  writer.addChannel(channel2);

  mcap::Message msg;
  // First message should not fill first chunk.
  WriteMsg(writer, channel1.id, 0, 100, 100, std::vector<std::byte>{20});
  // Second message fills current chunk and triggers a new one.
  WriteMsg(writer, channel2.id, 0, 200, 200, std::vector<std::byte>{400});

  writer.close();

  // read the records after the starting magic, stopping before the end magic.
  mcap::RecordReader reader(buffer, sizeof(mcap::Magic), buffer.size() - sizeof(mcap::Magic));

  std::vector<uint16_t> messageIndexChannelIds;
  uint32_t chunkCount = 0;

  for (std::optional<mcap::Record> rec = reader.next(); rec != std::nullopt; rec = reader.next()) {
    requireOk(reader.status());
    if (rec->opcode == mcap::OpCode::MessageIndex) {
      mcap::MessageIndex index;
      requireOk(mcap::McapReader::ParseMessageIndex(*rec, &index));
      REQUIRE(index.records.size() == 1);
      messageIndexChannelIds.push_back(index.channelId);
    }
    if (rec->opcode == mcap::OpCode::Chunk) {
      chunkCount++;
    }
  }
  requireOk(reader.status());

  REQUIRE(chunkCount == 2);
  REQUIRE(messageIndexChannelIds.size() == 2);
  REQUIRE(messageIndexChannelIds[0] == channel1.id);
  REQUIRE(messageIndexChannelIds[1] == channel2.id);
}

#ifndef MCAP_COMPRESSION_NO_LZ4
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
    WriteMsg(writer, channel.id, 0, 2, 1, data);

    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

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
}
#endif

#ifndef MCAP_COMPRESSION_NO_LZ4
TEST_CASE("zstd compression", "[reader][writer]") {
  SECTION("Roundtrip") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::Zstd;
    opts.forceCompression = true;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);

    mcap::Message msg;
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    WriteMsg(writer, channel.id, 0, 2, 1, data);

    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

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
}
#endif

TEST_CASE("Read Order", "[reader][writer]") {
  SECTION("Roundtrip two topics") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::None;
    opts.forceCompression = true;
    writer.open(buffer, opts);
    mcap::Schema schema1("schema1", "schemaEncoding", "ab");
    writer.addSchema(schema1);
    mcap::Channel channel1("topic1", "messageEncoding", schema1.id);
    writer.addChannel(channel1);
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    WriteMsg(writer, channel1.id, 0, 2, 1, data);

    mcap::Schema schema2("schema2", "schemaEncoding", "ab");
    writer.addSchema(schema2);
    mcap::Channel channel2("topic1", "messageEncoding", schema2.id);
    writer.addChannel(channel2);
    WriteMsg(writer, channel2.id, 1, 2, 1, data);

    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

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
  SECTION("Roundtrip unordered") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.chunkSize = 512 * 1024;
    opts.compression = mcap::Compression::None;
    opts.forceCompression = true;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);

    // Write larger-than-chunk-size messages.
    mcap::Message msg;
    std::vector<std::byte> data(1024 * 1024);
    std::fill(data.begin(), data.end(), std::byte(0x42));
    WriteMsg(writer, channel.id, 0, 0, 0, data);
    WriteMsg(writer, channel.id, 2, 2, 2, data);
    WriteMsg(writer, channel.id, 1, 1, 1, data);

    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

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
  SECTION("total ordering fallback to offset (chunked)") {
    Buffer buffer;

    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::None;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);

    mcap::Message msg;
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    WriteMsg(writer, channel.id, 0, 100, 100, data);
    WriteMsg(writer, channel.id, 1, 100, 100, data);
    WriteMsg(writer, channel.id, 2, 100, 100, data);
    WriteMsg(writer, channel.id, 3, 300, 300, data);
    WriteMsg(writer, channel.id, 4, 300, 300, data);
    WriteMsg(writer, channel.id, 5, 300, 300, data);
    WriteMsg(writer, channel.id, 6, 200, 200, data);
    writer.close();

    mcap::McapReader reader;
    requireOk(reader.open(buffer));

    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };

    mcap::ReadMessageOptions options;
    options.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
    size_t count = 0;
    const std::vector<uint32_t> forward_order_expected = {0, 1, 2, 6, 3, 4, 5};
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      REQUIRE(msgView.message.sequence == forward_order_expected[count]);
      count++;
    }
    REQUIRE(count == forward_order_expected.size());
    const std::vector<uint32_t> reverse_order_expected = {5, 4, 3, 6, 2, 1, 0};
    count = 0;
    options.readOrder = mcap::ReadMessageOptions::ReadOrder::ReverseLogTimeOrder;
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      REQUIRE(msgView.message.sequence == reverse_order_expected[count]);
      count++;
    }
    REQUIRE(count == reverse_order_expected.size());
  }
}

TEST_CASE("ReadJobQueue order", "[reader]") {
  SECTION("successive chunks with out-of-order timestamps") {
    mcap::internal::ReadJobQueue q(false);
    {
      mcap::internal::DecompressChunkJob chunk;
      chunk.messageStartTime = 100;
      chunk.messageEndTime = 200;
      chunk.chunkStartOffset = 1000;
      chunk.messageIndexEndOffset = 2000;
      q.push(std::move(chunk));
    }
    {
      mcap::internal::DecompressChunkJob chunk;
      chunk.messageStartTime = 0;
      chunk.messageEndTime = 100;
      chunk.chunkStartOffset = 2000;
      chunk.messageIndexEndOffset = 3000;
      q.push(std::move(chunk));
    }

    {
      auto result = q.pop();
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).messageStartTime == 0);
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).chunkStartOffset == 2000);
    }
    {
      auto result = q.pop();
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).messageStartTime == 100);
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).chunkStartOffset == 1000);
    }
  }
  SECTION("reverse time order: successive chunks with out-of-order timestamps") {
    mcap::internal::ReadJobQueue q(true);
    {
      mcap::internal::DecompressChunkJob chunk;
      chunk.messageStartTime = 100;
      chunk.messageEndTime = 200;
      chunk.chunkStartOffset = 1000;
      chunk.messageIndexEndOffset = 2000;
      q.push(std::move(chunk));
    }
    {
      mcap::internal::DecompressChunkJob chunk;
      chunk.messageStartTime = 0;
      chunk.messageEndTime = 100;
      chunk.chunkStartOffset = 2000;
      chunk.messageIndexEndOffset = 3000;
      q.push(std::move(chunk));
    }

    {
      auto result = q.pop();
      REQUIRE(std::holds_alternative<mcap::internal::DecompressChunkJob>(result));
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).messageStartTime == 100);
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).chunkStartOffset == 1000);
    }
    {
      auto result = q.pop();
      REQUIRE(std::holds_alternative<mcap::internal::DecompressChunkJob>(result));
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).messageStartTime == 0);
      REQUIRE(std::get<mcap::internal::DecompressChunkJob>(result).chunkStartOffset == 2000);
    }
  }
}

TEST_CASE("RecordOffset equality operators", "[reader]") {
  SECTION("non-equal records outside chunk") {
    mcap::RecordOffset a(10);
    mcap::RecordOffset b(20);

    REQUIRE(a != b);
    REQUIRE(b != a);

    REQUIRE(a < b);
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(!(b <= a));

    REQUIRE(!(a > b));
    REQUIRE(b > a);

    REQUIRE(!(a >= b));
    REQUIRE(b >= a);
  }

  SECTION("equal records outside chunk") {
    mcap::RecordOffset a(10);
    mcap::RecordOffset b(10);

    REQUIRE(a == b);
    REQUIRE(b == a);

    REQUIRE(!(a < b));
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(b <= a);

    REQUIRE(!(a > b));
    REQUIRE(!(b > a));

    REQUIRE(a >= b);
    REQUIRE(b >= a);
  }

  SECTION("non-equal records in same chunk") {
    mcap::RecordOffset a(10, 30);
    mcap::RecordOffset b(20, 30);

    REQUIRE(a != b);
    REQUIRE(b != a);

    REQUIRE(a < b);
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(!(b <= a));

    REQUIRE(!(a > b));
    REQUIRE(b > a);

    REQUIRE(!(a >= b));
    REQUIRE(b >= a);
  }

  SECTION("equal records inside chunk") {
    mcap::RecordOffset a(10, 30);
    mcap::RecordOffset b(10, 30);

    REQUIRE(a == b);
    REQUIRE(b == a);

    REQUIRE(!(a < b));
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(b <= a);

    REQUIRE(!(a > b));
    REQUIRE(!(b > a));

    REQUIRE(a >= b);
    REQUIRE(b >= a);
  }

  SECTION("non-equal records in same chunk") {
    mcap::RecordOffset a(10, 30);
    mcap::RecordOffset b(20, 30);

    REQUIRE(a != b);
    REQUIRE(b != a);

    REQUIRE(a < b);
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(!(b <= a));

    REQUIRE(!(a > b));
    REQUIRE(b > a);

    REQUIRE(!(a >= b));
    REQUIRE(b >= a);
  }

  SECTION("equally-offset records in different chunks") {
    mcap::RecordOffset a(10, 30);
    mcap::RecordOffset b(10, 40);

    REQUIRE(a != b);
    REQUIRE(b != a);

    REQUIRE(a < b);
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(!(b <= a));

    REQUIRE(!(a > b));
    REQUIRE(b > a);

    REQUIRE(!(a >= b));
    REQUIRE(b >= a);
  }

  SECTION("oppositely-offset records in different chunks") {
    mcap::RecordOffset a(20, 30);
    mcap::RecordOffset b(10, 40);

    REQUIRE(a != b);
    REQUIRE(b != a);

    REQUIRE(a < b);
    REQUIRE(!(b < a));

    REQUIRE(a <= b);
    REQUIRE(!(b <= a));

    REQUIRE(!(a > b));
    REQUIRE(b > a);

    REQUIRE(!(a >= b));
    REQUIRE(b >= a);
  }
}

TEST_CASE("parsing", "header") {
  Buffer buffer;
  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("my-profile");
  opts.library = "my-library";
  writer.open(buffer, opts);
  writer.close();

  mcap::McapReader reader;
  auto status = reader.open(buffer);
  requireOk(status);

  auto header = reader.header();
  REQUIRE(header != std::nullopt);

  REQUIRE(header->library == "my-library");
  REQUIRE(header->profile == "my-profile");
}

TEST_CASE("Schema isolation between files with noRepeatedSchemas=false", "[writer][reader]") {
  // First file with Schema1
  Buffer buffer1;
  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("test");
  opts.noRepeatedSchemas = false;
  opts.noRepeatedChannels = false;
  writer.open(buffer1, opts);

  mcap::Schema schema1("Schema1", "encoding1", "schema1_data");
  writer.addSchema(schema1);
  mcap::Channel channel1("topic1", "msg_encoding1", schema1.id);
  writer.addChannel(channel1);
  mcap::Schema schema2("Schema2", "encoding2", "schema2_data");
  writer.addSchema(schema2);
  mcap::Channel channel2("topic2", "msg_encoding2", schema2.id);
  writer.addChannel(channel2);

  {
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    WriteMsg(writer, channel1.id, 0, 1, 1, data);

    writer.close();
  }

  // Verify first file has all schemas and all channels, but only one message
  {
    mcap::McapReader reader;
    auto status = reader.open(buffer1);
    requireOk(status);

    status = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    requireOk(status);

    const auto maybeStats = reader.statistics();
    REQUIRE(maybeStats.has_value());
    const auto& stats = *maybeStats;
    REQUIRE(stats.messageCount == 1);
    REQUIRE(stats.schemaCount == 2);
    REQUIRE(stats.channelCount == 2);
    REQUIRE(stats.attachmentCount == 0);
    REQUIRE(stats.metadataCount == 0);
    REQUIRE(stats.channelMessageCounts.size() == 1);

    const auto& schemas = reader.schemas();
    REQUIRE(schemas.size() == 2);
    REQUIRE(std::find_if(schemas.begin(), schemas.end(), [](const auto& x) {
              return x.second->name == "Schema1";
            }) != schemas.end());
    REQUIRE(std::find_if(schemas.begin(), schemas.end(), [](const auto& x) {
              return x.second->name == "Schema2";
            }) != schemas.end());

    const auto& channels = reader.channels();
    REQUIRE(channels.size() == 2);
    REQUIRE(std::find_if(channels.begin(), channels.end(), [](const auto& x) {
              return x.second->topic == "topic1";
            }) != channels.end());
    REQUIRE(std::find_if(channels.begin(), channels.end(), [](const auto& x) {
              return x.second->topic == "topic2";
            }) != channels.end());

    reader.close();
  }

  // Second file with Schema2 - using same writer instance
  Buffer buffer2;
  {
    writer.open(buffer2, opts);

    std::vector<std::byte> data = {std::byte(4), std::byte(5), std::byte(6)};
    WriteMsg(writer, channel2.id, 0, 2, 2, data);

    writer.close();
  }

  // Verify second file has all schemas and all channels, but only one message
  {
    mcap::McapReader reader;
    auto status = reader.open(buffer2);
    requireOk(status);

    status = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    requireOk(status);

    const auto maybeStats = reader.statistics();
    REQUIRE(maybeStats.has_value());
    const auto& stats = *maybeStats;
    REQUIRE(stats.messageCount == 1);
    REQUIRE(stats.schemaCount == 2);
    REQUIRE(stats.channelCount == 2);
    REQUIRE(stats.attachmentCount == 0);
    REQUIRE(stats.metadataCount == 0);
    REQUIRE(stats.channelMessageCounts.size() == 1);

    const auto& schemas = reader.schemas();
    REQUIRE(schemas.size() == 2);
    REQUIRE(std::find_if(schemas.begin(), schemas.end(), [](const auto& x) {
              return x.second->name == "Schema1";
            }) != schemas.end());
    REQUIRE(std::find_if(schemas.begin(), schemas.end(), [](const auto& x) {
              return x.second->name == "Schema2";
            }) != schemas.end());

    const auto& channels = reader.channels();
    REQUIRE(channels.size() == 2);
    REQUIRE(std::find_if(channels.begin(), channels.end(), [](const auto& x) {
              return x.second->topic == "topic1";
            }) != channels.end());
    REQUIRE(std::find_if(channels.begin(), channels.end(), [](const auto& x) {
              return x.second->topic == "topic2";
            }) != channels.end());

    reader.close();
  }
}

TEST_CASE("FileReader works on files larger than 2GiB") {
  std::FILE* file = std::tmpfile();
#if defined _WIN32 || defined __CYGWIN__
  // Seeking past the end automatically makes sparse files on POSIX platforms if the filesystem
  // supports it, but Windows needs an ioctl. Without this, the test runs much slower.
  {
    HANDLE hFile = (HANDLE)_get_osfhandle(_fileno(file));
    REQUIRE(hFile != INVALID_HANDLE_VALUE);
    REQUIRE(DeviceIoControl(hFile, FSCTL_SET_SPARSE, nullptr, 0, nullptr, 0, nullptr, nullptr) ==
            TRUE);
  }
#endif
  // 2^30 + 2^30 = 2^31 > 2^31 - 1
  REQUIRE(std::fseek(file, 1L << 30L, SEEK_CUR) == 0);
  REQUIRE(std::fseek(file, 1L << 30L, SEEK_CUR) == 0);
  REQUIRE(std::fwrite("X", 1, 1, file) == 1);
  REQUIRE(std::ferror(file) == 0);
  std::rewind(file);
  auto reader = mcap::FileReader(file);
  REQUIRE(reader.size() == (1LL << 31LL) + 1);
  std::byte* output;
  REQUIRE(reader.read(&output, 1LL << 31LL, 1) == 1);
  REQUIRE((char)*output == 'X');
  REQUIRE(std::ferror(file) == 0);
  std::fclose(file);
}

TEST_CASE("Multiple empty channels and schemas are preserved", "[reader][writer]") {
  Buffer buffer;

  // Write
  {
    mcap::McapWriter writer;
    writer.open(buffer, mcap::McapWriterOptions("custom_profile"));

    mcap::Schema schema1("sensor_msgs/Imu", "ros2msg", "# IMU message definition");
    writer.addSchema(schema1);

    mcap::Schema schema2("geometry_msgs/Twist", "ros2msg", "# Twist message definition");
    writer.addSchema(schema2);

    mcap::Channel ch1("/imu/data", "cdr", schema1.id);
    writer.addChannel(ch1);

    mcap::Channel ch2("/cmd_vel", "cdr", schema2.id);
    writer.addChannel(ch2);

    // No messages written.
    writer.close();
  }

  // Read
  {
    mcap::McapReader reader;
    auto status = reader.open(buffer);
    REQUIRE(status.ok());

    status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
    REQUIRE(status.ok());

    const auto maybeStats = reader.statistics();
    REQUIRE(maybeStats.has_value());
    const auto& stats = *maybeStats;
    REQUIRE(stats.messageCount == 0);
    REQUIRE(stats.schemaCount == 2);
    REQUIRE(stats.channelCount == 2);
    REQUIRE(stats.attachmentCount == 0);
    REQUIRE(stats.metadataCount == 0);
    REQUIRE(stats.channelMessageCounts.size() == 0);

    // Verify schemas
    const auto& schemas = reader.schemas();
    REQUIRE(schemas.size() == 2);

    const auto& imu_schema = std::find_if(schemas.begin(), schemas.end(), [](const auto& x) {
      return x.second->name == "sensor_msgs/Imu";
    });
    REQUIRE(imu_schema != schemas.end());

    const auto& twist_schema = std::find_if(schemas.begin(), schemas.end(), [](const auto& x) {
      return x.second->name == "geometry_msgs/Twist";
    });
    REQUIRE(twist_schema != schemas.end());

    // Verify channels
    const auto& channels = reader.channels();
    REQUIRE(channels.size() == 2);

    const auto& imu_channel = std::find_if(channels.begin(), channels.end(), [](const auto& x) {
      return x.second->topic == "/imu/data";
    });
    REQUIRE(imu_channel != channels.end());

    const auto& cmd_vel_channel = std::find_if(channels.begin(), channels.end(), [](const auto& x) {
      return x.second->topic == "/cmd_vel";
    });
    REQUIRE(cmd_vel_channel != channels.end());

    reader.close();
  }
}
