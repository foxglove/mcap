#define MCAP_IMPLEMENTATION
#include <mcap/mcap.hpp>

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include <array>
#include <cstdio>
#include <filesystem>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#if defined _WIN32 || defined __CYGWIN__
#  include <io.h>
#  include <ioapiset.h>
#  include <winioctl.h>
#endif
#if defined _MSC_VER
#  include <crtdbg.h>
#  include <stdlib.h>
#endif

int main(int argc, char* argv[]) {
#if defined _MSC_VER
  // In Debug builds the Microsoft CRT reports failed assertions (including the STL's own
  // "invalid comparator" checks) through a modal dialog, which hangs an unattended CI run until
  // the job times out. Send those reports to stderr and abort instead.
  _set_abort_behavior(0, _WRITE_ABORT_MSG);
  _set_error_mode(_OUT_TO_STDERR);
  _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);
  _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDERR);
#endif
  return Catch::Session().run(argc, argv);
}

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

// Writes a file without chunks or a summary, so the records sit directly in the file:
// magic, header, schema, channel, message, data end, footer, magic.
static void WriteFlatFile(Buffer& out) {
  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("test");
  opts.noChunking = true;
  opts.noSummary = true;
  writer.open(out, opts);
  mcap::Schema schema("schema", "schemaEncoding", "ab");
  writer.addSchema(schema);
  mcap::Channel channel("topic", "messageEncoding", schema.id);
  writer.addChannel(channel);
  std::vector<std::byte> data(100, std::byte(7));
  WriteMsg(writer, channel.id, 0, 2, 1, data);
  writer.close();
}

// Returns the offset of the first top-level record with the given opcode.
static uint64_t FindRecord(const Buffer& file, mcap::OpCode opcode) {
  uint64_t offset = sizeof(mcap::Magic);
  while (file.buffer[offset] != std::byte(opcode)) {
    offset += 9 + mcap::internal::ParseUint64(&file.buffer[offset + 1]);
  }
  return offset;
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

TEST_CASE("internal::crc32 bulk paths match byte-at-a-time reference", "[writer]") {
  // Byte-at-a-time reference using only the first 256 table entries, so it
  // exercises none of the 8-byte tabular or SIMD bulk code under test.
  const auto reference = [](uint32_t crc, const std::byte* data, size_t len) {
    for (size_t i = 0; i < len; i++) {
      crc = mcap::internal::CRC32_TABLE[(crc ^ uint8_t(data[i])) & 0xff] ^ (crc >> 8);
    }
    return crc;
  };

  // Deterministic pseudo-random fill.
  std::vector<std::byte> buf(8 + 4093);
  uint32_t seed = 12345;
  for (auto& b : buf) {
    seed = seed * 1664525u + 1013904223u;
    b = std::byte(seed >> 24);
  }

  // Lengths straddling the dispatch thresholds of crc32Update (<= 8
  // byte-at-a-time, < 64 8-byte tabular, >= 64 SIMD where available), at
  // several alignments since the SIMD path uses 16-byte loads.
  const size_t lengths[] = {0, 1, 7, 8, 9, 15, 16, 63, 64, 65, 79, 80, 127, 128, 129, 1024, 4093};
  for (size_t align = 0; align < 8; align++) {
    for (size_t length : lengths) {
      CAPTURE(align, length);
      const std::byte* data = buf.data() + align;
      const uint32_t expected =
        mcap::internal::crc32Final(reference(mcap::internal::CRC32_INIT, data, length));
      REQUIRE(mcap::internal::crc32Final(
                mcap::internal::crc32Update(mcap::internal::CRC32_INIT, data, length)) == expected);

      // Streaming across a split must match the one-shot result.
      const size_t split = length / 3;
      uint32_t crc = mcap::internal::CRC32_INIT;
      crc = mcap::internal::crc32Update(crc, data, split);
      crc = mcap::internal::crc32Update(crc, data + split, length - split);
      REQUIRE(mcap::internal::crc32Final(crc) == expected);
    }
  }

  // Known-answer test long enough for the bulk paths: eight copies of the
  // classic "123456789" input.
  std::array<std::byte, 72> kat;
  for (size_t i = 0; i < kat.size(); i++) {
    kat[i] = std::byte("123456789"[i % 9]);
  }
  REQUIRE(mcap::internal::crc32Final(mcap::internal::crc32Update(
            mcap::internal::CRC32_INIT, kat.data(), kat.size())) == 0x8811a440);
}

#if defined(__linux__)
TEST_CASE("FileWriter reports filesystem write errors", "[writer]") {
  mcap::FileWriter writer;
  requireOk(writer.open("/dev/full"));

  // Use a payload larger than the stdio buffer so fwrite reaches the device immediately.
  const std::vector<std::byte> data(1024 * 1024);
  try {
    writer.write(data.data(), data.size());
    FAIL("Expected writing to /dev/full to fail");
  } catch (const std::system_error& error) {
    REQUIRE(error.code() == std::errc::no_space_on_device);
  }
}
#endif

TEST_CASE("internal::Parse*()", "[reader]") {
  SECTION("uint64_t") {
    const std::array<std::byte, 8> input = {std::byte(0xef), std::byte(0xcd), std::byte(0xab),
                                            std::byte(0x90), std::byte(0x78), std::byte(0x56),
                                            std::byte(0x34), std::byte(0x12)};
    REQUIRE(mcap::internal::ParseUint64(input.data()) == 0x1234567890abcdefull);
  }
}

TEST_CASE("McapReader::ParseChunk()", "[reader]") {
  // Builds the body of a Chunk record (the bytes after opcode + record length).
  const auto makeUncompressedChunk = [](uint64_t uncompressedSize,
                                        const std::vector<std::byte>& records) {
    std::vector<std::byte> data;
    const auto putU32 = [&](uint32_t v) {
      for (int i = 0; i < 4; ++i) {
        data.push_back(std::byte((v >> (8 * i)) & 0xff));
      }
    };
    const auto putU64 = [&](uint64_t v) {
      for (int i = 0; i < 8; ++i) {
        data.push_back(std::byte((v >> (8 * i)) & 0xff));
      }
    };
    putU64(0);                         // messageStartTime
    putU64(0);                         // messageEndTime
    putU64(uncompressedSize);          // uncompressedSize
    putU32(0);                         // uncompressedCrc
    putU32(0);                         // compression string length (empty == uncompressed)
    putU64(uint64_t(records.size()));  // compressedSize
    data.insert(data.end(), records.begin(), records.end());
    return data;
  };
  const std::vector<std::byte> records = {std::byte(1), std::byte(2), std::byte(3)};

  SECTION("rejects an uncompressed chunk whose declared sizes disagree") {
    auto data = makeUncompressedChunk(999, records);
    mcap::Record record{mcap::OpCode::Chunk, data.size(), data.data()};
    mcap::Chunk chunk;
    const auto status = mcap::McapReader::ParseChunk(record, &chunk);
    REQUIRE(status.code == mcap::StatusCode::DecompressionSizeMismatch);
  }

  SECTION("accepts an uncompressed chunk whose declared sizes agree") {
    auto data = makeUncompressedChunk(records.size(), records);
    mcap::Record record{mcap::OpCode::Chunk, data.size(), data.data()};
    mcap::Chunk chunk;
    const auto status = mcap::McapReader::ParseChunk(record, &chunk);
    REQUIRE(status.ok());
    REQUIRE(chunk.uncompressedSize == records.size());
    REQUIRE(chunk.compressedSize == records.size());
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

    // Before the summary is read, the range runs to the end of the file, through the footer and
    // trailing magic, so that a sequential read checks them.
    auto [startOffset, endOffset] = reader.byteRange(0);
    REQUIRE(startOffset == 25);
    REQUIRE(endOffset == buffer.size());
    REQUIRE(endOffset == 353);

    auto [startOffset2, endOffset2] = reader.byteRange(0, 0);
    REQUIRE(startOffset2 == 25);
    REQUIRE(endOffset2 == 353);

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

TEST_CASE("LinearMessageView::status()", "[reader]") {
  // Writes an uncompressed file containing `messageCount` messages on a single channel.
  const auto writeFile = [](Buffer& buffer, size_t messageCount, bool noChunking) {
    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::None;
    opts.noChunking = noChunking;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);
    for (size_t i = 0; i < messageCount; ++i) {
      WriteMsg(writer, channel.id, uint32_t(i), i + 3, i + 3, std::vector<std::byte>(8));
    }
    writer.close();
  };

  // Overwrites the length of the first record in the data section with a value that runs past
  // the end of the file.
  const auto corruptFirstDataRecord = [](mcap::McapReader& reader, Buffer& buffer) {
    const auto [dataStart, dataEnd] = reader.byteRange(0);
    (void)dataEnd;
    for (size_t i = 1; i <= 8; ++i) {
      buffer.buffer[dataStart + i] = std::byte(0xff);
    }
  };

  const auto countMessages = [](mcap::LinearMessageView& view) {
    size_t messageCount = 0;
    for (const auto& msgView : view) {
      (void)msgView;
      ++messageCount;
    }
    return messageCount;
  };

  SECTION("Not open, no-callback overload") {
    mcap::McapReader reader;
    auto view = reader.readMessages();
    REQUIRE(view.status().code == mcap::StatusCode::NotOpen);
    REQUIRE(view.begin() == view.end());
  }

  SECTION("Not open, callback overload") {
    std::optional<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported = status;
    };
    mcap::McapReader reader;
    auto view = reader.readMessages(onProblem);
    REQUIRE(reported.has_value());
    REQUIRE(reported->code == mcap::StatusCode::NotOpen);
    REQUIRE(view.status().code == mcap::StatusCode::NotOpen);
    REQUIRE(view.begin() == view.end());
  }

  SECTION("Not open, options overload") {
    std::optional<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported = status;
    };
    mcap::McapReader reader;
    mcap::ReadMessageOptions options;
    options.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
    auto view = reader.readMessages(onProblem, options);
    REQUIRE(reported.has_value());
    REQUIRE(reported->code == mcap::StatusCode::NotOpen);
    REQUIRE(view.status().code == mcap::StatusCode::NotOpen);
    REQUIRE(view.begin() == view.end());
  }

  SECTION("Not open after a failed re-open") {
    Buffer good;
    writeFile(good, 1, false);
    mcap::McapReader reader;
    requireOk(reader.open(good));

    // Large enough to pass the size check, but with the wrong magic bytes.
    Buffer bad;
    bad.buffer.resize(64);
    REQUIRE(reader.open(bad).code == mcap::StatusCode::MagicMismatch);
    REQUIRE(reader.dataSource() == nullptr);

    std::optional<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported = status;
    };
    auto view = reader.readMessages(onProblem);
    REQUIRE(reported.has_value());
    REQUIRE(reported->code == mcap::StatusCode::NotOpen);
    REQUIRE(view.status().code == mcap::StatusCode::NotOpen);
    REQUIRE(view.begin() == view.end());
  }

  SECTION("Not open after a failed re-open by filename") {
    Buffer buffer;
    writeFile(buffer, 1, false);
    // Use a random suffix so concurrent test runs on one machine do not collide.
    const auto suffix = std::to_string(std::random_device{}());
    const auto tempDir = std::filesystem::temp_directory_path();
    const auto goodPath =
      (tempDir / ("mcap_linear_message_view_status_good_" + suffix + ".mcap")).string();
    const auto missingPath =
      (tempDir / ("mcap_linear_message_view_status_missing_" + suffix + ".mcap")).string();

    // Remove both files when the section ends, including when a REQUIRE fails.
    struct RemoveOnExit {
      std::vector<std::string> paths;
      ~RemoveOnExit() {
        for (const auto& path : paths) {
          std::error_code ec;
          std::filesystem::remove(path, ec);
        }
      }
    } cleanup{{goodPath, missingPath}};
    {
      std::FILE* file = std::fopen(goodPath.c_str(), "wb");
      REQUIRE(file != nullptr);
      REQUIRE(std::fwrite(buffer.buffer.data(), 1, buffer.buffer.size(), file) ==
              buffer.buffer.size());
      REQUIRE(std::fclose(file) == 0);
    }

    mcap::McapReader reader;
    requireOk(reader.open(goodPath));
    REQUIRE(reader.open(missingPath).code == mcap::StatusCode::OpenFailed);
    REQUIRE(reader.dataSource() == nullptr);

    std::optional<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported = status;
    };
    auto view = reader.readMessages(onProblem);
    REQUIRE(reported.has_value());
    REQUIRE(reported->code == mcap::StatusCode::NotOpen);
    REQUIRE(view.status().code == mcap::StatusCode::NotOpen);
    REQUIRE(view.begin() == view.end());

    reader.close();
  }

  SECTION("Successful read") {
    Buffer buffer;
    writeFile(buffer, 1, false);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    auto view = reader.readMessages();
    REQUIRE(view.status().ok());
    REQUIRE(countMessages(view) == 1);
    REQUIRE(view.status().ok());
  }

  SECTION("Crossed time range, no-op callback") {
    // Without a callback that records anything, status() is the only way to find out why the
    // view is empty.
    const auto onProblem = [](const mcap::Status&) {};
    Buffer buffer;
    writeFile(buffer, 1, false);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    mcap::ReadMessageOptions options;
    options.startingAt(100).endingBefore(50);
    auto view = reader.readMessages(onProblem, options);
    REQUIRE(view.status().code == mcap::StatusCode::InvalidMessageReadOptions);
    REQUIRE(view.begin() == view.end());
  }

  SECTION("Crossed time range, callback overload") {
    std::vector<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported.push_back(status);
    };
    Buffer buffer;
    writeFile(buffer, 1, false);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    mcap::ReadMessageOptions options;
    options.startingAt(100).endingBefore(50);
    auto view = reader.readMessages(onProblem, options);
    REQUIRE(reported.size() == 1);
    REQUIRE(reported[0].code == mcap::StatusCode::InvalidMessageReadOptions);
    REQUIRE(view.status().code == reported[0].code);
    REQUIRE(view.status().message == reported[0].message);
    REQUIRE(view.begin() == view.end());
  }

  SECTION("Corrupt record, no-callback overload") {
    Buffer buffer;
    writeFile(buffer, 1, false);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    corruptFirstDataRecord(reader, buffer);

    auto view = reader.readMessages();
    REQUIRE(view.status().ok());
    REQUIRE(countMessages(view) == 0);
    REQUIRE(view.status().code == mcap::StatusCode::InvalidRecord);
  }

  SECTION("Corrupt record, options overload") {
    Buffer buffer;
    writeFile(buffer, 1, false);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    corruptFirstDataRecord(reader, buffer);

    std::vector<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported.push_back(status);
    };
    // LogTimeOrder reads through the IndexedMessageReader rather than the TypedRecordReader.
    mcap::ReadMessageOptions options;
    options.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
    auto view = reader.readMessages(onProblem, options);
    REQUIRE(view.status().ok());
    REQUIRE(countMessages(view) == 0);
    REQUIRE(!reported.empty());
    REQUIRE(view.status().code == mcap::StatusCode::InvalidRecord);
    REQUIRE(view.status().code == reported[0].code);
    REQUIRE(view.status().message == reported[0].message);
  }

  SECTION("Missing channel, view keeps the first problem") {
    Buffer buffer;
    writeFile(buffer, 2, true);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));

    // Point both Message records at a channel that does not exist.
    const auto [dataStart, dataEnd] = reader.byteRange(0);
    mcap::RecordReader recordReader(buffer, dataStart, dataEnd);
    size_t corrupted = 0;
    while (const auto record = recordReader.next()) {
      if (record->opcode == mcap::OpCode::Message) {
        // channel_id is the first field of the Message record payload.
        const auto offset = recordReader.curRecordOffset();
        buffer.buffer[offset + 9] = std::byte(0x63);
        buffer.buffer[offset + 10] = std::byte(0x00);
        ++corrupted;
      }
    }
    REQUIRE(corrupted == 2);

    std::vector<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported.push_back(status);
    };
    auto view = reader.readMessages(onProblem);
    REQUIRE(countMessages(view) == 0);
    REQUIRE(reported.size() == 2);
    REQUIRE(reported[0].code == mcap::StatusCode::InvalidChannelId);
    REQUIRE(reported[1].code == mcap::StatusCode::InvalidChannelId);
    REQUIRE(reported[0].message != reported[1].message);
    REQUIRE(view.status().code == mcap::StatusCode::InvalidChannelId);
    REQUIRE(view.status().message == reported[0].message);
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
 * each message being large enough to meet the chunk size threshold and close its chunk.
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
  // Each message meets the chunk size threshold, closing its chunk once written.
  WriteMsg(writer, channel1.id, 0, 100, 100, std::vector<std::byte>(400));
  WriteMsg(writer, channel2.id, 0, 200, 200, std::vector<std::byte>(400));

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

TEST_CASE("explicit time range bounds", "[reader]") {
  Buffer buffer;

  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("test");
  opts.compression = mcap::Compression::None;
  writer.open(buffer, opts);
  mcap::Schema schema("schema", "schemaEncoding", "ab");
  writer.addSchema(schema);
  mcap::Channel channel("topic", "messageEncoding", schema.id);
  writer.addChannel(channel);
  for (uint64_t t = 1; t <= 6; t++) {
    WriteMsg(writer, channel.id, 0, t, t, std::vector<std::byte>(8));
  }
  writer.close();

  auto logTimes = [&buffer](const mcap::ReadMessageOptions& options) {
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };
    std::vector<mcap::Timestamp> times;
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      times.push_back(msgView.message.logTime);
    }
    return times;
  };

  {
    // Inclusive and exclusive bounds on both sides, via the chained setters.
    mcap::ReadMessageOptions options;
    options.startingAt(3).endingBefore(5);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, 4});
    options.endingAt(5);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, 4, 5});
    options.startingAfter(3);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{4, 5});
  }
  {
    // Each setter replaces any previously-set bound on its side.
    mcap::ReadMessageOptions options;
    options.startingAfter(5).endingAt(6);
    options.startingAt(3);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, 4, 5, 6});
    options.endingBefore(5);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, 4});
  }
  {
    // Equal inclusive bounds select the single matching log time.
    mcap::ReadMessageOptions options;
    options.startingAt(3).endingAt(3);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3});
  }
  {
    // endingAt(MaxTime) means no upper bound.
    mcap::ReadMessageOptions options;
    options.endingAt(mcap::MaxTime);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{1, 2, 3, 4, 5, 6});
  }
  {
    // The deprecated fields keep their exact historical behavior.
    MCAP_DIAGNOSTIC_PUSH
    MCAP_IGNORE_DEPRECATED
    mcap::ReadMessageOptions options;
    options.startTime = 3;
    options.endTime = 5;
    MCAP_DIAGNOSTIC_POP
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, 4});
  }
  {
    // The no-argument overloads read everything.
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    size_t count = 0;
    for (const auto& msgView : reader.readMessages()) {
      (void)msgView;
      count++;
    }
    REQUIRE(count == 6);
  }
  {
    // The options-only overload selects messages without a problem callback.
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    mcap::ReadMessageOptions options;
    options.startingAt(3).endingBefore(5);
    std::vector<mcap::Timestamp> times;
    for (const auto& msgView : reader.readMessages(options)) {
      times.push_back(msgView.message.logTime);
    }
    REQUIRE(times == std::vector<mcap::Timestamp>{3, 4});
  }
  {
    // The deprecated positional overloads, including the defaulted end time, are unchanged.
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };
    MCAP_DIAGNOSTIC_PUSH
    MCAP_IGNORE_DEPRECATED
    std::vector<mcap::Timestamp> times;
    for (const auto& msgView : reader.readMessages(onProblem, 3, 5)) {
      times.push_back(msgView.message.logTime);
    }
    REQUIRE(times == std::vector<mcap::Timestamp>{3, 4});
    times.clear();
    for (const auto& msgView : reader.readMessages(onProblem, 3)) {
      times.push_back(msgView.message.logTime);
    }
    REQUIRE(times == std::vector<mcap::Timestamp>{3, 4, 5, 6});
    MCAP_DIAGNOSTIC_POP
  }
  {
    // With a summary loaded, the linear path narrows its scan to the overlapping chunks.
    auto logTimesWithSummary = [&buffer](const mcap::ReadMessageOptions& options) {
      mcap::McapReader reader;
      requireOk(reader.open(buffer));
      requireOk(reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan));
      const auto onProblem = [](const mcap::Status& status) {
        FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
      };
      std::vector<mcap::Timestamp> times;
      for (const auto& msgView : reader.readMessages(onProblem, options)) {
        times.push_back(msgView.message.logTime);
      }
      return times;
    };
    mcap::ReadMessageOptions inside;
    inside.startingAfter(2).endingAt(4);
    REQUIRE(logTimesWithSummary(inside) == std::vector<mcap::Timestamp>{3, 4});
    mcap::ReadMessageOptions pastTheEnd;
    pastTheEnd.startingAfter(6);
    REQUIRE(logTimesWithSummary(pastTheEnd) == std::vector<mcap::Timestamp>{});
    mcap::ReadMessageOptions beforeTheStart;
    beforeTheStart.endingBefore(1);
    REQUIRE(logTimesWithSummary(beforeTheStart) == std::vector<mcap::Timestamp>{});
  }
  {
    // The membership predicates evaluate the bounds exactly as provided, on both sides.
    mcap::ReadMessageOptions options;
    options.startingAfter(3).endingAt(5);
    REQUIRE(!options.includesLogTime(3));
    REQUIRE(options.includesLogTime(4));
    REQUIRE(options.includesLogTime(5));
    REQUIRE(!options.includesLogTime(6));
    // A chunk overlaps the range if any log time in its closed span does.
    REQUIRE(!options.overlapsLogTimes(1, 3));
    REQUIRE(options.overlapsLogTimes(1, 4));
    REQUIRE(options.overlapsLogTimes(5, 9));
    REQUIRE(!options.overlapsLogTimes(6, 9));
    // The deprecated fields participate only where no explicit bound was set.
    MCAP_DIAGNOSTIC_PUSH
    MCAP_IGNORE_DEPRECATED
    mcap::ReadMessageOptions deprecated;
    deprecated.startTime = 3;
    deprecated.endTime = 5;
    REQUIRE(!deprecated.includesLogTime(2));
    REQUIRE(deprecated.includesLogTime(3));
    REQUIRE(deprecated.includesLogTime(4));
    REQUIRE(!deprecated.includesLogTime(5));
    deprecated.endingAt(5);
    REQUIRE(deprecated.includesLogTime(5));
    MCAP_DIAGNOSTIC_POP
  }
  {
    // A crossing range is rejected; an empty range is not.
    mcap::ReadMessageOptions options;
    options.startingAt(5).endingBefore(3);
    REQUIRE(!options.validate().ok());
    // readMessages() reports the rejection through the problem callback and yields nothing.
    std::optional<mcap::Status> reported;
    const auto onProblem = [&reported](const mcap::Status& status) {
      reported = status;
    };
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    auto view = reader.readMessages(onProblem, options);
    REQUIRE(reported.has_value());
    REQUIRE(reported->code == mcap::StatusCode::InvalidMessageReadOptions);
    REQUIRE(view.status().code == mcap::StatusCode::InvalidMessageReadOptions);
    REQUIRE(view.begin() == view.end());
    options.startingAt(5).endingBefore(5);
    requireOk(options.validate());
    options.startingAt(5).endingAt(4);
    requireOk(options.validate());
    options.startingAt(6).endingAt(4);
    REQUIRE(!options.validate().ok());
  }
}

TEST_CASE("maximum timestamp bound", "[reader]") {
  Buffer buffer;

  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("test");
  opts.compression = mcap::Compression::None;
  writer.open(buffer, opts);
  mcap::Schema schema("schema", "schemaEncoding", "ab");
  writer.addSchema(schema);
  mcap::Channel channel("topic", "messageEncoding", schema.id);
  writer.addChannel(channel);
  WriteMsg(writer, channel.id, 0, 3, 3, std::vector<std::byte>(8));
  WriteMsg(writer, channel.id, 1, mcap::MaxTime, mcap::MaxTime, std::vector<std::byte>(8));
  writer.close();

  auto logTimes = [&buffer](const mcap::ReadMessageOptions& options) {
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };
    std::vector<mcap::Timestamp> times;
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      times.push_back(msgView.message.logTime);
    }
    return times;
  };

  {
    // endingAt(MaxTime) includes the message logged at MaxTime.
    mcap::ReadMessageOptions options;
    options.endingAt(mcap::MaxTime);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, mcap::MaxTime});
  }
  {
    // The deprecated endTime default keeps its historical exclusive-MaxTime behavior.
    mcap::ReadMessageOptions options;
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3});
  }
  {
    // An explicit end bound, including endingAt(MaxTime), wins over the deprecated endTime.
    MCAP_DIAGNOSTIC_PUSH
    MCAP_IGNORE_DEPRECATED
    mcap::ReadMessageOptions options;
    options.endTime = 4;
    options.endingAt(mcap::MaxTime);
    MCAP_DIAGNOSTIC_POP
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3, mcap::MaxTime});
  }
  {
    // startingAfter(MaxTime) matches nothing and is not an error.
    mcap::ReadMessageOptions options;
    options.startingAfter(mcap::MaxTime);
    REQUIRE(!options.includesLogTime(mcap::MaxTime));
    REQUIRE(!options.overlapsLogTimes(0, mcap::MaxTime));
    requireOk(options.validate());
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{});
  }
  {
    // With no upper bound, or one that also reaches MaxTime, it is an empty query. An upper
    // bound that stops short of MaxTime makes the range crossed, as it does for startingAt.
    mcap::ReadMessageOptions options;
    options.startingAfter(mcap::MaxTime).endingAt(mcap::MaxTime);
    requireOk(options.validate());
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{});
    options.startingAfter(mcap::MaxTime).endingBefore(5);
    REQUIRE(!options.validate().ok());
    options.startingAfter(mcap::MaxTime).endingAt(5);
    REQUIRE(!options.validate().ok());
    options.startingAfter(mcap::MaxTime).endingBefore(mcap::MaxTime);
    REQUIRE(!options.validate().ok());
    options.startingAt(mcap::MaxTime).endingBefore(5);
    REQUIRE(!options.validate().ok());
  }
  {
    // The indexed read path yields nothing for the empty range too.
    mcap::ReadMessageOptions options;
    options.startingAfter(mcap::MaxTime);
    options.readOrder = mcap::ReadMessageOptions::ReadOrder::LogTimeOrder;
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{});
  }
  {
    // A later lower bound replaces the empty range; the endTime default still drops MaxTime.
    mcap::ReadMessageOptions options;
    options.startingAfter(mcap::MaxTime).startingAt(3);
    REQUIRE(options.includesLogTime(3));
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{3});
  }
  {
    // startingAfter below MaxTime keeps its normal exclusive behavior.
    mcap::ReadMessageOptions options;
    options.startingAfter(3).endingAt(mcap::MaxTime);
    REQUIRE(logTimes(options) == std::vector<mcap::Timestamp>{mcap::MaxTime});
  }
}

TEST_CASE("time range bounds with duplicate log times", "[reader]") {
  Buffer buffer;

  mcap::McapWriter writer;
  mcap::McapWriterOptions opts("test");
  opts.compression = mcap::Compression::None;
  // A tiny chunk size puts every message in its own chunk, so the duplicates span chunks.
  opts.chunkSize = 1;
  writer.open(buffer, opts);
  mcap::Schema schema("schema", "schemaEncoding", "ab");
  writer.addSchema(schema);
  mcap::Channel channel("topic", "messageEncoding", schema.id);
  writer.addChannel(channel);
  const std::vector<mcap::Timestamp> written{2, 3, 3, 3, 4};
  for (const auto t : written) {
    WriteMsg(writer, channel.id, 0, t, t, std::vector<std::byte>(8));
  }
  writer.close();

  {
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    requireOk(reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan));
    REQUIRE(reader.chunkIndexes().size() == written.size());
  }

  using ReadOrder = mcap::ReadMessageOptions::ReadOrder;
  auto logTimes = [&buffer](mcap::ReadMessageOptions options, ReadOrder order) {
    options.readOrder = order;
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    const auto onProblem = [](const mcap::Status& status) {
      FAIL("Status " + std::to_string((int)status.code) + ": " + status.message);
    };
    std::vector<mcap::Timestamp> times;
    for (const auto& msgView : reader.readMessages(onProblem, options)) {
      times.push_back(msgView.message.logTime);
    }
    return times;
  };

  for (const auto order :
       {ReadOrder::FileOrder, ReadOrder::LogTimeOrder, ReadOrder::ReverseLogTimeOrder}) {
    // Equal inclusive bounds keep every message at that log time, across chunks.
    mcap::ReadMessageOptions point;
    point.startingAt(3).endingAt(3);
    REQUIRE(logTimes(point, order) == std::vector<mcap::Timestamp>{3, 3, 3});
    // The exclusive spellings drop every one of them.
    mcap::ReadMessageOptions after;
    after.startingAfter(3);
    REQUIRE(logTimes(after, order) == std::vector<mcap::Timestamp>{4});
    mcap::ReadMessageOptions before;
    before.endingBefore(3);
    REQUIRE(logTimes(before, order) == std::vector<mcap::Timestamp>{2});
  }
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

  SECTION("unchunked record before a chunk") {
    mcap::RecordOffset a(20);
    mcap::RecordOffset b(10, 30);

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

  SECTION("unchunked record after a chunk") {
    mcap::RecordOffset a(40);
    mcap::RecordOffset b(10, 30);

    REQUIRE(a != b);
    REQUIRE(b != a);

    REQUIRE(!(a < b));
    REQUIRE(b < a);

    REQUIRE(!(a <= b));
    REQUIRE(b <= a);

    REQUIRE(a > b);
    REQUIRE(!(b > a));

    REQUIRE(a >= b);
    REQUIRE(!(b >= a));
  }

  SECTION("unchunked offset at the start of a chunk precedes the chunk's records") {
    // The reverse-order indexed reader keys a chunk's decompress job on the end of its message
    // indexes, which is the start offset of the next chunk. That plain offset must sort before
    // every record inside the next chunk, including one at chunk-relative offset 0.
    mcap::RecordOffset a(30);
    mcap::RecordOffset b(0, 30);
    mcap::RecordOffset c(10, 30);

    REQUIRE(a != b);
    REQUIRE(a != c);

    REQUIRE(a < b);
    REQUIRE(a < c);
    REQUIRE(!(b < a));
    REQUIRE(!(c < a));

    REQUIRE(a <= b);
    REQUIRE(!(b <= a));

    REQUIRE(!(a > b));
    REQUIRE(b > a);
    REQUIRE(c > a);

    REQUIRE(!(a >= b));
    REQUIRE(b >= a);
  }

  SECTION("operators describe a total order") {
    // Every pair of offsets, chunked or not, must be ordered by exactly one of <, ==, >, with the
    // remaining operators derived consistently. This is what std::push_heap and std::pop_heap
    // require of the indexed reader's job queue.
    const std::vector<mcap::RecordOffset> offsets = {
      mcap::RecordOffset(10),     mcap::RecordOffset(30),     mcap::RecordOffset(30),
      mcap::RecordOffset(40),     mcap::RecordOffset(0, 30),  mcap::RecordOffset(10, 30),
      mcap::RecordOffset(10, 30), mcap::RecordOffset(20, 30), mcap::RecordOffset(5, 40),
    };
    for (const auto& a : offsets) {
      for (const auto& b : offsets) {
        CAPTURE(a.offset, a.chunkOffset.value_or(0), a.chunkOffset.has_value());
        CAPTURE(b.offset, b.chunkOffset.value_or(0), b.chunkOffset.has_value());
        const int ordered = int(a < b) + int(a == b) + int(a > b);
        REQUIRE(ordered == 1);
        REQUIRE((a < b) == (b > a));
        REQUIRE((a == b) == (b == a));
        REQUIRE((a != b) == !(a == b));
        REQUIRE((a <= b) == !(a > b));
        REQUIRE((a >= b) == !(a < b));
        for (const auto& c : offsets) {
          if (a < b && b < c) {
            REQUIRE(a < c);
          }
        }
      }
    }
  }
}

TEST_CASE("parsing", "header") {
  SECTION("default library") {
    Buffer buffer;
    mcap::McapWriter writer;
    writer.open(buffer, mcap::McapWriterOptions("my-profile"));
    writer.close();

    mcap::McapReader reader;
    auto status = reader.open(buffer);
    requireOk(status);

    auto header = reader.header();
    REQUIRE(header != std::nullopt);

    REQUIRE(header->library == "mcap-cpp/" MCAP_LIBRARY_VERSION);
    REQUIRE(header->profile == "my-profile");
  }

  SECTION("custom library") {
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

TEST_CASE("Linear reads check the trailing magic", "[reader]") {
  Buffer buffer;
  {
    mcap::McapWriter writer;
    mcap::McapWriterOptions opts("test");
    opts.compression = mcap::Compression::None;
    writer.open(buffer, opts);
    mcap::Schema schema("schema", "schemaEncoding", "ab");
    writer.addSchema(schema);
    mcap::Channel channel("topic", "messageEncoding", schema.id);
    writer.addChannel(channel);
    std::vector<std::byte> data = {std::byte(1), std::byte(2), std::byte(3)};
    WriteMsg(writer, channel.id, 0, 2, 1, data);
    writer.close();
  }

  // Reads every message in file order, collecting the problems reported along the way.
  const auto readAll = [](Buffer& source, std::vector<mcap::Status>& problems) {
    mcap::McapReader reader;
    requireOk(reader.open(source));
    size_t count = 0;
    const auto onProblem = [&](const mcap::Status& status) {
      problems.push_back(status);
    };
    for (const auto& msgView : reader.readMessages(onProblem)) {
      (void)msgView;
      ++count;
    }
    return count;
  };

  SECTION("an intact file reports nothing") {
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(buffer, problems) == 1);
    REQUIRE(problems.empty());
  }

  SECTION("wrong trailing magic is reported after the last message") {
    buffer.buffer.back() = std::byte(0);
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(buffer, problems) == 1);
    REQUIRE(problems.size() == 1);
    REQUIRE(problems[0].code == mcap::StatusCode::MagicMismatch);
  }

  SECTION("a file cut off in its footer is reported once, after the last message") {
    buffer.buffer.resize(buffer.buffer.size() - 20);
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(buffer, problems) == 1);
    REQUIRE(problems.size() == 1);
    REQUIRE(problems[0].code == mcap::StatusCode::InvalidRecord);
  }

  SECTION("bytes after the trailing magic are reported after the last message") {
    buffer.buffer.push_back(std::byte(0));
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(buffer, problems) == 1);
    REQUIRE(problems.size() == 1);
    REQUIRE(problems[0].code == mcap::StatusCode::InvalidFile);
  }

  SECTION("a file with no data section and wrong trailing magic is reported") {
    Buffer headerOnly;
    {
      mcap::McapWriter writer;
      mcap::McapWriterOptions opts("test");
      opts.noSummary = true;
      writer.open(headerOnly, opts);
      writer.close();
    }
    // Drop the Data End record that follows the header, leaving the reader nothing to iterate.
    const auto headerEnd = sizeof(mcap::Magic) + 9 +
                           mcap::internal::ParseUint64(&headerOnly.buffer[sizeof(mcap::Magic) + 1]);
    REQUIRE(headerOnly.buffer[headerEnd] == std::byte(mcap::OpCode::DataEnd));
    headerOnly.buffer.erase(headerOnly.buffer.begin() + headerEnd,
                            headerOnly.buffer.begin() + headerEnd + 9 + 4);
    headerOnly.buffer.back() = std::byte(0);
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(headerOnly, problems) == 0);
    REQUIRE(problems.size() == 1);
    REQUIRE(problems[0].code == mcap::StatusCode::MagicMismatch);
  }

  SECTION("a read narrowed by the chunk index does not look at the end of the file") {
    // The summary is recovered by the fallback scan, so the caller has opted into a damaged file
    // and the read, like an indexed read, says nothing about its end.
    buffer.buffer.resize(buffer.buffer.size() - 20);
    mcap::McapReader reader;
    requireOk(reader.open(buffer));
    requireOk(reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan));
    std::vector<mcap::Status> problems;
    const auto onProblem = [&](const mcap::Status& status) {
      problems.push_back(status);
    };
    const auto countMessages = [&](mcap::Timestamp startTime, mcap::Timestamp endTime) {
      size_t count = 0;
      mcap::ReadMessageOptions options;
      options.startingAt(startTime).endingBefore(endTime);
      for (const auto& msgView : reader.readMessages(onProblem, options)) {
        (void)msgView;
        ++count;
      }
      return count;
    };
    // The only message is at log time 2.
    REQUIRE(countMessages(0, 10) == 1);
    REQUIRE(countMessages(10, 20) == 0);
    REQUIRE(problems.empty());
  }

  SECTION("a file cut off inside a record is reported once") {
    Buffer flat;
    WriteFlatFile(flat);
    flat.buffer.resize(FindRecord(flat, mcap::OpCode::Message) + 60);
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(flat, problems) == 0);
    REQUIRE(problems.size() == 1);
    REQUIRE(problems[0].code == mcap::StatusCode::InvalidRecord);
  }

  SECTION("a file cut off at a record boundary is reported after the last message") {
    Buffer flat;
    WriteFlatFile(flat);
    flat.buffer.resize(FindRecord(flat, mcap::OpCode::DataEnd));
    std::vector<mcap::Status> problems;
    REQUIRE(readAll(flat, problems) == 1);
    REQUIRE(problems.size() == 1);
    REQUIRE(problems[0].code == mcap::StatusCode::InvalidFile);
    REQUIRE(problems[0].message.find("without a Footer record") != std::string::npos);
  }
}

TEST_CASE("TypedRecordReader::endOfInput", "[reader]") {
  Buffer flat;
  WriteFlatFile(flat);

  // Reads every record in `source`, counting messages, and returns the final status.
  const auto readAll = [](Buffer& source, mcap::TypedRecordReader::EndOfInput endOfInput,
                          size_t& messages) {
    mcap::TypedRecordReader reader{source, 0};
    reader.endOfInput = endOfInput;
    reader.onMessage = [&](const mcap::Message&, mcap::ByteOffset,
                           std::optional<mcap::ByteOffset>) {
      ++messages;
    };
    while (reader.next()) {
      if (!reader.status().ok()) {
        break;
      }
    }
    return reader.status();
  };

  SECTION("a fragment of records") {
    // The records between the header and the Data End, with no Footer or magic after them.
    Buffer fragment;
    const auto begin = flat.buffer.begin();
    fragment.buffer.assign(begin + FindRecord(flat, mcap::OpCode::Schema),
                           begin + FindRecord(flat, mcap::OpCode::DataEnd));

    SECTION("is reported by default") {
      size_t messages = 0;
      const auto status = readAll(fragment, mcap::TypedRecordReader::EndOfInput::Default, messages);
      REQUIRE(messages == 1);
      REQUIRE(status.code == mcap::StatusCode::InvalidFile);
      REQUIRE(status.message.find("without a Footer record") != std::string::npos);
    }

    SECTION("is accepted with AnyRecordBoundary") {
      size_t messages = 0;
      const auto status =
        readAll(fragment, mcap::TypedRecordReader::EndOfInput::AnyRecordBoundary, messages);
      REQUIRE(messages == 1);
      REQUIRE(status.ok());
    }
  }

  SECTION("FooterAndMagic checks a range narrower than the input") {
    // Reads [8, end) with FooterAndMagic and returns the final status.
    const auto readRange = [&](uint64_t end) {
      mcap::TypedRecordReader reader{flat, sizeof(mcap::Magic), end};
      reader.endOfInput = mcap::TypedRecordReader::EndOfInput::FooterAndMagic;
      while (reader.next() && reader.status().ok()) {
      }
      return reader.status();
    };
    const uint64_t fileEnd = flat.buffer.size();
    flat.buffer.push_back(std::byte(0));  // a byte after the file, outside the range

    SECTION("a range ending with the magic is accepted") {
      REQUIRE(readRange(fileEnd).ok());
    }

    SECTION("a range ending inside the magic is reported") {
      const auto status = readRange(fileEnd - 1);
      REQUIRE(status.code == mcap::StatusCode::MagicMismatch);
      REQUIRE(status.message.find("7 bytes after the Footer record") != std::string::npos);
    }

    SECTION("a range continuing past the magic is reported") {
      const auto status = readRange(fileEnd + 1);
      REQUIRE(status.code == mcap::StatusCode::InvalidFile);
      REQUIRE(status.message.find("1 bytes after the trailing magic bytes") != std::string::npos);
    }
  }

  SECTION("a whole file with wrong trailing magic") {
    flat.buffer.back() = std::byte(0);
    // Reading from offset 8 skips the leading magic, as callers of this reader do.
    const auto readFile = [&](mcap::TypedRecordReader::EndOfInput endOfInput) {
      mcap::TypedRecordReader reader{flat, sizeof(mcap::Magic)};
      reader.endOfInput = endOfInput;
      while (reader.next()) {
        if (!reader.status().ok()) {
          break;
        }
      }
      return reader.status();
    };

    SECTION("is reported by default") {
      REQUIRE(readFile(mcap::TypedRecordReader::EndOfInput::Default).code ==
              mcap::StatusCode::MagicMismatch);
    }

    SECTION("is accepted with AnyRecordBoundary") {
      REQUIRE(readFile(mcap::TypedRecordReader::EndOfInput::AnyRecordBoundary).ok());
    }
  }
}
