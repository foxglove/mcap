#define MCAP_IMPLEMENTATION
#include <mcap/mcap.hpp>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <array>
#include <numeric>

std::string_view StringView(const std::byte* data, size_t size) {
  return std::string_view{reinterpret_cast<const char*>(data), size};
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
