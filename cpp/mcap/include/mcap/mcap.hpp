#pragma once

#include "errors.hpp"
#include <climits>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mcap {

#define LIBRARY_VERSION "0.0.1"

constexpr char SpecVersion = '0';
constexpr char LibraryVersion[] = LIBRARY_VERSION;
constexpr char Magic[] = {char(137), 77, 67, 65, 80, SpecVersion, 13, 10};  // "\x89MCAP0\r\n"

using ChannelId = uint16_t;
using Timestamp = uint64_t;
using ByteOffset = uint64_t;
using KeyValueMap = std::unordered_map<std::string, std::string>;
using ByteArray = std::vector<uint8_t>;

enum struct OpCode : uint8_t {
  Header = 0x01,
  Footer = 0x02,
  ChannelInfo = 0x03,
  Message = 0x04,
  Chunk = 0x05,
  MessageIndex = 0x06,
  ChunkIndex = 0x07,
  Attachment = 0x08,
  AttachmentIndex = 0x09,
  Statistics = 0x0A,
};

struct Header {
  std::string profile;
  std::string library;
  mcap::KeyValueMap metadata;
};

struct Footer {
  mcap::ByteOffset indexOffset;
  uint32_t indexCrc;
};

struct ChannelInfo {
  mcap::ChannelId channelId;
  std::string topicName;
  std::string encoding;
  std::string schemaName;
  std::string schema;
  mcap::KeyValueMap userData;

  ChannelInfo(const std::string_view topicName, const std::string_view encoding,
              const std::string_view schemaName, const std::string_view schema)
      : topicName(topicName)
      , encoding(encoding)
      , schemaName(schemaName)
      , schema(schema) {}
};

struct Message {
  mcap::ChannelId channelId;
  uint32_t sequence;
  mcap::Timestamp publishTime;
  mcap::Timestamp recordTime;
  uint64_t dataSize;
  uint8_t* data;
};

struct Chunk {
  uint64_t uncompressedSize;
  uint32_t uncompressedCrc;
  std::string compression;
  mcap::ByteArray records;
};

struct MessageIndex {
  mcap::ChannelId channelId;
  uint32_t count;
  std::unordered_map<mcap::Timestamp, mcap::ByteOffset> records;
};

struct ChunkIndex {
  mcap::Timestamp startTime;
  mcap::Timestamp endTime;
  mcap::ByteOffset chunkOffset;
  std::unordered_map<mcap::ChannelId, mcap::ByteOffset> messageIndexOffsets;
  uint64_t messageIndexLength;
  std::string compression;
  uint64_t compressedSize;
  uint64_t uncompressedSized;
  uint32_t crc;
};

struct Attachment {
  std::string name;
  mcap::Timestamp recordTime;
  std::string contentType;
  uint64_t dataSize;
  uint8_t* data;
};

struct AttachmentIndex {
  mcap::Timestamp recordTime;
  uint64_t attachmentSize;
  std::string name;
  std::string contentType;
  mcap::ByteOffset offset;
};

struct Statistics {
  uint64_t messageCount;
  uint32_t channelCount;
  uint32_t attachmentCount;
  uint32_t chunkCount;
  std::unordered_map<mcap::ChannelId, uint64_t> channelMessageCounts;
};

struct UnknownRecord {
  uint8_t opcode;
  mcap::ByteArray data;
};

struct McapWriterOptions {
  bool indexed;
  std::string profile;
  std::string library;
  mcap::KeyValueMap metadata;

  McapWriterOptions(const std::string_view profile)
      : indexed(false)
      , profile(profile)
      , library("libmcap " LIBRARY_VERSION) {}
};

class McapWriter {
public:
  ~McapWriter();

  /**
   * @brief Open a new MCAP file for writing and write the header.
   *
   * @param stream Output stream to write to.
   */
  void open(std::ostream& stream, const McapWriterOptions& options);

  /**
   * @brief Write the MCAP footer and close the output stream.
   */
  void close();

  /**
   * @brief Add channel info and set `info.channelId` to a generated channel id.
   * The channel id is used when adding messages.
   *
   * @param info Description of the channel to register. The `channelId` value
   *   is ignored and will be set to a generated channel id.
   */
  void registerChannel(mcap::ChannelInfo& info);

  /**
   * @brief Write a message to the output stream.
   *
   * @param msg Message to add.
   * @return An error code on failure.
   */
  std::optional<std::error_code> write(const mcap::Message& message);

  /**
   * @brief Write an attachment to the output stream.
   *
   * @param attachment Attachment to add.
   * @return An error code on failure.
   */
  std::optional<std::error_code> write(const mcap::Attachment& attachment);

private:
  std::ostream* stream_ = nullptr;
  std::vector<mcap::ChannelInfo> channels_;
  std::unordered_set<mcap::ChannelId> writtenChannels_;

  void writeMagic();

  void write(const mcap::Header& header);
  void write(const mcap::Footer& footer);
  void write(const mcap::ChannelInfo& info);
  void write(const std::string_view str);
  void write(OpCode value);
  void write(uint16_t value);
  void write(uint32_t value);
  void write(uint64_t value);
  void write(uint8_t* data, uint64_t size);
  void write(const KeyValueMap& map, uint32_t size = 0);
};

}  // namespace mcap

#include "mcap.inl"
