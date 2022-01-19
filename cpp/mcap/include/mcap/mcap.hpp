#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace mcap {

constexpr char SpecVersionChar = '0';

using ChannelId = uint16_t;
using Timestamp = uint64_t;
using ByteOffset = uint64_t;
using KeyValueMap = std::unordered_map<std::string, std::string>;
using ByteArray = std::vector<uint8_t>;

enum struct OpCode: uint8_t {
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
};

struct Message {
  mcap::ChannelId channelId;
  uint32_t sequence;
  mcap::Timestamp publishTime;
  mcap::Timestamp recordTime;
  mcap::ByteArray data;
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
  mcap::ByteArray data;
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

} // namespace mcap
