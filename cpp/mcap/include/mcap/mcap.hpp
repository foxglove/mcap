#pragma once

#include "errors.hpp"
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mcap {

#define LIBRARY_VERSION "0.0.1"

constexpr char SpecVersion = '0';
constexpr char LibraryVersion[] = LIBRARY_VERSION;
constexpr uint8_t Magic[] = {137, 77, 67, 65, 80, SpecVersion, 13, 10};  // "\x89MCAP0\r\n"
constexpr uint64_t DefaultChunkSize = 1024 * 768;

using ChannelId = uint16_t;
using Timestamp = uint64_t;
using ByteOffset = uint64_t;
using KeyValueMap = std::unordered_map<std::string, std::string>;
using ByteArray = std::vector<uint8_t>;

enum struct Compression {
  None,
  Lz4,
  Zstd,
};

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
  const std::byte* data = nullptr;
};

struct Chunk {
  uint64_t uncompressedSize;
  uint32_t uncompressedCrc;
  std::string compression;
  uint64_t recordsSize;
  const std::byte* records = nullptr;
};

struct MessageIndex {
  mcap::ChannelId channelId;
  uint32_t count;
  std::vector<std::pair<mcap::Timestamp, mcap::ByteOffset>> records;
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
  const std::byte* data = nullptr;
};

struct AttachmentIndex {
  mcap::Timestamp recordTime;
  uint64_t attachmentSize;
  std::string name;
  std::string contentType;
  mcap::ByteOffset offset;

  AttachmentIndex(const Attachment& attachment, mcap::ByteOffset fileOffset)
      : recordTime(attachment.recordTime)
      , attachmentSize(attachment.dataSize)
      , name(attachment.name)
      , contentType(attachment.contentType)
      , offset(fileOffset) {}
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
  uint64_t dataSize;
  std::byte* data = nullptr;
};

struct McapWriterOptions {
  bool noChunking;
  bool noIndexing;
  uint64_t chunkSize;
  Compression compression;
  std::string profile;
  std::string library;
  mcap::KeyValueMap metadata;

  McapWriterOptions(const std::string_view profile)
      : noChunking(false)
      , noIndexing(false)
      , chunkSize(DefaultChunkSize)
      , compression(Compression::None)
      , profile(profile)
      , library("libmcap " LIBRARY_VERSION) {}
};

struct IWritable {
  virtual inline ~IWritable() = default;

  virtual void write(const std::byte* data, uint64_t size) = 0;
  virtual void end() = 0;
  virtual uint64_t size() const = 0;
};

struct IReadable {
  virtual inline ~IReadable() = default;

  virtual uint64_t size() const = 0;
  virtual uint64_t read(std::byte* output, uint64_t size) = 0;
};

/**
 * @brief An in-memory IWritable implementation backed by a growable buffer.
 */
class BufferedWriter final : public IWritable {
public:
  void write(const std::byte* data, uint64_t size) override;
  uint64_t size() const override;
  void end() override;
  const std::byte* data() const;

private:
  std::vector<std::byte> buffer_;
};

/**
 * @brief Implements the IWritable interface used by McapWriter by wrapping a
 * std::ostream stream.
 */
class StreamWriter final : public IWritable {
public:
  StreamWriter(std::ostream& stream);
  ~StreamWriter() override = default;

  void write(const std::byte* data, uint64_t size) override;
  void end() override;
  uint64_t size() const override;

private:
  std::ostream& stream_;
  uint64_t size_ = 0;
};

class McapWriter final {
public:
  ~McapWriter();

  /**
   * @brief Open a new MCAP file for writing and write the header.
   *
   * @param writer An implementation of the IWritable interface. Output bytes
   *   will be written to this object.
   * @param options Options for MCAP writing. `profile` is required.
   */
  void open(mcap::IWritable& writer, const McapWriterOptions& options);

  /**
   * @brief Open a new MCAP file for writing and write the header.
   *
   * @param stream Output stream to write to.
   * @param options Options for MCAP writing. `profile` is required.
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
  void addChannel(mcap::ChannelInfo& info);

  /**
   * @brief Write a message to the output stream.
   *
   * @param msg Message to add.
   * @return A non-zero error code on failure.
   */
  mcap::Status write(const mcap::Message& message);

  /**
   * @brief Write an attachment to the output stream.
   *
   * @param attachment Attachment to add.
   * @return A non-zero error code on failure.
   */
  mcap::Status write(const mcap::Attachment& attachment);

private:
  uint64_t chunkSize_ = DefaultChunkSize;
  mcap::IWritable* output_ = nullptr;
  std::unique_ptr<mcap::StreamWriter> streamOutput_;
  std::vector<mcap::ChannelInfo> channels_;
  std::vector<mcap::AttachmentIndex> attachmentIndex_;
  std::vector<mcap::ChunkIndex> chunkIndex_;
  Statistics statistics_{};
  mcap::BufferedWriter currentChunk_;
  std::unordered_map<mcap::ChannelId, mcap::MessageIndex> currentMessageIndex_;
  uint64_t currentChunkStart_ = std::numeric_limits<uint64_t>::max();
  uint64_t currentChunkEnd_ = std::numeric_limits<uint64_t>::min();
  bool indexing_ = true;

  void writeChunk(mcap::IWritable& output, const mcap::BufferedWriter& chunkData);

  static void writeMagic(mcap::IWritable& output);

  static void write(mcap::IWritable& output, const mcap::Header& header);
  static void write(mcap::IWritable& output, const mcap::Footer& footer);
  static void write(mcap::IWritable& output, const mcap::ChannelInfo& info);
  static void write(mcap::IWritable& output, const mcap::Message& message);
  static void write(mcap::IWritable& output, const mcap::Attachment& attachment);
  static void write(mcap::IWritable& output, const mcap::Chunk& chunk);
  static void write(mcap::IWritable& output, const mcap::MessageIndex& index);
  static void write(mcap::IWritable& output, const mcap::ChunkIndex& index);
  static void write(mcap::IWritable& output, const mcap::AttachmentIndex& index);
  static void write(mcap::IWritable& output, const mcap::Statistics& stats);
  static void write(mcap::IWritable& output, const mcap::UnknownRecord& record);

  static void write(mcap::IWritable& output, const std::string_view str);
  static void write(mcap::IWritable& output, OpCode value);
  static void write(mcap::IWritable& output, uint16_t value);
  static void write(mcap::IWritable& output, uint32_t value);
  static void write(mcap::IWritable& output, uint64_t value);
  static void write(mcap::IWritable& output, const std::byte* data, uint64_t size);
  static void write(mcap::IWritable& output, const KeyValueMap& map, uint32_t size = 0);
};

}  // namespace mcap

#include "mcap.inl"
