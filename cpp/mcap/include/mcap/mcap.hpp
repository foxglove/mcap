#pragma once

#include "errors.hpp"
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <lz4.h>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zstd_errors.h>

namespace mcap {

#define LIBRARY_VERSION "0.0.1"

using SchemaId = uint16_t;
using ChannelId = uint16_t;
using Timestamp = uint64_t;
using ByteOffset = uint64_t;
using KeyValueMap = std::unordered_map<std::string, std::string>;
using ByteArray = std::vector<std::byte>;

constexpr char SpecVersion = '0';
constexpr char LibraryVersion[] = LIBRARY_VERSION;
constexpr uint8_t Magic[] = {137, 77, 67, 65, 80, SpecVersion, 13, 10};  // "\x89MCAP0\r\n"
constexpr uint64_t DefaultChunkSize = 1024 * 768;
constexpr mcap::ByteOffset EndOffset = std::numeric_limits<mcap::ByteOffset>::max();

enum struct Compression {
  None,
  Lz4,
  Zstd,
};

enum struct CompressionLevel {
  Fastest,
  Fast,
  Default,
  Slow,
  Slowest,
};

enum struct OpCode : uint8_t {
  Header = 0x01,
  Footer = 0x02,
  Schema = 0x03,
  ChannelInfo = 0x04,
  Message = 0x05,
  Chunk = 0x06,
  MessageIndex = 0x07,
  ChunkIndex = 0x08,
  Attachment = 0x09,
  AttachmentIndex = 0x0A,
  Statistics = 0x0B,
  Metadata = 0x0C,
  MetadataIndex = 0x0D,
  SummaryOffset = 0x0E,
  DataEnd = 0x0F,
};

struct Record {
  OpCode opcode;
  uint64_t dataSize;
  std::byte* data;
};

struct Header {
  std::string profile;
  std::string library;
};

struct Footer {
  mcap::ByteOffset summaryStart;
  mcap::ByteOffset summaryOffsetStart;
  uint32_t summaryCrc;
};

struct Schema {
  mcap::SchemaId id;
  std::string name;
  std::string encoding;
  mcap::ByteArray data;

  Schema() = default;

  Schema(const std::string_view name, const std::string_view encoding, const std::string_view data)
      : name(name)
      , encoding(encoding)
      , data{reinterpret_cast<const std::byte*>(data.data()),
             reinterpret_cast<const std::byte*>(data.data() + data.size())} {}

  Schema(const std::string_view name, const std::string_view encoding, const mcap::ByteArray& data)
      : name(name)
      , encoding(encoding)
      , data{data} {}
};

struct ChannelInfo {
  mcap::ChannelId id;
  std::string topic;
  std::string messageEncoding;
  mcap::SchemaId schemaId;
  mcap::KeyValueMap metadata;

  ChannelInfo() = default;

  ChannelInfo(const std::string_view topic, const std::string_view messageEncoding,
              mcap::SchemaId schemaId, const KeyValueMap& metadata = {})
      : topic(topic)
      , messageEncoding(messageEncoding)
      , schemaId(schemaId)
      , metadata(metadata) {}
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
  uint64_t compressedSize;
  const std::byte* records = nullptr;
};

struct MessageIndex {
  mcap::ChannelId channelId;
  std::vector<std::pair<mcap::Timestamp, mcap::ByteOffset>> records;
};

struct ChunkIndex {
  mcap::Timestamp startTime;
  mcap::Timestamp endTime;
  mcap::ByteOffset chunkStartOffset;
  mcap::ByteOffset chunkLength;
  std::unordered_map<mcap::ChannelId, mcap::ByteOffset> messageIndexOffsets;
  mcap::ByteOffset messageIndexLength;
  std::string compression;
  mcap::ByteOffset compressedSize;
  mcap::ByteOffset uncompressedSize;
};

struct Attachment {
  std::string name;
  mcap::Timestamp createdAt;
  mcap::Timestamp logTime;
  std::string contentType;
  uint64_t dataSize;
  const std::byte* data = nullptr;
  uint32_t crc;
};

struct AttachmentIndex {
  mcap::ByteOffset offset;
  mcap::ByteOffset length;
  mcap::Timestamp logTime;
  uint64_t dataSize;
  std::string name;
  std::string contentType;

  AttachmentIndex() = default;
  AttachmentIndex(const Attachment& attachment, mcap::ByteOffset fileOffset)
      : offset(fileOffset)
      , length(4 + attachment.name.size() + 8 + 8 + 4 + attachment.contentType.size() + 8 +
               attachment.dataSize + 4)
      , logTime(attachment.logTime)
      , dataSize(attachment.dataSize)
      , name(attachment.name)
      , contentType(attachment.contentType) {}
};

struct Statistics {
  uint64_t messageCount;
  uint32_t channelCount;
  uint32_t attachmentCount;
  uint32_t metadataCount;
  uint32_t chunkCount;
  std::unordered_map<mcap::ChannelId, uint64_t> channelMessageCounts;
};

struct Metadata {
  std::string name;
  mcap::KeyValueMap metadata;
};

struct MetadataIndex {
  uint64_t offset;
  uint64_t length;
  std::string name;

  MetadataIndex() = default;
  MetadataIndex(const Metadata& metadata, mcap::ByteOffset fileOffset);
};

struct SummaryOffset {
  mcap::OpCode groupOpCode;
  mcap::ByteOffset groupStart;
  mcap::ByteOffset groupLength;
};

struct DataEnd {
  uint32_t dataSectionCrc;
};

struct UnknownRecord {
  uint8_t opcode;
  uint64_t dataSize;
  std::byte* data = nullptr;
};

struct McapReaderOptions {
  bool noSeeking;
  bool forceScan;
  bool allowFallbackScan;
};

struct McapWriterOptions {
  bool noChunking;
  bool noIndexing;
  uint64_t chunkSize;
  Compression compression;
  CompressionLevel compressionLevel;
  std::string profile;
  std::string library;
  mcap::KeyValueMap metadata;

  McapWriterOptions(const std::string_view profile)
      : noChunking(false)
      , noIndexing(false)
      , chunkSize(DefaultChunkSize)
      , compression(Compression::None)
      , compressionLevel(CompressionLevel::Default)
      , profile(profile)
      , library("libmcap " LIBRARY_VERSION) {}
};

struct IReadable {
  virtual inline ~IReadable() = default;

  virtual uint64_t size() const = 0;
  virtual uint64_t read(std::byte** output, uint64_t offset, uint64_t size) = 0;
};

class IChunkReader : public IReadable {
public:
  virtual inline ~IChunkReader() = default;

  virtual void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) = 0;
  virtual mcap::Status status() const = 0;
};

class BufferReader final : public IChunkReader {
public:
  void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;
  uint64_t size() const override;
  mcap::Status status() const override;

private:
  const std::byte* data_;
  uint64_t size_;
};

class FileStreamReader final : public IReadable {
public:
  FileStreamReader(std::ifstream& stream);

  uint64_t size() const override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;

private:
  std::ifstream& stream_;
  std::vector<std::byte> buffer_;
  uint64_t size_;
  uint64_t position_;
};

class LZ4Reader final : public IChunkReader {
public:
  void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;
  uint64_t size() const override;
  mcap::Status status() const override;

private:
  mcap::Status status_;
  const std::byte* compressedData_;
  ByteArray uncompressedData_;
  uint64_t compressedSize_;
  uint64_t uncompressedSize_;
};

class ZStdReader final : public IChunkReader {
public:
  void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;
  uint64_t size() const override;
  mcap::Status status() const override;

private:
  mcap::Status status_;
  const std::byte* compressedData_;
  ByteArray uncompressedData_;
  uint64_t compressedSize_;
  uint64_t uncompressedSize_;
};

struct IWritable {
  virtual inline ~IWritable() = default;

  virtual void write(const std::byte* data, uint64_t size) = 0;
  virtual void end() = 0;
  virtual uint64_t size() const = 0;
};

class IChunkWriter : public IWritable {
public:
  virtual inline ~IChunkWriter() = default;

  virtual void write(const std::byte* data, uint64_t size) = 0;
  virtual void end() = 0;
  virtual uint64_t size() const = 0;
  virtual bool empty() const = 0;
  virtual void clear() = 0;
  virtual const std::byte* data() const = 0;
};

/**
 * @brief An in-memory IChunkWriter implementation backed by a
 * growable buffer.
 */
class BufferWriter final : public IChunkWriter {
public:
  void write(const std::byte* data, uint64_t size) override;
  void end() override;
  uint64_t size() const override;
  bool empty() const override;
  void clear() override;
  const std::byte* data() const override;

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

  void write(const std::byte* data, uint64_t size) override;
  void end() override;
  uint64_t size() const override;

private:
  std::ostream& stream_;
  uint64_t size_ = 0;
};

/**
 * @brief An in-memory IChunkWriter implementation that holds data in a
 * temporary buffer before flushing to an LZ4-compressed buffer.
 */
class LZ4Writer final : public IChunkWriter {
public:
  LZ4Writer(CompressionLevel compressionLevel, uint64_t chunkSize);

  void write(const std::byte* data, uint64_t size) override;
  void end() override;
  uint64_t size() const override;
  bool empty() const override;
  void clear() override;
  const std::byte* data() const override;

private:
  std::vector<std::byte> preEndBuffer_;
  std::vector<std::byte> buffer_;
  int acceleration_ = 1;
};

/**
 * @brief An in-memory IChunkWriter implementation that holds data in a
 * temporary buffer before flushing to an ZStandard-compressed buffer.
 */
class ZStdWriter final : public IChunkWriter {
public:
  ZStdWriter(CompressionLevel compressionLevel, uint64_t chunkSize);
  ~ZStdWriter() override;

  void write(const std::byte* data, uint64_t size) override;
  void end() override;
  uint64_t size() const override;
  bool empty() const override;
  void clear() override;
  const std::byte* data() const override;

private:
  std::vector<std::byte> preEndBuffer_;
  std::vector<std::byte> buffer_;
  ZSTD_CCtx* zstdContext_ = nullptr;
};

struct LinearMessageView;

class McapReader final {
public:
  ~McapReader();

  mcap::Status open(mcap::IReadable& reader, const McapReaderOptions& options = {});
  mcap::Status open(std::ifstream& stream, const McapReaderOptions& options = {});

  LinearMessageView read() const;

  mcap::IReadable* dataSource() {
    return input_;
  }

  std::vector<mcap::Status>& problems() {
    return problems_;
  }

  void close();

  const std::optional<Header>& header() const {
    return header_;
  }

  const std::optional<Footer>& footer() const {
    return footer_;
  }

  static mcap::Status ReadRecord(mcap::IReadable& reader, uint64_t offset, mcap::Record* record);
  static mcap::Status ReadFooter(mcap::IReadable& reader, uint64_t offset, mcap::Footer* footer);

  static mcap::Status ParseHeader(const mcap::Record& record, mcap::Header* header);
  static mcap::Status ParseSchema(const mcap::Record& record, mcap::Schema* schema);
  static mcap::Status ParseChannelInfo(const mcap::Record& record, mcap::ChannelInfo* channelInfo);
  static mcap::Status ParseMessage(const mcap::Record& record, mcap::Message* message);
  static mcap::Status ParseChunk(const mcap::Record& record, mcap::Chunk* chunk);
  static mcap::Status ParseMessageIndex(const mcap::Record& record,
                                        mcap::MessageIndex* messageIndex);
  static mcap::Status ParseChunkIndex(const mcap::Record& record, mcap::ChunkIndex* chunkIndex);
  static mcap::Status ParseAttachment(const mcap::Record& record, mcap::Attachment* attachment);
  static mcap::Status ParseAttachmentIndex(const mcap::Record& record,
                                           mcap::AttachmentIndex* attachmentIndex);
  static mcap::Status ParseStatistics(const mcap::Record& record, mcap::Statistics* statistics);
  static mcap::Status ParseMetadata(const mcap::Record& record, mcap::Metadata* metadata);
  static mcap::Status ParseMetadataIndex(const mcap::Record& record,
                                         mcap::MetadataIndex* metadataIndex);
  static mcap::Status ParseSummaryOffset(const mcap::Record& record,
                                         mcap::SummaryOffset* summaryOffset);
  static mcap::Status ParseDataEnd(const mcap::Record& record, mcap::DataEnd* dataEnd);

  static std::optional<mcap::Compression> ParseCompression(const std::string_view compression);

private:
  mcap::IReadable* input_ = nullptr;
  McapReaderOptions options_{};
  std::unique_ptr<mcap::FileStreamReader> fileStreamInput_;
  std::vector<mcap::Status> problems_;
  std::optional<mcap::Header> header_;
  std::optional<mcap::Footer> footer_;
  std::optional<mcap::Statistics> statistics_;
  std::vector<mcap::ChunkIndex> chunkIndexes_;
  std::vector<mcap::AttachmentIndex> attachmentIndexes_;
  std::unordered_map<mcap::ChannelId, mcap::ChannelInfo> channelInfos_;
  // Used for uncompressed messages
  std::unordered_map<mcap::ChannelId, std::map<mcap::Timestamp, mcap::ByteOffset>> messageIndex_;
  // Used for messages inside compressed chunks
  std::unordered_map<mcap::ChannelId, std::map<mcap::Timestamp, mcap::ByteOffset>>
    messageChunkIndex_;
  uint64_t startTime_ = 0;
  uint64_t endTime_ = 0;
  bool parsedSummary_ = false;
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
   * @brief Write the MCAP footer, flush pending writes to the output stream,
   * and reset internal state.
   */
  void close();

  /**
   * @brief Reset internal state without writing the MCAP footer or flushing
   * pending writes. This should only be used in error cases as the output MCAP
   * file will be truncated.
   */
  void terminate();

  /**
   * @brief Add a new schema to the MCAP file and set `schema.id` to a generated
   * schema id. The schema id is used when adding channels to the file.
   *
   * @param schema Description of the schema to register. The `id` field is
   *   ignored and will be set to a generated schema id.
   */
  void addSchema(mcap::Schema& schema);

  /**
   * @brief Add a new channel to the MCAP file and set `channelInfo.id` to a
   * generated channel id. The channel id is used when adding messages to the
   * file.
   *
   * @param channelInfo Description of the channel to register. The `channelId`
   *   value is ignored and will be set to a generated channel id.
   */
  void addChannel(mcap::ChannelInfo& channelInfo);

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

  /**
   * @brief Write a metadata record to the output stream.
   *
   * @param metdata  Named group of key/value string pairs to add.
   * @return A non-zero error code on failure.
   */
  mcap::Status write(const mcap::Metadata& metdata);

  // The following static methods are used for serialization of records and
  // primitives to an output stream. They are not intended to be used directly
  // unless you are implementing a lower level writer or tests

  static void writeMagic(mcap::IWritable& output);

  static uint64_t write(mcap::IWritable& output, const mcap::Header& header);
  static uint64_t write(mcap::IWritable& output, const mcap::Footer& footer);
  static uint64_t write(mcap::IWritable& output, const mcap::Schema& schema);
  static uint64_t write(mcap::IWritable& output, const mcap::ChannelInfo& channelInfo);
  static uint64_t write(mcap::IWritable& output, const mcap::Message& message);
  static uint64_t write(mcap::IWritable& output, const mcap::Attachment& attachment);
  static uint64_t write(mcap::IWritable& output, const mcap::Metadata& metadata);
  static uint64_t write(mcap::IWritable& output, const mcap::Chunk& chunk);
  static uint64_t write(mcap::IWritable& output, const mcap::MessageIndex& index);
  static uint64_t write(mcap::IWritable& output, const mcap::ChunkIndex& index);
  static uint64_t write(mcap::IWritable& output, const mcap::AttachmentIndex& index);
  static uint64_t write(mcap::IWritable& output, const mcap::MetadataIndex& index);
  static uint64_t write(mcap::IWritable& output, const mcap::Statistics& stats);
  static uint64_t write(mcap::IWritable& output, const mcap::SummaryOffset& summaryOffset);
  static uint64_t write(mcap::IWritable& output, const mcap::DataEnd& dataEnd);
  static uint64_t write(mcap::IWritable& output, const mcap::UnknownRecord& record);

  static void write(mcap::IWritable& output, const std::string_view str);
  static void write(mcap::IWritable& output, const mcap::ByteArray bytes);
  static void write(mcap::IWritable& output, OpCode value);
  static void write(mcap::IWritable& output, uint16_t value);
  static void write(mcap::IWritable& output, uint32_t value);
  static void write(mcap::IWritable& output, uint64_t value);
  static void write(mcap::IWritable& output, const std::byte* data, uint64_t size);
  static void write(mcap::IWritable& output, const KeyValueMap& map, uint32_t size = 0);

private:
  uint64_t chunkSize_ = DefaultChunkSize;
  mcap::IWritable* output_ = nullptr;
  std::unique_ptr<mcap::StreamWriter> streamOutput_;
  std::unique_ptr<mcap::BufferWriter> uncompressedChunk_;
  std::unique_ptr<mcap::LZ4Writer> lz4Chunk_;
  std::unique_ptr<mcap::ZStdWriter> zstdChunk_;
  std::vector<mcap::Schema> schemas_;
  std::vector<mcap::ChannelInfo> channels_;
  std::vector<mcap::AttachmentIndex> attachmentIndex_;
  std::vector<mcap::MetadataIndex> metadataIndex_;
  std::vector<mcap::ChunkIndex> chunkIndex_;
  Statistics statistics_{};
  std::unordered_set<mcap::SchemaId> writtenSchemas_;
  std::unordered_map<mcap::ChannelId, mcap::MessageIndex> currentMessageIndex_;
  uint64_t currentChunkStart_ = std::numeric_limits<uint64_t>::max();
  uint64_t currentChunkEnd_ = std::numeric_limits<uint64_t>::min();
  Compression compression_ = Compression::None;
  uint64_t uncompressedSize_ = 0;
  bool indexing_ = true;
  bool opened_ = false;

  mcap::IWritable& getOutput();
  mcap::IChunkWriter* getChunkWriter();
  void writeChunk(mcap::IWritable& output, mcap::IChunkWriter& chunkData);
};

// RecordReader ////////////////////////////////////////////////////////////////

struct RecordReader {
  RecordReader(IReadable& dataSource, mcap::ByteOffset startOffset, mcap::ByteOffset endOffset);

  void reset(IReadable& dataSource, mcap::ByteOffset startOffset, mcap::ByteOffset endOffset);

  std::optional<mcap::Record> next();

  const mcap::Status& status();

private:
  IReadable& dataSource_;
  mcap::ByteOffset offset_;
  mcap::ByteOffset endOffset_;
  mcap::Status status_;
  mcap::Record curRecord_;
};

struct TypedChunkReader {
  std::function<void(const mcap::Schema&)> onSchema;
  std::function<void(const mcap::ChannelInfo&)> onChannelInfo;
  std::function<void(const mcap::Message&)> onMessage;

  TypedChunkReader();

  void reset(const Chunk& chunk, Compression compression);

  bool next();

  const mcap::Status& status();

private:
  RecordReader reader_;
  mcap::Status status_;
  BufferReader uncompressedReader_;
  LZ4Reader lz4Reader_;
  ZStdReader zstdReader_;
};

struct TypedRecordReader {
  std::function<void(const mcap::Schema&)> onSchema;
  std::function<void(const mcap::ChannelInfo&)> onChannelInfo;
  std::function<void(const mcap::Message&)> onMessage;
  std::function<void(const mcap::Chunk&)> onChunk;
  std::function<void(const mcap::MessageIndex&)> onMessageIndex;
  std::function<void(const mcap::ChunkIndex&)> onChunkIndex;
  std::function<void(const mcap::Attachment&)> onAttachment;
  std::function<void(const mcap::AttachmentIndex&)> onAttachmentIndex;
  std::function<void(const mcap::Statistics&)> onStatistics;
  std::function<void(const mcap::Metadata&)> onMetadata;
  std::function<void(const mcap::MetadataIndex&)> onMetadataIndex;
  std::function<void(const mcap::SummaryOffset&)> onSummaryOffset;
  std::function<void(const mcap::DataEnd&)> onDataEnd;

  TypedRecordReader(IReadable& dataSource, mcap::ByteOffset startOffset,
                    mcap::ByteOffset endOffset);

  bool next();

  const mcap::Status& status();

private:
  RecordReader reader_;
  TypedChunkReader chunkReader_;
  mcap::Status status_;
  bool parsingChunk_;
};

// Iterators ///////////////////////////////////////////////////////////////////

// struct LinearMessageView {
//   struct ForwardMessageIterator {
//     using iterator_category = std::forward_iterator_tag;
//     using difference_type = int64_t;
//     using value_type = Message;
//     using pointer = const Message*;
//     using reference = const Message&;

//     // FIXME: begin() needs to get as far as parsing the first message and caching it
//     // operator*() just returns the current cached parsed message, or an invalid message @ End
//     // operator++() advances offset_ and parses the next message

//     // begin() {
//     //   // parse records until encountering a message or chunk

//     //   // if a chunk is encountered, use the chunk iterator
//     // }

//     ForwardMessageIterator(LinearMessageView& view)
//         : view_(view)
//         , offset_(view.startOffset())
//         , curRecord_{}
//         , curMessage_{} {}

//     ForwardMessageIterator(LinearMessageView& view, uint64_t offset)
//         : view_(view)
//         , offset_(offset)
//         , curRecord_{}
//         , curMessage_{} {}

//     reference operator*() const {
//       return curMessage_;
//     }
//     pointer operator->() {
//       return &curMessage_;
//     }
//     ForwardMessageIterator& operator++() {
//       // Mark any offset past the end of the file as EndOffset
//       auto* dataSource = view_.reader().dataSource();
//       if (!dataSource || offset_ >= dataSource->size()) {
//         invalidate();
//         return *this;
//       }

//       // Read the current record if needed
//       if (!curRecord_.data) {
//         if (!readCurrentRecord(*dataSource)) {
//           return *this;
//         }
//       }

//       // Advance to the next record
//       offset_ += 9 + curRecord_.dataSize;

//       // Read the next record
//       readCurrentRecord(*dataSource);

//       return *this;
//     }
//     ForwardMessageIterator operator++(int) {
//       ForwardMessageIterator tmp = *this;
//       ++(*this);
//       return tmp;
//     }
//     friend bool operator==(const ForwardMessageIterator& a, const ForwardMessageIterator& b) {
//       return a.offset_ == b.offset_;
//     }
//     friend bool operator!=(const ForwardMessageIterator& a, const ForwardMessageIterator& b) {
//       return a.offset_ != b.offset_;
//     }

//   private:
//     LinearMessageView& view_;
//     mcap::ByteOffset offset_;
//     mcap::Record curRecord_;
//     mcap::Message curMessage_;

//     bool readCurrentRecord(mcap::IReadable& dataSource) {
//       if (auto status = McapReader::ReadRecord(dataSource, offset_, &curRecord_); !status.ok())
//       {
//         view_.reader().problems().push_back(status);
//         invalidate();
//         return false;
//       }
//       return true;
//     }

//     void invalidate() {
//       offset_ = EndOffset;
//       curRecord_ = {};
//       curMessage_ = {};
//     }
//   };

//   LinearMessageView(McapReader& reader, mcap::ByteOffset startOffset, mcap::Timestamp endTime)
//       : reader_(reader)
//       , startOffset_(startOffset)
//       , endTime_(endTime) {}

//   ForwardMessageIterator begin() {
//     return ForwardMessageIterator(*this, startOffset_);
//   }
//   ForwardMessageIterator end() {
//     return ForwardMessageIterator(*this, EndOffset);
//   }

//   mcap::McapReader& reader() {
//     return reader_;
//   }
//   mcap::ByteOffset startOffset() const {
//     return startOffset_;
//   }
//   mcap::Timestamp endTime() const {
//     return endTime_;
//   }

// private:
//   mcap::McapReader& reader_;
//   mcap::ByteOffset startOffset_;
//   mcap::Timestamp endTime_;
// };

}  // namespace mcap

#include "mcap.inl"
