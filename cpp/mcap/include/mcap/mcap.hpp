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
using ProblemCallback = std::function<void(const Status&)>;

constexpr char SpecVersion = '0';
constexpr char LibraryVersion[] = LIBRARY_VERSION;
constexpr uint8_t Magic[] = {137, 77, 67, 65, 80, SpecVersion, 13, 10};  // "\x89MCAP0\r\n"
constexpr uint64_t DefaultChunkSize = 1024 * 768;
constexpr ByteOffset EndOffset = std::numeric_limits<ByteOffset>::max();
constexpr Timestamp MaxTime = std::numeric_limits<Timestamp>::max();

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
  Channel = 0x04,
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

constexpr std::string_view OpCodeString(OpCode opcode);

struct Record {
  OpCode opcode;
  uint64_t dataSize;
  std::byte* data;

  uint64_t recordSize() const {
    return sizeof(opcode) + sizeof(dataSize) + dataSize;
  }
};

struct Header {
  std::string profile;
  std::string library;
};

struct Footer {
  ByteOffset summaryStart;
  ByteOffset summaryOffsetStart;
  uint32_t summaryCrc;
};

struct Schema {
  SchemaId id;
  std::string name;
  std::string encoding;
  ByteArray data;

  Schema() = default;

  Schema(const std::string_view name, const std::string_view encoding, const std::string_view data)
      : name(name)
      , encoding(encoding)
      , data{reinterpret_cast<const std::byte*>(data.data()),
             reinterpret_cast<const std::byte*>(data.data() + data.size())} {}

  Schema(const std::string_view name, const std::string_view encoding, const ByteArray& data)
      : name(name)
      , encoding(encoding)
      , data{data} {}
};

struct Channel {
  ChannelId id;
  std::string topic;
  std::string messageEncoding;
  SchemaId schemaId;
  KeyValueMap metadata;

  Channel() = default;

  Channel(const std::string_view topic, const std::string_view messageEncoding, SchemaId schemaId,
          const KeyValueMap& metadata = {})
      : topic(topic)
      , messageEncoding(messageEncoding)
      , schemaId(schemaId)
      , metadata(metadata) {}
};

struct Message {
  ChannelId channelId;
  uint32_t sequence;
  Timestamp publishTime;
  Timestamp logTime;
  uint64_t dataSize;
  const std::byte* data = nullptr;
};

struct Chunk {
  Timestamp startTime;
  Timestamp endTime;
  ByteOffset uncompressedSize;
  uint32_t uncompressedCrc;
  std::string compression;
  ByteOffset compressedSize;
  const std::byte* records = nullptr;
};

struct MessageIndex {
  ChannelId channelId;
  std::vector<std::pair<Timestamp, ByteOffset>> records;
};

struct ChunkIndex {
  Timestamp startTime;
  Timestamp endTime;
  ByteOffset chunkStartOffset;
  ByteOffset chunkLength;
  std::unordered_map<ChannelId, ByteOffset> messageIndexOffsets;
  ByteOffset messageIndexLength;
  std::string compression;
  ByteOffset compressedSize;
  ByteOffset uncompressedSize;
};

struct Attachment {
  std::string name;
  Timestamp createdAt;
  Timestamp logTime;
  std::string contentType;
  uint64_t dataSize;
  const std::byte* data = nullptr;
  uint32_t crc;
};

struct AttachmentIndex {
  ByteOffset offset;
  ByteOffset length;
  Timestamp logTime;
  uint64_t dataSize;
  std::string name;
  std::string contentType;

  AttachmentIndex() = default;
  AttachmentIndex(const Attachment& attachment, ByteOffset fileOffset)
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
  uint16_t schemaCount;
  uint32_t channelCount;
  uint32_t attachmentCount;
  uint32_t metadataCount;
  uint32_t chunkCount;
  std::unordered_map<ChannelId, uint64_t> channelMessageCounts;
};

struct Metadata {
  std::string name;
  KeyValueMap metadata;
};

struct MetadataIndex {
  uint64_t offset;
  uint64_t length;
  std::string name;

  MetadataIndex() = default;
  MetadataIndex(const Metadata& metadata, ByteOffset fileOffset);
};

struct SummaryOffset {
  OpCode groupOpCode;
  ByteOffset groupStart;
  ByteOffset groupLength;
};

struct DataEnd {
  uint32_t dataSectionCrc;
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
  KeyValueMap metadata;

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

class ICompressedReader : public IReadable {
public:
  virtual inline ~ICompressedReader() = default;

  virtual void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) = 0;
  virtual Status status() const = 0;
};

class BufferReader final : public ICompressedReader {
public:
  void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;
  uint64_t size() const override;
  Status status() const override;

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

class LZ4Reader final : public ICompressedReader {
public:
  void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;
  uint64_t size() const override;
  Status status() const override;

private:
  Status status_;
  const std::byte* compressedData_;
  ByteArray uncompressedData_;
  uint64_t compressedSize_;
  uint64_t uncompressedSize_;
};

class ZStdReader final : public ICompressedReader {
public:
  void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) override;
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override;
  uint64_t size() const override;
  Status status() const override;

private:
  Status status_;
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

  Status open(IReadable& reader, const McapReaderOptions& options = {});
  Status open(std::ifstream& stream, const McapReaderOptions& options = {});

  void close();

  Status readSummary();

  LinearMessageView readMessages(Timestamp startTime = 0, Timestamp endTime = MaxTime);
  LinearMessageView readMessages(const ProblemCallback& onProblem, Timestamp startTime = 0,
                                 Timestamp endTime = MaxTime);

  IReadable* dataSource();
  const std::optional<Header>& header() const;
  const std::optional<Footer>& footer() const;

  static Status ReadRecord(IReadable& reader, uint64_t offset, Record* record);
  static Status ReadFooter(IReadable& reader, uint64_t offset, Footer* footer);

  static Status ParseHeader(const Record& record, Header* header);
  static Status ParseFooter(const Record& record, Footer* footer);
  static Status ParseSchema(const Record& record, Schema* schema);
  static Status ParseChannel(const Record& record, Channel* channel);
  static Status ParseMessage(const Record& record, Message* message);
  static Status ParseChunk(const Record& record, Chunk* chunk);
  static Status ParseMessageIndex(const Record& record, MessageIndex* messageIndex);
  static Status ParseChunkIndex(const Record& record, ChunkIndex* chunkIndex);
  static Status ParseAttachment(const Record& record, Attachment* attachment);
  static Status ParseAttachmentIndex(const Record& record, AttachmentIndex* attachmentIndex);
  static Status ParseStatistics(const Record& record, Statistics* statistics);
  static Status ParseMetadata(const Record& record, Metadata* metadata);
  static Status ParseMetadataIndex(const Record& record, MetadataIndex* metadataIndex);
  static Status ParseSummaryOffset(const Record& record, SummaryOffset* summaryOffset);
  static Status ParseDataEnd(const Record& record, DataEnd* dataEnd);

  static std::optional<Compression> ParseCompression(const std::string_view compression);

private:
  IReadable* input_ = nullptr;
  McapReaderOptions options_{};
  std::unique_ptr<FileStreamReader> fileStreamInput_;
  std::optional<Header> header_;
  std::optional<Footer> footer_;
  std::optional<Statistics> statistics_;
  std::vector<ChunkIndex> chunkIndexes_;
  std::vector<AttachmentIndex> attachmentIndexes_;
  std::unordered_map<ChannelId, Channel> channels_;
  // Used for uncompressed messages
  std::unordered_map<ChannelId, std::map<Timestamp, ByteOffset>> messageIndex_;
  // Used for messages inside compressed chunks
  std::unordered_map<ChannelId, std::map<Timestamp, ByteOffset>> messageChunkIndex_;
  ByteOffset dataStart_ = 0;
  ByteOffset dataEnd_ = EndOffset;
  Timestamp startTime_ = 0;
  Timestamp endTime_ = 0;
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
  void open(IWritable& writer, const McapWriterOptions& options);

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
  void addSchema(Schema& schema);

  /**
   * @brief Add a new channel to the MCAP file and set `channel.id` to a
   * generated channel id. The channel id is used when adding messages to the
   * file.
   *
   * @param channel Description of the channel to register. The `id` value is
   *   ignored and will be set to a generated channel id.
   */
  void addChannel(Channel& channel);

  /**
   * @brief Write a message to the output stream.
   *
   * @param msg Message to add.
   * @return A non-zero error code on failure.
   */
  Status write(const Message& message);

  /**
   * @brief Write an attachment to the output stream.
   *
   * @param attachment Attachment to add.
   * @return A non-zero error code on failure.
   */
  Status write(const Attachment& attachment);

  /**
   * @brief Write a metadata record to the output stream.
   *
   * @param metdata  Named group of key/value string pairs to add.
   * @return A non-zero error code on failure.
   */
  Status write(const Metadata& metdata);

  // The following static methods are used for serialization of records and
  // primitives to an output stream. They are not intended to be used directly
  // unless you are implementing a lower level writer or tests

  static void writeMagic(IWritable& output);

  static uint64_t write(IWritable& output, const Header& header);
  static uint64_t write(IWritable& output, const Footer& footer);
  static uint64_t write(IWritable& output, const Schema& schema);
  static uint64_t write(IWritable& output, const Channel& channel);
  static uint64_t write(IWritable& output, const Message& message);
  static uint64_t write(IWritable& output, const Attachment& attachment);
  static uint64_t write(IWritable& output, const Metadata& metadata);
  static uint64_t write(IWritable& output, const Chunk& chunk);
  static uint64_t write(IWritable& output, const MessageIndex& index);
  static uint64_t write(IWritable& output, const ChunkIndex& index);
  static uint64_t write(IWritable& output, const AttachmentIndex& index);
  static uint64_t write(IWritable& output, const MetadataIndex& index);
  static uint64_t write(IWritable& output, const Statistics& stats);
  static uint64_t write(IWritable& output, const SummaryOffset& summaryOffset);
  static uint64_t write(IWritable& output, const DataEnd& dataEnd);
  static uint64_t write(IWritable& output, const Record& record);

  static void write(IWritable& output, const std::string_view str);
  static void write(IWritable& output, const ByteArray bytes);
  static void write(IWritable& output, OpCode value);
  static void write(IWritable& output, uint16_t value);
  static void write(IWritable& output, uint32_t value);
  static void write(IWritable& output, uint64_t value);
  static void write(IWritable& output, const std::byte* data, uint64_t size);
  static void write(IWritable& output, const KeyValueMap& map, uint32_t size = 0);

private:
  uint64_t chunkSize_ = DefaultChunkSize;
  IWritable* output_ = nullptr;
  std::unique_ptr<StreamWriter> streamOutput_;
  std::unique_ptr<BufferWriter> uncompressedChunk_;
  std::unique_ptr<LZ4Writer> lz4Chunk_;
  std::unique_ptr<ZStdWriter> zstdChunk_;
  std::vector<Schema> schemas_;
  std::vector<Channel> channels_;
  std::vector<AttachmentIndex> attachmentIndex_;
  std::vector<MetadataIndex> metadataIndex_;
  std::vector<ChunkIndex> chunkIndex_;
  Statistics statistics_{};
  std::unordered_set<SchemaId> writtenSchemas_;
  std::unordered_map<ChannelId, MessageIndex> currentMessageIndex_;
  uint64_t currentChunkStart_ = std::numeric_limits<uint64_t>::max();
  uint64_t currentChunkEnd_ = std::numeric_limits<uint64_t>::min();
  Compression compression_ = Compression::None;
  uint64_t uncompressedSize_ = 0;
  bool indexing_ = true;
  bool opened_ = false;

  IWritable& getOutput();
  IChunkWriter* getChunkWriter();
  void writeChunk(IWritable& output, IChunkWriter& chunkData);
};

// RecordReader ////////////////////////////////////////////////////////////////

struct RecordReader {
  ByteOffset offset;
  ByteOffset endOffset;

  RecordReader(IReadable& dataSource, ByteOffset startOffset, ByteOffset endOffset = EndOffset);

  void reset(IReadable& dataSource, ByteOffset startOffset, ByteOffset endOffset);

  std::optional<Record> next();

  const Status& status();

private:
  IReadable* dataSource_ = nullptr;
  Status status_;
  Record curRecord_;
};

struct TypedChunkReader {
  std::function<void(const Schema&)> onSchema;
  std::function<void(const Channel&)> onChannel;
  std::function<void(const Message&)> onMessage;
  std::function<void(const Record&)> onUnknownRecord;

  TypedChunkReader();

  void reset(const Chunk& chunk, Compression compression);

  bool next();

  ByteOffset offset() const;

  const Status& status() const;

private:
  RecordReader reader_;
  Status status_;
  BufferReader uncompressedReader_;
  LZ4Reader lz4Reader_;
  ZStdReader zstdReader_;
};

struct TypedRecordReader {
  std::function<void(const Header&)> onHeader;
  std::function<void(const Footer&)> onFooter;
  std::function<void(const Schema&)> onSchema;
  std::function<void(const Channel&)> onChannel;
  std::function<void(const Message&)> onMessage;
  std::function<void(const Chunk&)> onChunk;
  std::function<void(const MessageIndex&)> onMessageIndex;
  std::function<void(const ChunkIndex&)> onChunkIndex;
  std::function<void(const Attachment&)> onAttachment;
  std::function<void(const AttachmentIndex&)> onAttachmentIndex;
  std::function<void(const Statistics&)> onStatistics;
  std::function<void(const Metadata&)> onMetadata;
  std::function<void(const MetadataIndex&)> onMetadataIndex;
  std::function<void(const SummaryOffset&)> onSummaryOffset;
  std::function<void(const DataEnd&)> onDataEnd;
  std::function<void(const Record&)> onUnknownRecord;
  std::function<void(void)> onChunkEnd;

  TypedRecordReader(IReadable& dataSource, ByteOffset startOffset,
                    ByteOffset endOffset = EndOffset);

  bool next();

  ByteOffset offset() const;

  const Status& status() const;

private:
  RecordReader reader_;
  TypedChunkReader chunkReader_;
  Status status_;
  bool parsingChunk_;
};

struct LinearMessageView {
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = int64_t;
    using value_type = Message;
    using pointer = const Message*;
    using reference = const Message&;

    reference operator*() const;
    pointer operator->() const;
    Iterator& operator++();
    Iterator operator++(int);
    friend bool operator==(const Iterator& a, const Iterator& b);
    friend bool operator!=(const Iterator& a, const Iterator& b);

    static const Iterator& end() {
      static auto onProblem = [](const Status& problem) {};
      static LinearMessageView::Iterator emptyIterator{onProblem};
      return emptyIterator;
    }

  private:
    friend LinearMessageView;

    std::optional<TypedRecordReader> reader_;
    Timestamp startTime_;
    Timestamp endTime_;
    const ProblemCallback& onProblem_;
    Message curMessage_;

    explicit Iterator(const ProblemCallback& onProblem);
    Iterator(IReadable& dataSource, ByteOffset dataStart, ByteOffset dataEnd, Timestamp startTime,
             Timestamp endTime, const ProblemCallback& onProblem);

    void readNext();
  };

  explicit LinearMessageView(const ProblemCallback& onProblem);
  LinearMessageView(IReadable* dataSource, ByteOffset dataStart, ByteOffset dataEnd,
                    Timestamp startTime, Timestamp endTime, const ProblemCallback& onProblem);

  Iterator begin();
  Iterator end();

private:
  IReadable* dataSource_;
  ByteOffset dataStart_;
  ByteOffset dataEnd_;
  Timestamp startTime_;
  Timestamp endTime_;
  const ProblemCallback onProblem_;
};

}  // namespace mcap

#include "mcap.inl"
