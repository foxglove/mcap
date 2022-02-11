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

/**
 * @brief Supported MCAP compression algorithms.
 */
enum struct Compression {
  None,
  Lz4,
  Zstd,
};

/**
 * @brief Compression level to use when compression is enabled. Slower generally
 * produces smaller files, at the expense of more CPU time. These levels map to
 * different internal settings for each compression algorithm.
 */
enum struct CompressionLevel {
  Fastest,
  Fast,
  Default,
  Slow,
  Slowest,
};

/**
 * @brief MCAP record types.
 */
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

/**
 * @brief Get the string representation of an OpCode.
 */
constexpr std::string_view OpCodeString(OpCode opcode);

/**
 * @brief A generic Type-Length-Value record using a uint8 type and uint64
 * length. This is the generic form of all MCAP records.
 */
struct Record {
  OpCode opcode;
  uint64_t dataSize;
  std::byte* data;

  uint64_t recordSize() const {
    return sizeof(opcode) + sizeof(dataSize) + dataSize;
  }
};

/**
 * @brief Appears at the beginning of every MCAP file (after the magic byte
 * sequence) and contains the recording profile (see
 * <https://github.com/foxglove/mcap/tree/main/docs/specification/profiles>) and
 * a string signature of the recording library.
 */
struct Header {
  std::string profile;
  std::string library;
};

/**
 * @brief The final record in an MCAP file (before the trailing magic byte
 * sequence). Contains byte offsets from the start of the file to the Summary
 * and Summary Offset sections, along with an optional CRC of the combined
 * Summary and Summary Offset sections. A `summaryStart` and
 * `summaryOffsetStart` of zero indicates no Summary section is available.
 */
struct Footer {
  ByteOffset summaryStart;
  ByteOffset summaryOffsetStart;
  uint32_t summaryCrc;
};

/**
 * @brief Describes a schema used for message encoding and decoding and/or
 * describing the shape of messages. One or more Channel records map to a single
 * Schema.
 */
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

/**
 * @brief Describes a Channel that messages are written to. A Channel represents
 * a single connection from a publisher to a topic, so each topic will have one
 * Channel per publisher. Channels optionally reference a Schema, for message
 * encodings that are not self-describing (e.g. JSON) or when schema information
 * is available (e.g. JSONSchema).
 */
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

using SchemaPtr = std::shared_ptr<Schema>;
using ChannelPtr = std::shared_ptr<Channel>;

/**
 * @brief A single Message published to a Channel.
 */
struct Message {
  ChannelId channelId;
  /**
   * @brief An optional sequence number. If non-zero, sequence numbers should be
   * unique per channel and increasing over time.
   */
  uint32_t sequence;
  /**
   * @brief Nanosecond timestamp when this message was recorded or received for
   * recording.
   */
  Timestamp logTime;
  /**
   * @brief Nanosecond timestamp when this message was initially published. If
   * not available, this should be set to `logTime`.
   */
  Timestamp publishTime;
  /**
   * @brief Size of the message payload in bytes, pointed to via `data`.
   */
  uint64_t dataSize;
  /**
   * @brief A pointer to the message payload. For readers, this pointer is only
   * valid for the lifetime of an onMessage callback or before the message
   * iterator is advanced.
   */
  const std::byte* data = nullptr;
};

/**
 * @brief An collection of Schemas, Channels, and Messages that supports
 * compression and indexing.
 */
struct Chunk {
  Timestamp messageStartTime;
  Timestamp messageEndTime;
  ByteOffset uncompressedSize;
  uint32_t uncompressedCrc;
  std::string compression;
  ByteOffset compressedSize;
  const std::byte* records = nullptr;
};

/**
 * @brief A list of timestamps to byte offsets for a single Channel. This record
 * appears after each Chunk, one per Channel that appeared in that Chunk.
 */
struct MessageIndex {
  ChannelId channelId;
  std::vector<std::pair<Timestamp, ByteOffset>> records;
};

/**
 * @brief Chunk Index records are found in the Summary section, providing
 * summary information for a single Chunk and pointing to each Message Index
 * record associated with that Chunk.
 */
struct ChunkIndex {
  Timestamp messageStartTime;
  Timestamp messageEndTime;
  ByteOffset chunkStartOffset;
  ByteOffset chunkLength;
  std::unordered_map<ChannelId, ByteOffset> messageIndexOffsets;
  ByteOffset messageIndexLength;
  std::string compression;
  ByteOffset compressedSize;
  ByteOffset uncompressedSize;
};

/**
 * @brief An Attachment is an arbitrary file embedded in an MCAP file, including
 * a name, content-type, timestamps, and optional CRC. Attachment records are
 * written in the Data section, outside of Chunks.
 */
struct Attachment {
  std::string name;
  Timestamp createdAt;
  Timestamp logTime;
  std::string contentType;
  uint64_t dataSize;
  const std::byte* data = nullptr;
  uint32_t crc;
};

/**
 * @brief Attachment Index records are found in the Summary section, providing
 * summary information for a single Attachment.
 */
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

/**
 * @brief The Statistics record is found in the Summary section, providing
 * counts and timestamp ranges for the entire file.
 */
struct Statistics {
  uint64_t messageCount;
  uint16_t schemaCount;
  uint32_t channelCount;
  uint32_t attachmentCount;
  uint32_t metadataCount;
  uint32_t chunkCount;
  Timestamp messageStartTime;
  Timestamp messageEndTime;
  std::unordered_map<ChannelId, uint64_t> channelMessageCounts;
};

/**
 * @brief Holds a named map of key/value strings containing arbitrary user data.
 * Metadata records are found in the Data section, outside of Chunks.
 */
struct Metadata {
  std::string name;
  KeyValueMap metadata;
};

/**
 * @brief Metdata Index records are found in the Summary section, providing
 * summary information for a single Metadata record.
 */
struct MetadataIndex {
  uint64_t offset;
  uint64_t length;
  std::string name;

  MetadataIndex() = default;
  MetadataIndex(const Metadata& metadata, ByteOffset fileOffset);
};

/**
 * @brief Summary Offset records are found in the Summary Offset section.
 * Records in the Summary section are grouped together, and for each record type
 * found in the Summary section, a Summary Offset references the file offset and
 * length where that type of Summary record can be found.
 */
struct SummaryOffset {
  OpCode groupOpCode;
  ByteOffset groupStart;
  ByteOffset groupLength;
};

/**
 * @brief The final record in the Data section, signaling the end of Data and
 * beginning of Summary. Optionally contains a CRC of the entire Data section.
 */
struct DataEnd {
  uint32_t dataSectionCrc;
};

/**
 * @brief Returned when iterating over Messages in a file, MessageView contains
 * a reference to one Message, a pointer to its Channel, and an optional pointer
 * to that Channel's Schema. The Channel pointer is guaranteed to be valid,
 * while the Schema pointer may be null if the Channel references schema_id 0.
 */
struct MessageView {
  const Message& message;
  const ChannelPtr channel;
  const SchemaPtr schema;

  MessageView(const Message& message, const ChannelPtr channel, const SchemaPtr schema)
      : message(message)
      , channel(channel)
      , schema(schema) {}
};

/**
 * @brief Configuration options for McapReader.
 */
struct McapReaderOptions {
  /**
   * @brief Read the file sequentially from Header to DataEnd, skipping the
   * Summary section at the end. Seeking requires reading the file up to the
   * desired seek point.
   */
  bool forceScan;
  /**
   * @brief If the Summary section is missing or incomplete, allow falling back
   * to reading the file sequentially during summary operations or seeking. This
   * is equivalent to `forceScan = true` when the Summary cannot be read.
   */
  bool allowFallbackScan;
};

/**
 * @brief Configuration options for McapWriter.
 */
struct McapWriterOptions {
  /**
   * @brief Disable CRC calculations for Chunks, Attachments, and the Data and
   * Summary sections.
   */
  bool noCRC;
  /**
   * @brief Do not write Chunks to the file, instead writing Schema, Channel,
   * and Message records directly into the Data section.
   */
  bool noChunking;
  /**
   * @brief Do not write Summary or Summary Offset sections to the file, placing
   * the Footer record immediately after DataEnd. This can provide some speed
   * boost to file writing and produce smaller files, at the expense of
   * requiring a conversion process later if fast summarization or indexed
   * access is desired.
   */
  bool noSummary;
  /**
   * @brief Target uncompressed Chunk payload size in bytes. Once a Chunk's
   * uncompressed data meets or exceeds this size, the Chunk will be compressed
   * (if compression is enabled) and written to disk. Note that smaller Chunks
   * may be written, such as the last Chunk in the Data section. This option is
   * ignored if `noChunking=true`.
   */
  uint64_t chunkSize;
  /**
   * @brief Compression algorithm to use when writing Chunks. This option is
   * ignored if `noChunking=true`.
   */
  Compression compression;
  /**
   * @brief Compression level to use when writing Chunks. Slower generally
   * produces smaller files, at the expense of more CPU time. These levels map
   * to different internal settings for each compression algorithm.
   */
  CompressionLevel compressionLevel;
  /**
   * @brief The recording profile. See
   * <https://github.com/foxglove/mcap/tree/main/docs/specification/profiles>
   * for more information on well-known profiles.
   */
  std::string profile;
  /**
   * @brief A freeform string written by recording libraries. For this library,
   * the default is "libmcap {Major}.{Minor}.{Patch}".
   */
  std::string library;

  McapWriterOptions(const std::string_view profile)
      : noChunking(false)
      , noSummary(false)
      , chunkSize(DefaultChunkSize)
      , compression(Compression::None)
      , compressionLevel(CompressionLevel::Default)
      , profile(profile)
      , library("libmcap " LIBRARY_VERSION) {}
};

/**
 * @brief An abstract interface for reading MCAP data.
 */
struct IReadable {
  virtual inline ~IReadable() = default;

  /**
   * @brief Returns the size of the file in bytes.
   *
   * @return uint64_t The total number of bytes in the MCAP file.
   */
  virtual uint64_t size() const = 0;
  /**
   * @brief This method is called by MCAP reader classes when they need to read
   * a portion of the file.
   *
   * @param output A pointer to a pointer to the buffer to write to. This method
   *   is expected to either maintain an internal buffer, read data into it, and
   *   update this pointer to point at the internal buffer, or update this
   *   pointer to point directly at the source data if possible. The pointer and
   *   data must remain valid and unmodified until the next call to read().
   * @param offset The offset in bytes from the beginning of the file to read.
   * @param size The number of bytes to read.
   * @return uint64_t Number of bytes actually read. This may be less than the
   *   requested size if the end of the file is reached. The output pointer must
   *   be readable from `output` to `output + size`. If the read fails, this
   *   method should return 0.
   */
  virtual uint64_t read(std::byte** output, uint64_t offset, uint64_t size) = 0;
};

/**
 * @brief IReadable implementation wrapping a std::ifstream input file stream.
 */
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

/**
 * @brief An abstract interface for compressed readers.
 */
class ICompressedReader : public IReadable {
public:
  virtual inline ~ICompressedReader() = default;

  /**
   * @brief Reset the reader state, clearing any internal buffers and state, and
   * initialize with new compressed data.
   *
   * @param data Compressed data to read from.
   * @param size Size of the compressed data in bytes.
   * @param uncompressedSize Size of the data in bytes after decompression. A
   *   buffer of this size will be allocated for the uncompressed data.
   */
  virtual void reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) = 0;
  /**
   * @brief Report the current status of decompression. A StatusCode other than
   * `StatusCode::Success` after `reset()` is called indicates the decompression
   * was not successful and the reader is in an invalid state.
   */
  virtual Status status() const = 0;
};

/**
 * @brief A "null" compressed reader that directly passes through uncompressed
 * data. No internal buffers are allocated.
 */
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

/**
 * @brief ICompressedReader implementation that decompresses Zstandard
 * (https://facebook.github.io/zstd/) data.
 */
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

/**
 * @brief ICompressedReader implementation that decompresses LZ4
 * (https://lz4.github.io/lz4/) data.
 */
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

/**
 * @brief An abstract interface for writing MCAP data.
 */
struct IWritable {
  virtual inline ~IWritable() = default;

  /**
   * @brief Called whenever the writer needs to write data to the output MCAP
   * file.
   *
   * @param data A pointer to the data to write.
   * @param size Size of the data in bytes.
   */
  virtual void write(const std::byte* data, uint64_t size) = 0;
  /**
   * @brief Called when the writer is finished writing data to the output MCAP
   * file.
   */
  virtual void end() = 0;
  /**
   * @brief Returns the current size of the file in bytes. This must be equal to
   * the sum of all `size` parameters passed to `write()`.
   */
  virtual uint64_t size() const = 0;
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
 * @brief An abstract interface for writing Chunk data. Chunk data is buffered
 * in memory and written to disk as a single record, to support optimal
 * compression and calculating the final Chunk data size.
 */
class IChunkWriter : public IWritable {
public:
  virtual inline ~IChunkWriter() = default;

  /**
   * @brief Called whenever the writer needs to write data to the current output
   * Chunk.
   *
   * @param data A pointer to the data to write.
   * @param size Size of the data in bytes.
   */
  virtual void write(const std::byte* data, uint64_t size) = 0;
  /**
   * @brief Called when the writer wants to close the current output Chunk.
   * After this call, `data()` and `size()` should return the data and size of
   * the compressed data.
   */
  virtual void end() = 0;
  /**
   * @brief Returns the size in bytes of the compressed data. This will only be
   * called after `end()`.
   */
  virtual uint64_t size() const = 0;
  /**
   * @brief Returns true if `write()` has never been called since initialization
   * or the last call to `clear()`.
   */
  virtual bool empty() const = 0;
  /**
   * @brief Clear the internal state of the writer, discarding any input or
   * output buffers.
   */
  virtual void clear() = 0;
  /**
   * @brief Returns a pointer to the compressed data. This will only be called
   * after `end()`.
   */
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

/**
 * @brief Provides a read interface to an MCAP file.
 */
class McapReader final {
public:
  ~McapReader();

  /**
   * @brief Opens an MCAP file for reading.
   *
   * @param reader An implementation of the IReader interface that provides raw
   *   MCAP data.
   * @param options McapReader configuration options.
   * @return Status StatusCode::Success on success. If a non-success Status is
   *   returned, the data source is not considered open and McapReader is not
   *   usable until `open()` is called and a success response is returned.
   */
  Status open(IReadable& reader, const McapReaderOptions& options = {});
  /**
   * @brief Opens an MCAP file for reading from a std::ifstream input file
   * stream.
   *
   * @param stream Input file stream to read MCAP data from.
   * @param options McapReader configuration options.
   * @return Status StatusCode::Success on success. If a non-success Status is
   *   returned, the file is not considered open and McapReader is not usable
   *   until `open()` is called and a success response is returned.
   */
  Status open(std::ifstream& stream, const McapReaderOptions& options = {});

  /**
   * @brief Closes the MCAP file, clearing any internal data structures and
   * state and dropping the data source reference.
   *
   */
  void close();

  /**
   * @brief Read and parse the Summary section at the end of the MCAP file, if
   * available. This will populate internal indexes to allow for efficient
   * summarization and random access. This method will automatically be called
   * upon requesting summary data or first seek if Summary section parsing is
   * allowed by the configuration options.
   */
  Status readSummary();

  /**
   * @brief Returns an iterable view with `begin()` and `end()` methods for
   * iterating Messages in the MCAP file. If a non-zero `startTime` is provided,
   * this will first parse the Summary section (by calling `readSummary()`) if
   * allowed by the configuration options and it has not been parsed yet.
   *
   * @param startTime Optional start time in nanoseconds. Messages before this
   *   time will not be returned.
   * @param endTime Optional end time in nanoseconds. Messages equal to or after
   *   this time will not be returned.
   */
  LinearMessageView readMessages(Timestamp startTime = 0, Timestamp endTime = MaxTime);
  /**
   * @brief Returns an iterable view with `begin()` and `end()` methods for
   * iterating Messages in the MCAP file. If a non-zero `startTime` is provided,
   * this will first parse the Summary section (by calling `readSummary()`) if
   * allowed by the configuration options and it has not been parsed yet.
   *
   * @param onProblem A callback that will be called when a parsing error
   *   occurs. Problems can either be recoverable, indicating some data could
   *   not be read, or non-recoverable, stopping the iteration.
   * @param startTime Optional start time in nanoseconds. Messages before this
   *   time will not be returned.
   * @param endTime Optional end time in nanoseconds. Messages equal to or after
   *   this time will not be returned.
   */
  LinearMessageView readMessages(const ProblemCallback& onProblem, Timestamp startTime = 0,
                                 Timestamp endTime = MaxTime);

  /**
   * @brief Returns a pointer to the IReadable data source backing this reader.
   * Will return nullptr if the reader is not open.
   */
  IReadable* dataSource();

  /**
   * @brief Returns the parsed Header record, if it has been encountered.
   */
  const std::optional<Header>& header() const;
  /**
   * @brief Returns the parsed Footer record, if it has been encountered.
   */
  const std::optional<Footer>& footer() const;

  /**
   * @brief Look up a Channel record by channel ID. If the Channel has not been
   * encountered yet or does not exist in the file, this will return nullptr.
   *
   * @param channelId Channel ID to search for
   * @return ChannelPtr A shared pointer to a Channel record, or nullptr
   */
  ChannelPtr channel(ChannelId channelId) const;
  /**
   * @brief Look up a Schema record by schema ID. If the Schema has not been
   * encountered yet or does not exist in the file, this will return nullptr.
   *
   * @param schemaId Schema ID to search for
   * @return SchemaPtr A shared pointer to a Schema record, or nullptr
   */
  SchemaPtr schema(SchemaId schemaId) const;

  // The following static methods are used internally for parsing MCAP records
  // and do not need to be called directly unless you are implementing your own
  // reader functionality or tests.

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

  /**
   * @brief Converts a compression string ("", "zstd", "lz4") to the Compression enum.
   */
  static std::optional<Compression> ParseCompression(const std::string_view compression);

private:
  friend LinearMessageView;

  IReadable* input_ = nullptr;
  McapReaderOptions options_{};
  std::unique_ptr<FileStreamReader> fileStreamInput_;
  std::optional<Header> header_;
  std::optional<Footer> footer_;
  std::optional<Statistics> statistics_;
  std::vector<ChunkIndex> chunkIndexes_;
  std::vector<AttachmentIndex> attachmentIndexes_;
  std::unordered_map<SchemaId, SchemaPtr> schemas_;
  std::unordered_map<ChannelId, ChannelPtr> channels_;
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

/**
 * @brief Provides a write interface to an MCAP file.
 */
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
  bool writeSummary_ = true;
  bool opened_ = false;

  IWritable& getOutput();
  IChunkWriter* getChunkWriter();
  void writeChunk(IWritable& output, IChunkWriter& chunkData);
};

/**
 * @brief A low-level interface for parsing MCAP-style TLV records from a data
 * source.
 */
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
  std::function<void(const SchemaPtr)> onSchema;
  std::function<void(const ChannelPtr)> onChannel;
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

/**
 * @brief A mid-level interface for parsing and validating MCAP records from a
 * data source.
 */
struct TypedRecordReader {
  std::function<void(const Header&)> onHeader;
  std::function<void(const Footer&)> onFooter;
  std::function<void(const SchemaPtr)> onSchema;
  std::function<void(const ChannelPtr)> onChannel;
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

/**
 * @brief An iterable view of Messages in an MCAP file.
 */
struct LinearMessageView {
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = int64_t;
    using value_type = MessageView;
    using pointer = const MessageView*;
    using reference = const MessageView&;

    reference operator*() const;
    pointer operator->() const;
    Iterator& operator++();
    Iterator operator++(int);
    friend bool operator==(const Iterator& a, const Iterator& b);
    friend bool operator!=(const Iterator& a, const Iterator& b);

    static const Iterator& end() {
      static McapReader emptyReader;
      static auto onProblem = [](const Status& problem) {};
      static LinearMessageView::Iterator emptyIterator{emptyReader, onProblem};
      return emptyIterator;
    }

  private:
    friend LinearMessageView;

    McapReader& mcapReader_;
    std::optional<TypedRecordReader> recordReader_;
    Timestamp startTime_;
    Timestamp endTime_;
    const ProblemCallback& onProblem_;
    std::optional<MessageView> curMessage_;

    Iterator(McapReader& mcapReader, const ProblemCallback& onProblem);
    Iterator(McapReader& mcapReader, ByteOffset dataStart, ByteOffset dataEnd, Timestamp startTime,
             Timestamp endTime, const ProblemCallback& onProblem);

    void readNext();
  };

  LinearMessageView(McapReader& mcapReader, const ProblemCallback& onProblem);
  LinearMessageView(McapReader& mcapReader, ByteOffset dataStart, ByteOffset dataEnd,
                    Timestamp startTime, Timestamp endTime, const ProblemCallback& onProblem);

  Iterator begin();
  Iterator end();

private:
  McapReader& mcapReader_;
  ByteOffset dataStart_;
  ByteOffset dataEnd_;
  Timestamp startTime_;
  Timestamp endTime_;
  const ProblemCallback onProblem_;
};

}  // namespace mcap

#include "mcap.inl"
