#pragma once

#include "types.hpp"
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mcap {

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
 * @brief An abstract interface for reading MCAP data.
 */
struct IReadable {
  virtual ~IReadable() = default;

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
  virtual ~ICompressedReader() = default;

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
      static auto onProblem = [](const Status&) {};
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
    Message curMessage_;
    std::optional<MessageView> curMessageView_;

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

#ifdef MCAP_IMPLEMENTATION
#include "reader.inl"
#endif
