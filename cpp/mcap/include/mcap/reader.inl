#include "internal.hpp"
#include <cassert>
#include <lz4.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zstd_errors.h>

namespace mcap {

// BufferReader ////////////////////////////////////////////////////////////////

void BufferReader::reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) {
  (void)uncompressedSize;
  assert(size == uncompressedSize);
  data_ = data;
  size_ = size;
}

uint64_t BufferReader::read(std::byte** output, uint64_t offset, uint64_t size) {
  if (!data_ || offset >= size_) {
    return 0;
  }

  const auto available = size_ - offset;
  *output = const_cast<std::byte*>(data_) + offset;
  return std::min(size, available);
}

uint64_t BufferReader::size() const {
  return size_;
}

Status BufferReader::status() const {
  return StatusCode::Success;
}

// FileStreamReader ////////////////////////////////////////////////////////////

FileStreamReader::FileStreamReader(std::ifstream& stream)
    : stream_(stream) {
  assert(stream.is_open());

  // Determine the size of the file
  stream_.seekg(0, stream.end);
  size_ = stream_.tellg();
  stream_.seekg(0, stream.beg);

  position_ = 0;
}

uint64_t FileStreamReader::size() const {
  return size_;
}

uint64_t FileStreamReader::read(std::byte** output, uint64_t offset, uint64_t size) {
  if (offset >= size_) {
    return 0;
  }

  if (offset != position_) {
    stream_.seekg(offset);
    position_ = offset;
  }

  if (size > buffer_.size()) {
    buffer_.resize(size);
  }

  stream_.read(reinterpret_cast<char*>(buffer_.data()), size);
  *output = buffer_.data();

  const uint64_t bytesRead = stream_.gcount();
  position_ += bytesRead;
  return bytesRead;
}

// LZ4Reader ///////////////////////////////////////////////////////////////////

void LZ4Reader::reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) {
  status_ = StatusCode::Success;
  compressedData_ = data;
  compressedSize_ = size;
  uncompressedSize_ = uncompressedSize;

  // Allocate a buffer for the uncompressed data
  uncompressedData_.resize(uncompressedSize_);

  const auto status = LZ4_decompress_safe(reinterpret_cast<const char*>(compressedData_),
                                          reinterpret_cast<char*>(uncompressedData_.data()),
                                          compressedSize_, uncompressedSize_);
  if (uint64_t(status) != uncompressedSize_) {
    if (status < 0) {
      const auto msg = internal::StrFormat(
        "lz4 decompression of {} bytes into {} output bytes failed with error {}", compressedSize_,
        uncompressedSize_, status);
      status_ = Status{StatusCode::DecompressionFailed, msg};
    } else {
      const auto msg = internal::StrFormat(
        "lz4 decompression of {} bytes into {} output bytes only produced {} bytes",
        compressedSize_, uncompressedSize_, status);
      status_ = StatusCode::DecompressionSizeMismatch;
    }

    uncompressedSize_ = 0;
    uncompressedData_.clear();
  }
}

uint64_t LZ4Reader::read(std::byte** output, uint64_t offset, uint64_t size) {
  if (offset >= uncompressedSize_) {
    return 0;
  }

  const auto available = uncompressedSize_ - offset;
  *output = uncompressedData_.data() + offset;
  return std::min(size, available);
}

uint64_t LZ4Reader::size() const {
  return uncompressedSize_;
}

Status LZ4Reader::status() const {
  return status_;
}

// ZStdReader //////////////////////////////////////////////////////////////////

void ZStdReader::reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) {
  status_ = StatusCode::Success;
  compressedData_ = data;
  compressedSize_ = size;
  uncompressedSize_ = uncompressedSize;

  // Allocate a buffer for the uncompressed data
  uncompressedData_.resize(uncompressedSize_);

  const auto status =
    ZSTD_decompress(uncompressedData_.data(), uncompressedSize_, compressedData_, compressedSize_);
  if (status != uncompressedSize_) {
    if (ZSTD_isError(status)) {
      const auto msg = internal::StrFormat(
        "zstd decompression of {} bytes into {} output bytes failed with error {}", compressedSize_,
        uncompressedSize_, ZSTD_getErrorName(status));
      status_ = Status{StatusCode::DecompressionFailed, msg};
    } else {
      const auto msg = internal::StrFormat(
        "zstd decompression of {} bytes into {} output bytes only produced {} bytes",
        compressedSize_, uncompressedSize_, status);
      status_ = StatusCode::DecompressionSizeMismatch;
    }

    uncompressedSize_ = 0;
    uncompressedData_.clear();
  }
}

uint64_t ZStdReader::read(std::byte** output, uint64_t offset, uint64_t size) {
  if (offset >= uncompressedSize_) {
    return 0;
  }

  const auto available = uncompressedSize_ - offset;
  *output = uncompressedData_.data() + offset;
  return std::min(size, available);
}

uint64_t ZStdReader::size() const {
  return uncompressedSize_;
}

Status ZStdReader::status() const {
  return status_;
}

// McapReader //////////////////////////////////////////////////////////////////

McapReader::~McapReader() {
  close();
}

Status McapReader::open(IReadable& reader, const McapReaderOptions& options) {
  close();
  options_ = options;

  const uint64_t fileSize = reader.size();

  if (fileSize < internal::MinHeaderLength + internal::FooterLength) {
    return StatusCode::FileTooSmall;
  }

  std::byte* data = nullptr;
  uint64_t bytesRead;

  // Read the magic bytes and header up to the first variable length string
  bytesRead = reader.read(&data, 0, sizeof(Magic) + 1 + 8 + 4);
  if (bytesRead != sizeof(Magic) + 1 + 8 + 4) {
    return StatusCode::ReadFailed;
  }

  // Check the header magic bytes
  if (std::memcmp(data, Magic, sizeof(Magic)) != 0) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidMagic, "Header", internal::MagicToHex(data));
    return Status{StatusCode::MagicMismatch, msg};
  }

  // Read the Header record
  Record record;
  if (auto status = ReadRecord(reader, sizeof(Magic), &record); !status.ok()) {
    return status;
  }
  if (record.opcode != OpCode::Header) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidOpcode, "Header", uint8_t(record.opcode));
    return Status{StatusCode::InvalidFile, msg};
  }
  Header header;
  if (auto status = ParseHeader(record, &header); !status.ok()) {
    return status;
  }
  header_ = header;

  // The Data section starts after the magic bytes and Header record
  dataStart_ = sizeof(Magic) + record.recordSize();
  // Set dataEnd_ to just before the Footer for now. This will be updated when
  // the Data End record is encountered and/or the summary section is parsed
  dataEnd_ = fileSize - internal::FooterLength;

  input_ = &reader;

  return StatusCode::Success;
}

Status McapReader::open(std::ifstream& stream, const McapReaderOptions& options) {
  fileStreamInput_ = std::make_unique<FileStreamReader>(stream);
  return open(*fileStreamInput_, options);
}

void McapReader::close() {
  input_ = nullptr;
  fileStreamInput_.reset();
  header_ = std::nullopt;
  footer_ = std::nullopt;
  statistics_ = std::nullopt;
  chunkIndexes_.clear();
  attachmentIndexes_.clear();
  schemas_.clear();
  channels_.clear();
  messageIndex_.clear();
  messageChunkIndex_.clear();
  dataStart_ = 0;
  dataEnd_ = EndOffset;
  startTime_ = 0;
  endTime_ = 0;
  parsedSummary_ = false;
}

Status McapReader::readSummary() {
  if (!input_) {
    return StatusCode::NotOpen;
  }

  parsedSummary_ = true;

  auto& reader = *input_;
  const uint64_t fileSize = reader.size();

  // Read the footer
  auto footer = Footer{};
  if (auto status = ReadFooter(reader, fileSize - internal::FooterLength, &footer); !status.ok()) {
    return status;
  }
  footer_ = footer;

  return StatusCode::Success;
}

LinearMessageView McapReader::readMessages(Timestamp startTime, Timestamp endTime) {
  const auto onProblem = [](const Status&) {};
  return readMessages(onProblem, startTime, endTime);
}

LinearMessageView McapReader::readMessages(const ProblemCallback& onProblem, Timestamp startTime,
                                           Timestamp endTime) {
  // Check that open() has been successfully called
  if (!dataSource() || dataStart_ == 0) {
    onProblem(StatusCode::NotOpen);
    return LinearMessageView{*this, onProblem};
  }

  return LinearMessageView{*this, dataStart_, dataEnd_, startTime, endTime, onProblem};
}

IReadable* McapReader::dataSource() {
  return input_;
}

const std::optional<Header>& McapReader::header() const {
  return header_;
}

const std::optional<Footer>& McapReader::footer() const {
  return footer_;
}

ChannelPtr McapReader::channel(ChannelId channelId) const {
  const auto& maybeChannel = channels_.find(channelId);
  return (maybeChannel == channels_.end()) ? nullptr : maybeChannel->second;
}

SchemaPtr McapReader::schema(SchemaId schemaId) const {
  const auto& maybeSchema = schemas_.find(schemaId);
  return (maybeSchema == schemas_.end()) ? nullptr : maybeSchema->second;
}

Status McapReader::ReadRecord(IReadable& reader, uint64_t offset, Record* record) {
  // Check that we can read at least 9 bytes (opcode + length)
  auto maxSize = reader.size() - offset;
  if (maxSize < 9) {
    const auto msg =
      internal::StrFormat("cannot read record at offset {}, {} bytes remaining", offset, maxSize);
    return Status{StatusCode::InvalidFile, msg};
  }

  // Read opcode and length
  std::byte* data;
  uint64_t bytesRead = reader.read(&data, offset, 9);
  if (bytesRead != 9) {
    return StatusCode::ReadFailed;
  }

  // Parse opcode and length
  record->opcode = OpCode(data[0]);
  record->dataSize = internal::ParseUint64(data + 1);

  // Read payload
  maxSize -= 9;
  if (maxSize < record->dataSize) {
    const auto msg = internal::StrFormat(
      "record type 0x{:02x} at offset {} has length {} but only {} bytes remaining",
      uint8_t(record->opcode), offset, record->dataSize, maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  bytesRead = reader.read(&record->data, offset + 9, record->dataSize);
  if (bytesRead != record->dataSize) {
    const auto msg = internal::StrFormat(
      "attempted to read {} bytes for record type 0x{:02x} at offset {} but only read {} bytes",
      record->dataSize, uint8_t(record->opcode), offset, bytesRead);
    return Status{StatusCode::ReadFailed, msg};
  }

  return StatusCode::Success;
}

Status McapReader::ReadFooter(IReadable& reader, uint64_t offset, Footer* footer) {
  std::byte* data;
  uint64_t bytesRead = reader.read(&data, offset, internal::FooterLength);
  if (bytesRead != internal::FooterLength) {
    return StatusCode::ReadFailed;
  }

  // Check the footer magic bytes
  if (std::memcmp(data + internal::FooterLength - sizeof(Magic), Magic, sizeof(Magic)) != 0) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidMagic, "Footer",
                          internal::MagicToHex(data + internal::FooterLength - sizeof(Magic)));
    return Status{StatusCode::MagicMismatch, msg};
  }

  if (OpCode(data[0]) != OpCode::Footer) {
    const auto msg = internal::StrFormat(internal::ErrorMsgInvalidOpcode, "Footer", data[0]);
    return Status{StatusCode::InvalidFile, msg};
  }

  // Sanity check the record length. This is just an additional safeguard, since the footer has a
  // fixed length
  const uint64_t length = internal::ParseUint64(data + 1);
  if (length != 8 + 8 + 4) {
    const auto msg = internal::StrFormat(internal::ErrorMsgInvalidLength, "Footer", length);
    return Status{StatusCode::InvalidRecord, msg};
  }

  footer->summaryStart = internal::ParseUint64(data + 1 + 8);
  footer->summaryOffsetStart = internal::ParseUint64(data + 1 + 8 + 8);
  footer->summaryCrc = internal::ParseUint32(data + 1 + 8 + 8 + 8);
  return StatusCode::Success;
}

Status McapReader::ParseHeader(const Record& record, Header* header) {
  constexpr uint64_t MinSize = 4 + 4;

  assert(record.opcode == OpCode::Header);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Header", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  if (auto status = internal::ParseString(record.data, record.dataSize, &header->profile);
      !status.ok()) {
    return status;
  }
  const uint64_t maxSize = record.dataSize - 4 - header->profile.size();
  if (auto status = internal::ParseString(record.data, maxSize, &header->library); !status.ok()) {
    return status;
  }
  return StatusCode::Success;
}

Status McapReader::ParseFooter(const Record& record, Footer* footer) {
  constexpr uint64_t FooterSize = 8 + 8 + 4;

  assert(record.opcode == OpCode::Footer);
  if (record.dataSize != FooterSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Footer", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  footer->summaryStart = internal::ParseUint64(record.data);
  footer->summaryOffsetStart = internal::ParseUint64(record.data + 8);
  footer->summaryCrc = internal::ParseUint32(record.data + 8 + 8);

  return StatusCode::Success;
}

Status McapReader::ParseSchema(const Record& record, Schema* schema) {
  constexpr uint64_t MinSize = 2 + 4 + 4 + 4;

  assert(record.opcode == OpCode::Schema);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Schema", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  size_t offset = 0;

  // id
  schema->id = internal::ParseUint16(record.data);
  offset += 2;
  // name
  if (auto status =
        internal::ParseString(record.data + offset, record.dataSize - offset, &schema->name);
      !status.ok()) {
    return status;
  }
  offset += 4 + schema->name.size();
  // encoding
  if (auto status =
        internal::ParseString(record.data + offset, record.dataSize - offset, &schema->encoding);
      !status.ok()) {
    return status;
  }
  offset += 4 + schema->encoding.size();
  // data
  if (auto status =
        internal::ParseByteArray(record.data + offset, record.dataSize - offset, &schema->data);
      !status.ok()) {
    return status;
  }

  return StatusCode::Success;
}

Status McapReader::ParseChannel(const Record& record, Channel* channel) {
  constexpr uint64_t MinSize = 2 + 4 + 4 + 2 + 4;

  assert(record.opcode == OpCode::Channel);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Channel", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  size_t offset = 0;

  // id
  channel->id = internal::ParseUint16(record.data);
  offset += 2;
  // schema_id
  channel->schemaId = internal::ParseUint16(record.data + offset);
  offset += 2;
  // topic
  if (auto status =
        internal::ParseString(record.data + offset, record.dataSize - offset, &channel->topic);
      !status.ok()) {
    return status;
  }
  offset += 4 + channel->topic.size();
  // message_encoding
  if (auto status = internal::ParseString(record.data + offset, record.dataSize - offset,
                                          &channel->messageEncoding);
      !status.ok()) {
    return status;
  }
  offset += 4 + channel->messageEncoding.size();
  // metadata
  if (auto status = internal::ParseKeyValueMap(record.data + offset, record.dataSize - offset,
                                               &channel->metadata);
      !status.ok()) {
    return status;
  }
  return StatusCode::Success;
}

Status McapReader::ParseMessage(const Record& record, Message* message) {
  constexpr uint64_t MessagePreambleSize = 2 + 4 + 8 + 8;

  assert(record.opcode == OpCode::Message);
  if (record.dataSize < MessagePreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Message", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  message->channelId = internal::ParseUint16(record.data);
  message->sequence = internal::ParseUint32(record.data + 2);
  message->logTime = internal::ParseUint64(record.data + 2 + 4);
  message->publishTime = internal::ParseUint64(record.data + 2 + 4 + 8);
  message->dataSize = record.dataSize - MessagePreambleSize;
  message->data = record.data + MessagePreambleSize;
  return StatusCode::Success;
}

Status McapReader::ParseChunk(const Record& record, Chunk* chunk) {
  constexpr uint64_t ChunkPreambleSize = 8 + 8 + 8 + 4 + 4;

  assert(record.opcode == OpCode::Chunk);
  if (record.dataSize < ChunkPreambleSize) {
    const auto msg = internal::StrFormat(internal::ErrorMsgInvalidLength, "Chunk", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  chunk->messageStartTime = internal::ParseUint64(record.data);
  chunk->messageEndTime = internal::ParseUint64(record.data + 8);
  chunk->uncompressedSize = internal::ParseUint64(record.data + 8 + 8);
  chunk->uncompressedCrc = internal::ParseUint32(record.data + 8 + 8 + 8);

  size_t offset = 8 + 8 + 8 + 4;

  // compression
  if (auto status =
        internal::ParseString(record.data + offset, record.dataSize - offset, &chunk->compression);
      !status.ok()) {
    return status;
  }
  offset += 4 + chunk->compression.size();
  // compressed_size
  if (auto status = internal::ParseUint64(record.data + offset, record.dataSize - offset,
                                          &chunk->compressedSize);
      !status.ok()) {
    return status;
  }
  offset += 8;
  if (chunk->compressedSize > record.dataSize - offset) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Chunk.records", chunk->compressedSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  // records
  chunk->records = record.data + offset;

  return StatusCode::Success;
}

Status McapReader::ParseMessageIndex(const Record& record, MessageIndex* messageIndex) {
  constexpr uint64_t PreambleSize = 2 + 4;

  assert(record.opcode == OpCode::MessageIndex);
  if (record.dataSize < PreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "MessageIndex", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  messageIndex->channelId = internal::ParseUint16(record.data);
  const uint32_t recordsSize = internal::ParseUint32(record.data + 2);

  if (recordsSize % 16 != 0 || recordsSize > record.dataSize - PreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "MessageIndex.records", recordsSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  const size_t recordsCount = size_t(recordsSize / 16);
  messageIndex->records.reserve(recordsCount);
  for (size_t i = 0; i < recordsCount; ++i) {
    const auto timestamp = internal::ParseUint64(record.data + PreambleSize + i * 16);
    const auto offset = internal::ParseUint64(record.data + PreambleSize + i * 16 + 8);
    messageIndex->records.emplace_back(timestamp, offset);
  }
  return StatusCode::Success;
}

Status McapReader::ParseChunkIndex(const Record& record, ChunkIndex* chunkIndex) {
  constexpr uint64_t PreambleSize = 8 + 8 + 8 + 8 + 4;

  assert(record.opcode == OpCode::ChunkIndex);
  if (record.dataSize < PreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "ChunkIndex", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  chunkIndex->messageStartTime = internal::ParseUint64(record.data);
  chunkIndex->messageEndTime = internal::ParseUint64(record.data + 8);
  chunkIndex->chunkStartOffset = internal::ParseUint64(record.data + 8 + 8);
  chunkIndex->chunkLength = internal::ParseUint64(record.data + 8 + 8 + 8);
  const uint32_t messageIndexOffsetsSize = internal::ParseUint32(record.data + 8 + 8 + 8 + 8);

  if (messageIndexOffsetsSize % 10 != 0 ||
      messageIndexOffsetsSize > record.dataSize - PreambleSize) {
    const auto msg = internal::StrFormat(
      internal::ErrorMsgInvalidLength, "ChunkIndex.message_index_offsets", messageIndexOffsetsSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  const size_t messageIndexOffsetsCount = size_t(messageIndexOffsetsSize / 10);
  chunkIndex->messageIndexOffsets.reserve(messageIndexOffsetsCount);
  for (size_t i = 0; i < messageIndexOffsetsCount; ++i) {
    const auto channelId = internal::ParseUint16(record.data + PreambleSize + i * 10);
    const auto offset = internal::ParseUint64(record.data + PreambleSize + i * 10 + 2);
    chunkIndex->messageIndexOffsets.emplace(channelId, offset);
  }

  uint64_t offset = PreambleSize + messageIndexOffsetsSize;
  // message_index_length
  if (auto status = internal::ParseUint64(record.data + offset, record.dataSize - offset,
                                          &chunkIndex->messageIndexLength);
      !status.ok()) {
    return status;
  }
  offset += 8;
  // compression
  if (auto status = internal::ParseString(record.data + offset, record.dataSize - offset,
                                          &chunkIndex->compression);
      !status.ok()) {
    return status;
  }
  offset += 4 + chunkIndex->compression.size();
  // compressed_size
  if (auto status = internal::ParseUint64(record.data + offset, record.dataSize - offset,
                                          &chunkIndex->compressedSize);
      !status.ok()) {
    return status;
  }
  offset += 8;
  // uncompressed_size
  if (auto status = internal::ParseUint64(record.data + offset, record.dataSize - offset,
                                          &chunkIndex->uncompressedSize);
      !status.ok()) {
    return status;
  }

  return StatusCode::Success;
}

Status McapReader::ParseAttachment(const Record& record, Attachment* attachment) {
  constexpr uint64_t MinSize = /* log_time */ 8 +
                               /* create_time */ 8 +
                               /* name */ 4 +
                               /* content_type */ 4 +
                               /* data_size */ 8 +
                               /* crc */ 4;

  assert(record.opcode == OpCode::Attachment);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Attachment", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  uint32_t offset = 0;
  // log_time
  if (auto status =
        internal::ParseUint64(record.data + offset, record.dataSize - offset, &attachment->logTime);
      !status.ok()) {
    return status;
  }
  offset += 8;
  // create_time
  if (auto status = internal::ParseUint64(record.data + offset, record.dataSize - offset,
                                          &attachment->createTime);
      !status.ok()) {
    return status;
  }
  offset += 8;
  // name
  if (auto status =
        internal::ParseString(record.data + offset, record.dataSize - offset, &attachment->name);
      !status.ok()) {
    return status;
  }
  offset += 4 + attachment->name.size();
  // content_type
  if (auto status = internal::ParseString(record.data + offset, record.dataSize - offset,
                                          &attachment->contentType);
      !status.ok()) {
    return status;
  }
  offset += 4 + attachment->contentType.size();
  // data_size
  if (auto status = internal::ParseUint64(record.data + offset, record.dataSize - offset,
                                          &attachment->dataSize);
      !status.ok()) {
    return status;
  }
  offset += 8;
  // data
  if (attachment->dataSize > record.dataSize - offset) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Attachment.data", attachment->dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  attachment->data = record.data + offset;
  offset += attachment->dataSize;
  // crc
  if (auto status =
        internal::ParseUint32(record.data + offset, record.dataSize - offset, &attachment->crc);
      !status.ok()) {
    return status;
  }

  return StatusCode::Success;
}

Status McapReader::ParseAttachmentIndex(const Record& record, AttachmentIndex* attachmentIndex) {
  constexpr uint64_t PreambleSize = 8 + 8 + 8 + 8 + 8 + 4;

  assert(record.opcode == OpCode::AttachmentIndex);
  if (record.dataSize < PreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "AttachmentIndex", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  attachmentIndex->offset = internal::ParseUint64(record.data);
  attachmentIndex->length = internal::ParseUint64(record.data + 8);
  attachmentIndex->logTime = internal::ParseUint64(record.data + 8 + 8);
  attachmentIndex->createTime = internal::ParseUint64(record.data + 8 + 8 + 8);
  attachmentIndex->dataSize = internal::ParseUint64(record.data + 8 + 8 + 8 + 8);

  uint32_t offset = 8 + 8 + 8 + 8 + 8;

  // name
  if (auto status = internal::ParseString(record.data + offset, record.dataSize - offset,
                                          &attachmentIndex->name);
      !status.ok()) {
    return status;
  }
  offset += 4 + attachmentIndex->name.size();
  // content_type
  if (auto status = internal::ParseString(record.data + offset, record.dataSize - offset,
                                          &attachmentIndex->contentType);
      !status.ok()) {
    return status;
  }

  return StatusCode::Success;
}

Status McapReader::ParseStatistics(const Record& record, Statistics* statistics) {
  constexpr uint64_t PreambleSize = 8 + 2 + 4 + 4 + 4 + 4 + 8 + 8 + 4;

  assert(record.opcode == OpCode::Statistics);
  if (record.dataSize < PreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Statistics", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  statistics->messageCount = internal::ParseUint64(record.data);
  statistics->schemaCount = internal::ParseUint16(record.data + 8);
  statistics->channelCount = internal::ParseUint32(record.data + 8 + 2);
  statistics->attachmentCount = internal::ParseUint32(record.data + 8 + 2 + 4);
  statistics->metadataCount = internal::ParseUint32(record.data + 8 + 2 + 4 + 4);
  statistics->chunkCount = internal::ParseUint32(record.data + 8 + 2 + 4 + 4 + 4);
  statistics->messageStartTime = internal::ParseUint64(record.data + 8 + 2 + 4 + 4 + 4 + 4);
  statistics->messageEndTime = internal::ParseUint64(record.data + 8 + 2 + 4 + 4 + 4 + 4 + 8);

  const uint32_t channelMessageCountsSize =
    internal::ParseUint32(record.data + 8 + 2 + 4 + 4 + 4 + 4 + 8 + 8);
  if (channelMessageCountsSize % 10 != 0 ||
      channelMessageCountsSize > record.dataSize - PreambleSize) {
    const auto msg = internal::StrFormat(
      internal::ErrorMsgInvalidLength, "Statistics.channelMessageCounts", channelMessageCountsSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  const size_t channelMessageCountsCount = size_t(channelMessageCountsSize / 10);
  statistics->channelMessageCounts.reserve(channelMessageCountsCount);
  for (size_t i = 0; i < channelMessageCountsCount; ++i) {
    const auto channelId = internal::ParseUint16(record.data + PreambleSize + i * 10);
    const auto messageCount = internal::ParseUint64(record.data + PreambleSize + i * 10 + 2);
    statistics->channelMessageCounts.emplace(channelId, messageCount);
  }

  return StatusCode::Success;
}

Status McapReader::ParseMetadata(const Record& record, Metadata* metadata) {
  constexpr uint64_t MinSize = 4 + 4;

  assert(record.opcode == OpCode::Metadata);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Metadata", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  // name
  if (auto status = internal::ParseString(record.data, record.dataSize, &metadata->name);
      !status.ok()) {
    return status;
  }
  uint64_t offset = 4 + metadata->name.size();
  // metadata
  if (auto status = internal::ParseKeyValueMap(record.data + offset, record.dataSize - offset,
                                               &metadata->metadata);
      !status.ok()) {
    return status;
  }

  return StatusCode::Success;
}

Status McapReader::ParseMetadataIndex(const Record& record, MetadataIndex* metadataIndex) {
  constexpr uint64_t PreambleSize = 8 + 8 + 4;

  assert(record.opcode == OpCode::MetadataIndex);
  if (record.dataSize < PreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "MetadataIndex", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  metadataIndex->offset = internal::ParseUint64(record.data);
  metadataIndex->length = internal::ParseUint64(record.data + 8);
  uint64_t offset = 8 + 8;
  if (auto status =
        internal::ParseString(record.data + offset, record.dataSize - offset, &metadataIndex->name);
      !status.ok()) {
    return status;
  }

  return StatusCode::Success;
}

Status McapReader::ParseSummaryOffset(const Record& record, SummaryOffset* summaryOffset) {
  constexpr uint64_t MinSize = 1 + 8 + 8;

  assert(record.opcode == OpCode::SummaryOffset);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "SummaryOffset", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  summaryOffset->groupOpCode = OpCode(record.data[0]);
  summaryOffset->groupStart = internal::ParseUint64(record.data + 1);
  summaryOffset->groupLength = internal::ParseUint64(record.data + 1 + 8);

  return StatusCode::Success;
}

Status McapReader::ParseDataEnd(const Record& record, DataEnd* dataEnd) {
  constexpr uint64_t MinSize = 4;

  assert(record.opcode == OpCode::DataEnd);
  if (record.dataSize < MinSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "DataEnd", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  dataEnd->dataSectionCrc = internal::ParseUint32(record.data);
  return StatusCode::Success;
}

std::optional<Compression> McapReader::ParseCompression(const std::string_view compression) {
  if (compression == "") {
    return Compression::None;
  } else if (compression == "lz4") {
    return Compression::Lz4;
  } else if (compression == "zstd") {
    return Compression::Zstd;
  } else {
    return std::nullopt;
  }
}

// RecordReader ////////////////////////////////////////////////////////////////

RecordReader::RecordReader(IReadable& dataSource, ByteOffset startOffset, ByteOffset endOffset)
    : offset(startOffset)
    , endOffset(endOffset)
    , dataSource_(&dataSource)
    , status_(StatusCode::Success)
    , curRecord_{} {}

void RecordReader::reset(IReadable& dataSource, ByteOffset startOffset, ByteOffset endOffset) {
  dataSource_ = &dataSource;
  this->offset = startOffset;
  this->endOffset = endOffset;
  status_ = StatusCode::Success;
  curRecord_ = {};
}

std::optional<Record> RecordReader::next() {
  if (!dataSource_ || offset >= endOffset) {
    return std::nullopt;
  }
  status_ = McapReader::ReadRecord(*dataSource_, offset, &curRecord_);
  if (!status_.ok()) {
    offset = EndOffset;
    return std::nullopt;
  }
  offset += curRecord_.recordSize();
  return curRecord_;
}

const Status& RecordReader::status() {
  return status_;
}

// TypedChunkReader ////////////////////////////////////////////////////////////

TypedChunkReader::TypedChunkReader()
    : reader_{uncompressedReader_, 0, 0}
    , status_{StatusCode::Success} {}

void TypedChunkReader::reset(const Chunk& chunk, Compression compression) {
  ICompressedReader* decompressor =
    (compression == Compression::None)  ? static_cast<ICompressedReader*>(&uncompressedReader_)
    : (compression == Compression::Lz4) ? static_cast<ICompressedReader*>(&lz4Reader_)
                                        : static_cast<ICompressedReader*>(&zstdReader_);
  decompressor->reset(chunk.records, chunk.compressedSize, chunk.uncompressedSize);
  reader_.reset(*decompressor, 0, decompressor->size());
  status_ = decompressor->status();
}

bool TypedChunkReader::next() {
  const auto maybeRecord = reader_.next();
  status_ = reader_.status();
  if (!maybeRecord.has_value()) {
    return false;
  }
  const Record& record = maybeRecord.value();
  switch (record.opcode) {
    case OpCode::Schema: {
      if (onSchema) {
        SchemaPtr schemaPtr = std::make_shared<Schema>();
        status_ = McapReader::ParseSchema(record, schemaPtr.get());
        if (status_.ok()) {
          onSchema(schemaPtr);
        }
      }
      break;
    }
    case OpCode::Channel: {
      if (onChannel) {
        ChannelPtr channelPtr = std::make_shared<Channel>();
        status_ = McapReader::ParseChannel(record, channelPtr.get());
        if (status_.ok()) {
          onChannel(channelPtr);
        }
      }
      break;
    }
    case OpCode::Message: {
      if (onMessage) {
        Message message;
        status_ = McapReader::ParseMessage(record, &message);
        if (status_.ok()) {
          onMessage(message);
        }
      }
      break;
    }
    case OpCode::Header:
    case OpCode::Footer:
    case OpCode::Chunk:
    case OpCode::MessageIndex:
    case OpCode::ChunkIndex:
    case OpCode::Attachment:
    case OpCode::AttachmentIndex:
    case OpCode::Statistics:
    case OpCode::Metadata:
    case OpCode::MetadataIndex:
    case OpCode::SummaryOffset:
    case OpCode::DataEnd: {
      // These opcodes should not appear inside chunks
      const auto msg =
        internal::StrFormat("record type {} cannot appear in Chunk", uint8_t(record.opcode));
      status_ = Status{StatusCode::InvalidOpCode, msg};
      break;
    }
    default: {
      // Unknown opcode, ignore it
      break;
    }
  }

  return true;
}

ByteOffset TypedChunkReader::offset() const {
  return reader_.offset;
}

const Status& TypedChunkReader::status() const {
  return status_;
}

// TypedRecordReader ///////////////////////////////////////////////////////////

TypedRecordReader::TypedRecordReader(IReadable& dataSource, ByteOffset startOffset,
                                     ByteOffset endOffset)
    : reader_(dataSource, startOffset, std::min(endOffset, dataSource.size()))
    , status_(StatusCode::Success)
    , parsingChunk_(false) {
  chunkReader_.onSchema = [&](const SchemaPtr schema) {
    if (onSchema) {
      onSchema(schema);
    }
  };
  chunkReader_.onChannel = [&](const ChannelPtr channel) {
    if (onChannel) {
      onChannel(channel);
    }
  };
  chunkReader_.onMessage = [&](const Message& message) {
    if (onMessage) {
      onMessage(message);
    }
  };
}

bool TypedRecordReader::next() {
  if (parsingChunk_) {
    const bool chunkInProgress = chunkReader_.next();
    status_ = chunkReader_.status();
    if (!chunkInProgress) {
      parsingChunk_ = false;
      if (onChunkEnd) {
        onChunkEnd();
      }
    }
    return true;
  }

  const auto maybeRecord = reader_.next();
  status_ = reader_.status();
  if (!maybeRecord.has_value()) {
    return false;
  }
  const Record& record = maybeRecord.value();

  switch (record.opcode) {
    case OpCode::Header: {
      if (onHeader) {
        Header header;
        if (status_ = McapReader::ParseHeader(record, &header); status_.ok()) {
          onHeader(header);
        }
      }
      break;
    }
    case OpCode::Footer: {
      if (onFooter) {
        Footer footer;
        if (status_ = McapReader::ParseFooter(record, &footer); status_.ok()) {
          onFooter(footer);
        }
      }
      reader_.offset = EndOffset;
      break;
    }
    case OpCode::Schema: {
      if (onSchema) {
        SchemaPtr schemaPtr = std::make_shared<Schema>();
        if (status_ = McapReader::ParseSchema(record, schemaPtr.get()); status_.ok()) {
          onSchema(schemaPtr);
        }
      }
      break;
    }
    case OpCode::Channel: {
      if (onChannel) {
        ChannelPtr channelPtr = std::make_shared<Channel>();
        if (status_ = McapReader::ParseChannel(record, channelPtr.get()); status_.ok()) {
          onChannel(channelPtr);
        }
      }
      break;
    }
    case OpCode::Message: {
      if (onMessage) {
        Message message;
        if (status_ = McapReader::ParseMessage(record, &message); status_.ok()) {
          onMessage(message);
        }
      }
      break;
    }
    case OpCode::Chunk: {
      if (onMessage || onChunk || onSchema || onChannel) {
        Chunk chunk;
        status_ = McapReader::ParseChunk(record, &chunk);
        if (!status_.ok()) {
          return true;
        }
        if (onChunk) {
          onChunk(chunk);
        }
        if (onMessage || onSchema || onChannel) {
          const auto maybeCompression = McapReader::ParseCompression(chunk.compression);
          if (!maybeCompression.has_value()) {
            const auto msg =
              internal::StrFormat("unrecognized compression \"{}\"", chunk.compression);
            status_ = Status{StatusCode::UnrecognizedCompression, msg};
            return true;
          }

          // Start iterating through this chunk
          chunkReader_.reset(chunk, maybeCompression.value());
          status_ = chunkReader_.status();
          parsingChunk_ = true;
        }
      }
      break;
    }
    case OpCode::MessageIndex: {
      if (onMessageIndex) {
        MessageIndex messageIndex;
        if (status_ = McapReader::ParseMessageIndex(record, &messageIndex); status_.ok()) {
          onMessageIndex(messageIndex);
        }
      }
      break;
    }
    case OpCode::ChunkIndex: {
      if (onChunkIndex) {
        ChunkIndex chunkIndex;
        if (status_ = McapReader::ParseChunkIndex(record, &chunkIndex); status_.ok()) {
          onChunkIndex(chunkIndex);
        }
      }
      break;
    }
    case OpCode::Attachment: {
      if (onAttachment) {
        Attachment attachment;
        if (status_ = McapReader::ParseAttachment(record, &attachment); status_.ok()) {
          onAttachment(attachment);
        }
      }
      break;
    }
    case OpCode::AttachmentIndex: {
      if (onAttachmentIndex) {
        AttachmentIndex attachmentIndex;
        if (status_ = McapReader::ParseAttachmentIndex(record, &attachmentIndex); status_.ok()) {
          onAttachmentIndex(attachmentIndex);
        }
      }
      break;
    }
    case OpCode::Statistics: {
      if (onStatistics) {
        Statistics statistics;
        if (status_ = McapReader::ParseStatistics(record, &statistics); status_.ok()) {
          onStatistics(statistics);
        }
      }
      break;
    }
    case OpCode::Metadata: {
      if (onMetadata) {
        Metadata metadata;
        if (status_ = McapReader::ParseMetadata(record, &metadata); status_.ok()) {
          onMetadata(metadata);
        }
      }
      break;
    }
    case OpCode::MetadataIndex: {
      if (onMetadataIndex) {
        MetadataIndex metadataIndex;
        if (status_ = McapReader::ParseMetadataIndex(record, &metadataIndex); status_.ok()) {
          onMetadataIndex(metadataIndex);
        }
      }
      break;
    }
    case OpCode::SummaryOffset: {
      if (onSummaryOffset) {
        SummaryOffset summaryOffset;
        if (status_ = McapReader::ParseSummaryOffset(record, &summaryOffset); status_.ok()) {
          onSummaryOffset(summaryOffset);
        }
      }
      break;
    }
    case OpCode::DataEnd: {
      if (onDataEnd) {
        DataEnd dataEnd;
        if (status_ = McapReader::ParseDataEnd(record, &dataEnd); status_.ok()) {
          onDataEnd(dataEnd);
        }
      }
      break;
    }
    default:
      if (onUnknownRecord) {
        onUnknownRecord(record);
      }
      break;
  }

  return true;
}

ByteOffset TypedRecordReader::offset() const {
  return reader_.offset + (parsingChunk_ ? chunkReader_.offset() : 0);
}

const Status& TypedRecordReader::status() const {
  return status_;
}

// LinearMessageView ///////////////////////////////////////////////////////////

LinearMessageView::LinearMessageView(McapReader& mcapReader, const ProblemCallback& onProblem)
    : mcapReader_(mcapReader)
    , dataStart_(0)
    , dataEnd_(0)
    , startTime_(0)
    , endTime_(0)
    , onProblem_(onProblem) {}

LinearMessageView::LinearMessageView(McapReader& mcapReader, ByteOffset dataStart,
                                     ByteOffset dataEnd, Timestamp startTime, Timestamp endTime,
                                     const ProblemCallback& onProblem)
    : mcapReader_(mcapReader)
    , dataStart_(dataStart)
    , dataEnd_(dataEnd)
    , startTime_(startTime)
    , endTime_(endTime)
    , onProblem_(onProblem) {}

LinearMessageView::Iterator LinearMessageView::begin() {
  if (dataStart_ == dataEnd_ || !mcapReader_.dataSource()) {
    return end();
  }
  return LinearMessageView::Iterator{mcapReader_, dataStart_, dataEnd_,
                                     startTime_,  endTime_,   onProblem_};
}

LinearMessageView::Iterator LinearMessageView::end() {
  return LinearMessageView::Iterator::end();
}

// LinearMessageView::Iterator /////////////////////////////////////////////////

LinearMessageView::Iterator::Iterator(McapReader& mcapReader, const ProblemCallback& onProblem)
    : mcapReader_(mcapReader)
    , recordReader_(std::nullopt)
    , startTime_(0)
    , endTime_(0)
    , onProblem_(onProblem) {}

LinearMessageView::Iterator::Iterator(McapReader& mcapReader, ByteOffset dataStart,
                                      ByteOffset dataEnd, Timestamp startTime, Timestamp endTime,
                                      const ProblemCallback& onProblem)
    : mcapReader_(mcapReader)
    , recordReader_(std::in_place, *mcapReader.dataSource(), dataStart, dataEnd)
    , startTime_(startTime)
    , endTime_(endTime)
    , onProblem_(onProblem) {
  recordReader_->onSchema = [this](const SchemaPtr schema) {
    mcapReader_.schemas_.insert_or_assign(schema->id, schema);
  };
  recordReader_->onChannel = [this](const ChannelPtr channel) {
    mcapReader_.channels_.insert_or_assign(channel->id, channel);
  };
  recordReader_->onMessage = [this](const Message& message) {
    auto maybeChannel = mcapReader_.channel(message.channelId);
    if (!maybeChannel) {
      onProblem_(Status{
        StatusCode::InvalidChannelId,
        internal::StrFormat("message at log_time {} (seq {}) references missing channel id {}",
                            message.logTime, message.sequence, message.channelId)});
      return;
    }

    auto& channel = *maybeChannel;
    SchemaPtr maybeSchema;
    if (channel.schemaId != 0) {
      maybeSchema = mcapReader_.schema(channel.schemaId);
      if (!maybeSchema) {
        onProblem_(Status{StatusCode::InvalidSchemaId,
                          internal::StrFormat("channel {} ({}) references missing schema id {}",
                                              channel.id, channel.topic, channel.schemaId)});
        return;
      }
    }

    curMessage_ = message;  // copy message, which may be a reference to a temporary
    curMessageView_.emplace(curMessage_, maybeChannel, maybeSchema);
  };

  ++(*this);
}

LinearMessageView::Iterator::reference LinearMessageView::Iterator::operator*() const {
  return *curMessageView_;
}

LinearMessageView::Iterator::pointer LinearMessageView::Iterator::operator->() const {
  return &*curMessageView_;
}

LinearMessageView::Iterator& LinearMessageView::Iterator::operator++() {
  curMessageView_ = std::nullopt;

  if (!recordReader_.has_value()) {
    return *this;
  }

  // Keep iterate through records until we find a message with a logTime >= startTime_
  while (!curMessageView_.has_value() || curMessageView_->message.logTime < startTime_) {
    const bool found = recordReader_->next();

    // Surface any problem that may have occurred while reading
    auto& status = recordReader_->status();
    if (!status.ok()) {
      onProblem_(status);
    }

    if (!found) {
      recordReader_ = std::nullopt;
      return *this;
    }
  }

  // Check if this message is past the time range of this view
  if (curMessageView_->message.logTime >= endTime_) {
    recordReader_ = std::nullopt;
  }
  return *this;
}

bool operator==(const LinearMessageView::Iterator& a, const LinearMessageView::Iterator& b) {
  const bool aEnd = !a.recordReader_.has_value();
  const bool bEnd = !b.recordReader_.has_value();
  if (aEnd && bEnd) {
    return true;
  } else if (aEnd || bEnd) {
    return false;
  }
  return a.recordReader_->offset() == b.recordReader_->offset();
}

bool operator!=(const LinearMessageView::Iterator& a, const LinearMessageView::Iterator& b) {
  return !(a == b);
}

}  // namespace mcap
