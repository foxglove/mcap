// Do not compile on systems with non-8-bit bytes
static_assert(std::numeric_limits<unsigned char>::digits == 8);

namespace mcap {

// Internal methods ////////////////////////////////////////////////////////////

namespace internal {

constexpr std::string_view ErrorMsgInvalidOpcode = "invalid opcode, expected {}: 0x{:02x}";
constexpr std::string_view ErrorMsgInvalidLength = "invalid {} length: {}";
constexpr std::string_view ErrorMsgInvalidMagic = "invalid magic bytes in {}: 0x{}";

uint32_t KeyValueMapSize(const KeyValueMap& map) {
  uint32_t size = 0;
  for (const auto& [key, value] : map) {
    size += 4 + key.size() + 4 + value.size();
  }
  return size;
}

const std::string& CompressionString(Compression compression) {
  static std::string none = "";
  static std::string lz4 = "lz4";
  static std::string zstd = "zstd";
  switch (compression) {
    case Compression::None:
      return none;
    case Compression::Lz4:
      return lz4;
    case Compression::Zstd:
      return zstd;
  }
}

uint16_t ReadUint16(const std::byte* data) {
  return uint16_t(data[0]) | (uint16_t(data[1]) << 8);
}

uint32_t ReadUint32(const std::byte* data) {
  return uint32_t(data[0]) | (uint32_t(data[1]) << 8) | (uint32_t(data[2]) << 16) |
         (uint32_t(data[3]) << 24);
}

mcap::Status ReadUint32(const std::byte* data, uint64_t maxSize, uint32_t* output) {
  if (maxSize < 4) {
    const auto msg = internal::StrFormat("cannot read uint32 from {} bytes", maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  *output = ReadUint32(data);
  return StatusCode::Success;
}

uint32_t ReadUint64(const std::byte* data) {
  return uint64_t(data[0]) | (uint64_t(data[1]) << 8) | (uint64_t(data[2]) << 16) |
         (uint64_t(data[3]) << 24) | (uint64_t(data[4]) << 32) | (uint64_t(data[5]) << 40) |
         (uint64_t(data[6]) << 48) | (uint64_t(data[7]) << 56);
}

mcap::Status ReadUint64(const std::byte* data, uint64_t maxSize, uint64_t* output) {
  if (maxSize < 8) {
    const auto msg = internal::StrFormat("cannot read uint64 from {} bytes", maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  *output = ReadUint64(data);
  return StatusCode::Success;
}

mcap::Status ReadString(const std::byte* data, uint64_t maxSize, std::string_view* output) {
  uint32_t size;
  if (auto status = ReadUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg =
      internal::StrFormat("string size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return mcap::Status(mcap::StatusCode::InvalidRecord, msg);
  }
  *output = std::string_view(reinterpret_cast<const char*>(data + 4), size);
  return StatusCode::Success;
}

mcap::Status ReadString(const std::byte* data, uint64_t maxSize, std::string* output) {
  uint32_t size;
  if (auto status = ReadUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg =
      internal::StrFormat("string size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return mcap::Status(mcap::StatusCode::InvalidRecord, msg);
  }
  *output = std::string(reinterpret_cast<const char*>(data + 4), size);
  return StatusCode::Success;
}

mcap::Status ReadByteArray(const std::byte* data, uint64_t maxSize, mcap::ByteArray* output) {
  uint32_t size;
  if (auto status = ReadUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg =
      internal::StrFormat("byte array size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return mcap::Status(mcap::StatusCode::InvalidRecord, msg);
  }
  output->resize(size);
  std::memcpy(output->data(), data + 4, size);
  return StatusCode::Success;
}

mcap::Status ReadKeyValueMap(const std::byte* data, uint64_t maxSize, mcap::KeyValueMap* output) {
  uint32_t sizeInBytes;
  if (auto status = ReadUint32(data, maxSize, &sizeInBytes); !status.ok()) {
    return status;
  }
  if (sizeInBytes > (maxSize - 4)) {
    const auto msg = internal::StrFormat("key-value map size {} exceeds remaining bytes {}",
                                         sizeInBytes, (maxSize - 4));
    return mcap::Status(mcap::StatusCode::InvalidRecord, msg);
  }
  output->clear();
  uint64_t pos = 4;
  while (pos < sizeInBytes) {
    std::string_view key;
    if (auto status = ReadString(data + pos, sizeInBytes - pos, &key); !status.ok()) {
      return status;
    }
    pos += 4 + key.size();
    std::string_view value;
    if (auto status = ReadString(data + pos, sizeInBytes - pos, &value); !status.ok()) {
      return status;
    }
    pos += 4 + value.size();
    output->emplace(key, value);
  }
  return StatusCode::Success;
}

std::string MagicToHex(const std::byte* data) {
  return internal::StrFormat("{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", data[0], data[1],
                             data[2], data[3], data[4], data[5], data[6], data[7]);
}

}  // namespace internal

MetadataIndex::MetadataIndex(const Metadata& metadata, mcap::ByteOffset fileOffset)
    : offset(fileOffset)
    , length(9 + 4 + metadata.name.size() + 4 + internal::KeyValueMapSize(metadata.metadata))
    , name(metadata.name) {}

// McapWriter //////////////////////////////////////////////////////////////////

McapWriter::~McapWriter() {
  close();
}

void McapWriter::open(mcap::IWritable& writer, const McapWriterOptions& options) {
  opened_ = true;
  chunkSize_ = options.noChunking ? 0 : options.chunkSize;
  indexing_ = !options.noIndexing;
  compression_ = chunkSize_ > 0 ? options.compression : Compression::None;
  switch (compression_) {
    case Compression::None:
      uncompressedChunk_ = std::make_unique<mcap::BufferedWriter>();
      break;
    case Compression::Lz4:
      lz4Chunk_ = std::make_unique<mcap::LZ4Writer>(options.compressionLevel, chunkSize_);
      break;
    case Compression::Zstd:
      zstdChunk_ = std::make_unique<mcap::ZStdWriter>(options.compressionLevel, chunkSize_);
      break;
  }
  output_ = &writer;
  writeMagic(writer);
  write(writer, Header{options.profile, options.library});

  // FIXME: Write options.metadata
}

void McapWriter::open(std::ostream& stream, const McapWriterOptions& options) {
  streamOutput_ = std::make_unique<mcap::StreamWriter>(stream);
  open(*streamOutput_, options);
}

void McapWriter::close() {
  if (!opened_ || !output_) {
    return;
  }
  auto* chunkWriter = getChunkWriter();
  auto& fileOutput = *output_;

  // Check if there is an open chunk that needs to be closed
  if (chunkWriter && !chunkWriter->empty()) {
    writeChunk(fileOutput, *chunkWriter);
  }

  // Write the Data End record
  uint32_t dataSectionCrc = 0;
  write(fileOutput, DataEnd{dataSectionCrc});

  mcap::ByteOffset summaryStart = 0;
  mcap::ByteOffset summaryOffsetStart = 0;
  uint32_t summaryCrc = 0;

  if (indexing_) {
    // Get the offset of the End Of File section
    summaryStart = fileOutput.size();

    // Write all channel info records
    for (const auto& channel : channels_) {
      write(fileOutput, channel);
    }

    // Write chunk index records
    mcap::ByteOffset chunkIndexStart = fileOutput.size();
    for (const auto& chunkIndexRecord : chunkIndex_) {
      write(fileOutput, chunkIndexRecord);
    }

    // Write attachment index records
    mcap::ByteOffset attachmentIndexStart = fileOutput.size();
    for (const auto& attachmentIndexRecord : attachmentIndex_) {
      write(fileOutput, attachmentIndexRecord);
    }

    // Write metadata index records
    mcap::ByteOffset metadataIndexStart = fileOutput.size();
    for (const auto& metadataIndexRecord : metadataIndex_) {
      write(fileOutput, metadataIndexRecord);
    }

    // Write the statistics record
    mcap::ByteOffset statisticsStart = fileOutput.size();
    write(fileOutput, statistics_);

    // Write summary offset records
    mcap::ByteOffset summaryOffsetStart = fileOutput.size();
    if (!channels_.empty()) {
      write(fileOutput,
            SummaryOffset{OpCode::ChannelInfo, summaryStart, chunkIndexStart - summaryStart});
    }
    if (!chunkIndex_.empty()) {
      write(fileOutput, SummaryOffset{OpCode::ChunkIndex, chunkIndexStart,
                                      attachmentIndexStart - chunkIndexStart});
    }
    if (!attachmentIndex_.empty()) {
      write(fileOutput, SummaryOffset{OpCode::AttachmentIndex, attachmentIndexStart,
                                      metadataIndexStart - attachmentIndexStart});
    }
    if (!metadataIndex_.empty()) {
      write(fileOutput, SummaryOffset{OpCode::MetadataIndex, metadataIndexStart,
                                      statisticsStart - metadataIndexStart});
    }
  }

  // TODO: Calculate the summary CRC

  // Write the footer and trailing magic
  write(fileOutput, mcap::Footer{summaryStart, summaryOffsetStart, summaryCrc});
  writeMagic(fileOutput);

  // Flush output
  fileOutput.end();

  terminate();
}

void McapWriter::terminate() {
  output_ = nullptr;
  streamOutput_.reset();
  uncompressedChunk_.reset();
  zstdChunk_.reset();

  channels_.clear();
  attachmentIndex_.clear();
  metadataIndex_.clear();
  chunkIndex_.clear();
  statistics_ = {};
  currentMessageIndex_.clear();
  currentChunkStart_ = std::numeric_limits<uint64_t>::max();
  currentChunkEnd_ = std::numeric_limits<uint64_t>::min();

  opened_ = false;
}

void McapWriter::addChannel(mcap::ChannelInfo& info) {
  info.id = uint16_t(channels_.size() + 1);
  channels_.push_back(info);
}

mcap::Status McapWriter::write(const mcap::Message& message) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& output = getOutput();
  auto& channelMessageCounts = statistics_.channelMessageCounts;

  // Write out channel info if we have not yet done so
  if (channelMessageCounts.find(message.channelId) == channelMessageCounts.end()) {
    const size_t index = message.channelId - 1;
    if (index >= channels_.size()) {
      return StatusCode::InvalidChannelId;
    }

    // Write the channel info record
    uncompressedSize_ += write(output, channels_[index]);

    // Update channel statistics
    channelMessageCounts.emplace(message.channelId, 0);
    ++statistics_.channelCount;
  }

  const uint64_t messageOffset = uncompressedSize_;

  // Write the message
  uncompressedSize_ += write(output, message);

  // Update message statistics
  if (indexing_) {
    ++statistics_.messageCount;
    channelMessageCounts[message.channelId] += 1;
  }

  auto* chunkWriter = getChunkWriter();
  if (chunkWriter) {
    if (indexing_) {
      // Update the message index
      auto& messageIndex = currentMessageIndex_[message.channelId];
      messageIndex.channelId = message.channelId;
      ++messageIndex.count;
      messageIndex.records.emplace_back(message.recordTime, messageOffset);

      // Update the chunk index start/end times
      currentChunkStart_ = std::min(currentChunkStart_, message.recordTime);
      currentChunkEnd_ = std::max(currentChunkEnd_, message.recordTime);
    }

    // Check if the current chunk is ready to close
    if (uncompressedSize_ >= chunkSize_) {
      auto& fileOutput = *output_;
      writeChunk(fileOutput, *chunkWriter);
    }
  }

  return StatusCode::Success;
}

mcap::Status McapWriter::write(const mcap::Attachment& attachment) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& fileOutput = *output_;

  // Check if we have an open chunk that needs to be closed
  auto* chunkWriter = getChunkWriter();
  if (chunkWriter && !chunkWriter->empty()) {
    writeChunk(fileOutput, *chunkWriter);
  }

  const uint64_t fileOffset = fileOutput.size();

  // Write the attachment
  write(fileOutput, attachment);

  // Update statistics and attachment index
  if (indexing_) {
    ++statistics_.attachmentCount;
    attachmentIndex_.emplace_back(attachment, fileOffset);
  }

  return StatusCode::Success;
}

mcap::Status McapWriter::write(const mcap::Metadata& metadata) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& fileOutput = *output_;

  // Check if we have an open chunk that needs to be closed
  auto* chunkWriter = getChunkWriter();
  if (chunkWriter && !chunkWriter->empty()) {
    writeChunk(fileOutput, *chunkWriter);
  }

  const uint64_t fileOffset = fileOutput.size();

  // Write the metadata
  write(fileOutput, metadata);

  // Update statistics and metadata index
  if (indexing_) {
    ++statistics_.metadataCount;
    metadataIndex_.emplace_back(metadata, fileOffset);
  }

  return StatusCode::Success;
}

// Private methods /////////////////////////////////////////////////////////////

mcap::IWritable& McapWriter::getOutput() {
  if (chunkSize_ == 0) {
    return *output_;
  }
  switch (compression_) {
    case Compression::None:
      return *uncompressedChunk_;
    case Compression::Lz4:
      return *lz4Chunk_;
    case Compression::Zstd:
      return *zstdChunk_;
  }
}

mcap::IChunkWriter* McapWriter::getChunkWriter() {
  switch (compression_) {
    case Compression::None:
      return uncompressedChunk_.get();
    case Compression::Lz4:
      return lz4Chunk_.get();
    case Compression::Zstd:
      return zstdChunk_.get();
  }
}

void McapWriter::writeChunk(mcap::IWritable& output, mcap::IChunkWriter& chunkData) {
  const auto& compression = internal::CompressionString(compression_);

  // Flush any in-progress compression stream
  chunkData.end();

  const uint64_t compressedSize = chunkData.size();
  const std::byte* data = chunkData.data();
  const uint32_t uncompressedCrc = 0;

  // Write the chunk
  const uint64_t chunkStartOffset = output.size();
  write(output, Chunk{uncompressedSize_, uncompressedCrc, compression, compressedSize, data});

  if (indexing_) {
    // Update statistics
    const uint64_t chunkLength = output.size() - chunkStartOffset;
    ++statistics_.chunkCount;

    // Create a chunk index record
    auto& chunkIndexRecord = chunkIndex_.emplace_back();

    // Write the message index records
    const uint64_t messageIndexOffset = output.size();
    for (const auto& [channelId, messageIndex] : currentMessageIndex_) {
      chunkIndexRecord.messageIndexOffsets.emplace(channelId, output.size());
      write(output, messageIndex);
    }
    currentMessageIndex_.clear();
    const uint64_t messageIndexLength = output.size() - messageIndexOffset;

    // Fill in the newly created chunk index record. This will be written into
    // the summary section when close() is called
    chunkIndexRecord.startTime = currentChunkStart_;
    chunkIndexRecord.endTime = currentChunkEnd_;
    chunkIndexRecord.chunkStartOffset = chunkStartOffset;
    chunkIndexRecord.chunkLength = chunkLength;
    chunkIndexRecord.messageIndexLength = messageIndexLength;
    chunkIndexRecord.compression = compression;
    chunkIndexRecord.compressedSize = compressedSize;
    chunkIndexRecord.uncompressedSized = uncompressedSize_;

    // Reset uncompressedSize and start/end times for the next chunk
    uncompressedSize_ = 0;
    currentChunkStart_ = std::numeric_limits<uint64_t>::max();
    currentChunkEnd_ = std::numeric_limits<uint64_t>::min();
  }

  // Reset the chunk writer
  chunkData.clear();
}

void McapWriter::writeMagic(mcap::IWritable& output) {
  write(output, reinterpret_cast<const std::byte*>(Magic), sizeof(Magic));
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Header& header) {
  const uint64_t recordSize = 4 + header.profile.size() + 4 + header.library.size();

  write(output, OpCode::Header);
  write(output, recordSize);
  write(output, header.profile);
  write(output, header.library);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Footer& footer) {
  const uint64_t recordSize = 12;

  write(output, OpCode::Footer);
  write(output, recordSize);
  write(output, footer.summaryStart);
  write(output, footer.summaryOffsetStart);
  write(output, footer.summaryCrc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::ChannelInfo& info) {
  const uint32_t metadataSize = internal::KeyValueMapSize(info.metadata);
  const uint64_t recordSize = /* id */ 2 +
                              /* topic */ 4 + info.topic.size() +
                              /* message_encoding */ 4 + info.messageEncoding.size() +
                              /* schema_encoding */ 4 + info.schemaEncoding.size() +
                              /* schema */ 4 + info.schema.size() +
                              /* metadata */ 4 + metadataSize;

  write(output, OpCode::ChannelInfo);
  write(output, recordSize);
  write(output, info.id);
  write(output, info.topic);
  write(output, info.messageEncoding);
  write(output, info.schemaEncoding);
  write(output, info.schema);
  write(output, info.metadata, metadataSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Message& message) {
  const uint64_t recordSize = 2 + 4 + 8 + 8 + message.dataSize;

  write(output, OpCode::Message);
  write(output, recordSize);
  write(output, message.channelId);
  write(output, message.sequence);
  write(output, message.publishTime);
  write(output, message.recordTime);
  write(output, message.data, message.dataSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Attachment& attachment) {
  const uint64_t recordSize =
    4 + attachment.name.size() + 8 + 4 + attachment.contentType.size() + attachment.dataSize;

  write(output, OpCode::Attachment);
  write(output, recordSize);
  write(output, attachment.name);
  write(output, attachment.recordTime);
  write(output, attachment.contentType);
  write(output, attachment.data, attachment.dataSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Metadata& metadata) {
  const uint32_t metadataSize = internal::KeyValueMapSize(metadata.metadata);
  const uint64_t recordSize = 4 + metadata.name.size() + 4 + metadataSize;

  write(output, OpCode::Metadata);
  write(output, recordSize);
  write(output, metadata.name);
  write(output, metadata.metadata, metadataSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Chunk& chunk) {
  const uint64_t recordSize = 8 + 4 + 4 + chunk.compression.size() + chunk.recordsSize;

  write(output, OpCode::Chunk);
  write(output, recordSize);
  write(output, chunk.uncompressedSize);
  write(output, chunk.uncompressedCrc);
  write(output, chunk.compression);
  write(output, chunk.records, chunk.recordsSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::MessageIndex& index) {
  const uint32_t recordsSize = index.records.size() * 16;
  const uint64_t recordSize = 2 + 4 + 4 + recordsSize + 4;
  const uint32_t crc = 0;

  write(output, OpCode::MessageIndex);
  write(output, recordSize);
  write(output, index.channelId);
  write(output, index.count);

  write(output, recordsSize);
  for (const auto& [timestamp, offset] : index.records) {
    write(output, timestamp);
    write(output, offset);
  }

  write(output, crc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::ChunkIndex& index) {
  const uint32_t messageIndexOffsetsSize = index.messageIndexOffsets.size() * 10;
  const uint64_t recordSize =
    8 + 8 + 8 + 4 + messageIndexOffsetsSize + 8 + 4 + index.compression.size() + 8 + 8 + 4;
  const uint32_t crc = 0;

  write(output, OpCode::ChunkIndex);
  write(output, recordSize);
  write(output, index.startTime);
  write(output, index.endTime);
  write(output, index.chunkStartOffset);

  write(output, messageIndexOffsetsSize);
  for (const auto& [channelId, offset] : index.messageIndexOffsets) {
    write(output, channelId);
    write(output, offset);
  }

  write(output, index.messageIndexLength);
  write(output, index.compression);
  write(output, index.compressedSize);
  write(output, index.uncompressedSized);
  write(output, crc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::AttachmentIndex& index) {
  const uint64_t recordSize = 8 + 8 + 4 + index.name.size() + 4 + index.contentType.size() + 8;

  write(output, OpCode::AttachmentIndex);
  write(output, recordSize);
  write(output, index.recordTime);
  write(output, index.attachmentSize);
  write(output, index.name);
  write(output, index.contentType);
  write(output, index.offset);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::MetadataIndex& index) {
  const uint64_t recordSize = 8 + 8 + 4 + index.name.size();

  write(output, OpCode::MetadataIndex);
  write(output, recordSize);
  write(output, index.offset);
  write(output, index.length);
  write(output, index.name);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Statistics& stats) {
  const uint32_t channelMessageCountsSize = stats.channelMessageCounts.size() * 10;
  const uint64_t recordSize = 8 + 4 + 4 + 4 + 4 + channelMessageCountsSize;

  write(output, OpCode::Statistics);
  write(output, recordSize);
  write(output, stats.messageCount);
  write(output, stats.channelCount);
  write(output, stats.attachmentCount);
  write(output, stats.chunkCount);

  write(output, channelMessageCountsSize);
  for (const auto& [channelId, messageCount] : stats.channelMessageCounts) {
    write(output, channelId);
    write(output, messageCount);
  }

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::SummaryOffset& summaryOffset) {
  const uint64_t recordSize = 1 + 8 + 8;

  write(output, OpCode::SummaryOffset);
  write(output, recordSize);
  write(output, summaryOffset.groupOpCode);
  write(output, summaryOffset.groupStart);
  write(output, summaryOffset.groupLength);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::DataEnd& dataEnd) {
  const uint64_t recordSize = 4;

  write(output, OpCode::DataEnd);
  write(output, recordSize);
  write(output, dataEnd.dataSectionCrc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::UnknownRecord& record) {
  write(output, mcap::OpCode(record.opcode));
  write(output, record.dataSize);
  write(output, record.data, record.dataSize);

  return 9 + record.dataSize;
}

void McapWriter::write(mcap::IWritable& output, const std::string_view str) {
  write(output, uint32_t(str.size()));
  output.write(reinterpret_cast<const std::byte*>(str.data()), str.size());
}

void McapWriter::write(mcap::IWritable& output, const mcap::ByteArray bytes) {
  write(output, uint32_t(bytes.size()));
  output.write(bytes.data(), bytes.size());
}

void McapWriter::write(mcap::IWritable& output, OpCode value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(mcap::IWritable& output, uint16_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(mcap::IWritable& output, uint32_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(mcap::IWritable& output, uint64_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(mcap::IWritable& output, const std::byte* data, uint64_t size) {
  output.write(reinterpret_cast<const std::byte*>(data), size);
}

void McapWriter::write(mcap::IWritable& output, const KeyValueMap& map, uint32_t size) {
  write(output, size > 0 ? size : internal::KeyValueMapSize(map));
  for (const auto& [key, value] : map) {
    write(output, key);
    write(output, value);
  }
}

// BufferedWriter //////////////////////////////////////////////////////////////

void BufferedWriter::write(const std::byte* data, uint64_t size) {
  buffer_.insert(buffer_.end(), data, data + size);
}

void BufferedWriter::end() {
  // no-op
}

uint64_t BufferedWriter::size() const {
  return buffer_.size();
}

bool BufferedWriter::empty() const {
  return buffer_.empty();
}

void BufferedWriter::clear() {
  buffer_.clear();
}

const std::byte* BufferedWriter::data() const {
  return buffer_.data();
}

// StreamWriter ////////////////////////////////////////////////////////////////

StreamWriter::StreamWriter(std::ostream& stream)
    : stream_(stream)
    , size_(0) {}

void StreamWriter::write(const std::byte* data, uint64_t size) {
  stream_.write(reinterpret_cast<const char*>(data), std::streamsize(size));
  size_ += size;
}

void StreamWriter::end() {
  stream_.flush();
}

uint64_t StreamWriter::size() const {
  return size_;
}

// LZ4Writer ///////////////////////////////////////////////////////////////////

namespace internal {

constexpr uint64_t MinHeaderLength = /* magic bytes */ sizeof(Magic) +
                                     /* opcode */ 1 +
                                     /* record length */ 8 +
                                     /* profile length */ 4 +
                                     /* library length */ 4;
constexpr uint64_t FooterLength = /* opcode */ 1 +
                                  /* record length */ 8 +
                                  /* summary start */ 8 +
                                  /* summary offset start */ 8 +
                                  /* summary crc */ 4 +
                                  /* magic bytes */ sizeof(Magic);

int LZ4AccelerationLevel(CompressionLevel level) {
  switch (level) {
    case CompressionLevel::Fastest:
      return 65537;
    case CompressionLevel::Fast:
      return 32768;
    case CompressionLevel::Default:
    case CompressionLevel::Slow:
    case CompressionLevel::Slowest:
      return 1;
  }
}

}  // namespace internal

LZ4Writer::LZ4Writer(CompressionLevel compressionLevel, uint64_t chunkSize) {
  acceleration_ = internal::LZ4AccelerationLevel(compressionLevel);
  preEndBuffer_.reserve(chunkSize);
}

void LZ4Writer::write(const std::byte* data, uint64_t size) {
  preEndBuffer_.insert(preEndBuffer_.end(), data, data + size);
}

void LZ4Writer::end() {
  const auto dstCapacity = LZ4_compressBound(preEndBuffer_.size());
  buffer_.resize(dstCapacity);
  const int dstSize = LZ4_compress_fast(reinterpret_cast<const char*>(preEndBuffer_.data()),
                                        reinterpret_cast<char*>(buffer_.data()),
                                        preEndBuffer_.size(), dstCapacity, acceleration_);
  buffer_.resize(dstSize);
  preEndBuffer_.clear();
}

uint64_t LZ4Writer::size() const {
  return buffer_.size();
}

bool LZ4Writer::empty() const {
  return buffer_.empty() && preEndBuffer_.empty();
}

void LZ4Writer::clear() {
  preEndBuffer_.clear();
  buffer_.clear();
}

const std::byte* LZ4Writer::data() const {
  return buffer_.data();
}

// ZStdWriter //////////////////////////////////////////////////////////////////

namespace internal {

int ZStdCompressionLevel(CompressionLevel level) {
  switch (level) {
    case CompressionLevel::Fastest:
      return -5;
    case CompressionLevel::Fast:
      return -3;
    case CompressionLevel::Default:
      return 1;
    case CompressionLevel::Slow:
      return 5;
    case CompressionLevel::Slowest:
      return 19;
  }
}

}  // namespace internal

// ZStdWriter //////////////////////////////////////////////////////////////////

ZStdWriter::ZStdWriter(CompressionLevel compressionLevel, uint64_t chunkSize) {
  zstdContext_ = ZSTD_createCCtx();
  ZSTD_CCtx_setParameter(zstdContext_, ZSTD_c_compressionLevel,
                         internal::ZStdCompressionLevel(compressionLevel));
  preEndBuffer_.reserve(chunkSize);
}

ZStdWriter::~ZStdWriter() {
  ZSTD_freeCCtx(zstdContext_);
}

void ZStdWriter::write(const std::byte* data, uint64_t size) {
  preEndBuffer_.insert(preEndBuffer_.end(), data, data + size);
}

void ZStdWriter::end() {
  const auto dstCapacity = ZSTD_compressBound(preEndBuffer_.size());
  buffer_.resize(dstCapacity);
  const int dstSize = ZSTD_compress2(zstdContext_, buffer_.data(), dstCapacity,
                                     preEndBuffer_.data(), preEndBuffer_.size());
  if (ZSTD_isError(dstSize)) {
    const auto errCode = ZSTD_getErrorCode(dstSize);
    std::cerr << "ZSTD_compress2 failed: " << ZSTD_getErrorName(dstSize) << " ("
              << ZSTD_getErrorString(errCode) << ")\n";
    std::abort();
  }
  ZSTD_CCtx_reset(zstdContext_, ZSTD_reset_session_only);
  buffer_.resize(dstSize);
  preEndBuffer_.clear();
}

uint64_t ZStdWriter::size() const {
  return buffer_.size();
}

bool ZStdWriter::empty() const {
  return buffer_.empty() && preEndBuffer_.empty();
}

void ZStdWriter::clear() {
  preEndBuffer_.clear();
  buffer_.clear();
}

const std::byte* ZStdWriter::data() const {
  return buffer_.data();
}

// McapReader //////////////////////////////////////////////////////////////////

McapReader::~McapReader() {
  close();
}

mcap::Status McapReader::open(mcap::IReadable& reader, const McapReaderOptions& options) {
  close();

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
  header_ = Header{};
  Record record;
  if (auto status = ReadRecord(reader, sizeof(Magic), &record); !status.ok()) {
    return status;
  }
  if (auto status = ParseHeader(record, &header_.value()); !status.ok()) {
    return status;
  }

  // Read the footer
  auto footer = Footer{};
  if (auto status = ReadFooter(reader, fileSize - internal::FooterLength, &footer); !status.ok()) {
    problems_.push_back(status);
  } else {
    footer_ = footer;
  }

  input_ = &reader;
  options_ = options;
  return StatusCode::Success;
}

void McapReader::close() {
  input_ = nullptr;
}

mcap::Status McapReader::ReadRecord(mcap::IReadable& reader, uint64_t offset,
                                    mcap::Record* record) {
  // Check that we can read at least 9 bytes (opcode + length)
  const auto maxSize = reader.size() - offset;
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

  record->opcode = mcap::OpCode(data[0]);
  if (auto status = internal::ReadUint64(data, maxSize - 9, &record->dataSize); !status.ok()) {
    return status;
  }
  record->data = data + 9;
  return StatusCode::Success;
}

mcap::Status McapReader::ReadFooter(mcap::IReadable& reader, uint64_t offset,
                                    mcap::Footer* footer) {
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
  const uint64_t length = internal::ReadUint64(data + 1);
  if (length != 8 + 8 + 4) {
    const auto msg = internal::StrFormat(internal::ErrorMsgInvalidLength, "Footer", length);
    return Status{StatusCode::InvalidRecord, msg};
  }

  footer->summaryStart = internal::ReadUint64(data + 1 + 8);
  footer->summaryOffsetStart = internal::ReadUint64(data + 1 + 8 + 8);
  footer->summaryCrc = internal::ReadUint32(data + 1 + 8 + 8 + 8);
  return StatusCode::Success;
}

mcap::Status McapReader::ParseHeader(const mcap::Record& record, mcap::Header* header) {
  if (record.opcode != OpCode::Header) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidOpcode, "Header", uint8_t(record.opcode));
    return Status{StatusCode::InvalidFile, msg};
  }
  if (record.dataSize < 4 + 4) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Header", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  if (auto status = internal::ReadString(record.data, record.dataSize, &header->profile);
      !status.ok()) {
    return status;
  }
  const uint64_t maxSize = record.dataSize - 4 - header->profile.size();
  if (auto status = internal::ReadString(record.data, maxSize, &header->library); !status.ok()) {
    return status;
  }
  return StatusCode::Success;
}

mcap::Status McapReader::ParseChannelInfo(const mcap::Record& record,
                                          mcap::ChannelInfo* channelInfo) {
  if (record.opcode != OpCode::ChannelInfo) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidOpcode, "ChannelInfo", uint8_t(record.opcode));
    return Status{StatusCode::InvalidFile, msg};
  }
  if (record.dataSize < 2 + 4 + 4 + 4 + 4 + 4 + 4) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "ChannelInfo", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  size_t offset = 0;

  // id
  channelInfo->id = internal::ReadUint16(record.data);
  offset += 2;
  // topic
  if (auto status =
        internal::ReadString(record.data + offset, record.dataSize - offset, &channelInfo->topic);
      !status.ok()) {
    return status;
  }
  offset += 4 + channelInfo->topic.size();
  // message_encoding
  if (auto status = internal::ReadString(record.data + offset, record.dataSize - offset,
                                         &channelInfo->messageEncoding);
      !status.ok()) {
    return status;
  }
  offset += 4 + channelInfo->messageEncoding.size();
  // schema_encoding
  if (auto status = internal::ReadString(record.data + offset, record.dataSize - offset,
                                         &channelInfo->schemaEncoding);
      !status.ok()) {
    return status;
  }
  offset += 4 + channelInfo->schemaEncoding.size();
  // schema
  if (auto status = internal::ReadByteArray(record.data + offset, record.dataSize - offset,
                                            &channelInfo->schema);
      !status.ok()) {
    return status;
  }
  offset += 4 + channelInfo->schema.size();
  // schema_name
  if (auto status = internal::ReadString(record.data + offset, record.dataSize - offset,
                                         &channelInfo->schemaName);
      !status.ok()) {
    return status;
  }
  offset += 4 + channelInfo->schemaName.size();
  // metadata
  if (auto status = internal::ReadKeyValueMap(record.data + offset, record.dataSize - offset,
                                              &channelInfo->metadata);
      !status.ok()) {
    return status;
  }
  return StatusCode::Success;
}

mcap::Status McapReader::ParseMessage(const mcap::Record& record, mcap::Message* message) {
  constexpr uint64_t MessagePreambleSize = 2 + 4 + 8 + 8;

  if (record.opcode != OpCode::Message) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidOpcode, "Message", uint8_t(record.opcode));
    return Status{StatusCode::InvalidFile, msg};
  }
  if (record.dataSize < MessagePreambleSize) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidLength, "Message", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  message->channelId = internal::ReadUint16(record.data);
  message->sequence = internal::ReadUint32(record.data + 2);
  message->publishTime = internal::ReadUint64(record.data + 2 + 4);
  message->recordTime = internal::ReadUint64(record.data + 2 + 4 + 8);
  message->dataSize = record.dataSize - MessagePreambleSize;
  message->data = record.data + MessagePreambleSize;
  return StatusCode::Success;
}

mcap::Status McapReader::ParseChunk(const mcap::Record& record, mcap::Chunk* chunk) {
  constexpr uint64_t ChunkPreambleSize = 8 + 4 + 4;

  if (record.opcode != OpCode::Chunk) {
    const auto msg =
      internal::StrFormat(internal::ErrorMsgInvalidOpcode, "Chunk", uint8_t(record.opcode));
    return Status{StatusCode::InvalidFile, msg};
  }
  if (record.dataSize < ChunkPreambleSize) {
    const auto msg = internal::StrFormat(internal::ErrorMsgInvalidLength, "Chunk", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  chunk->uncompressedSize = internal::ReadUint64(record.data);
  chunk->uncompressedCrc = internal::ReadUint32(record.data + 8);
  if (auto status = internal::ReadString(record.data + 8 + 4, record.dataSize - ChunkPreambleSize,
                                         &chunk->compression);
      !status.ok()) {
    return status;
  }
  chunk->recordsSize = record.dataSize - ChunkPreambleSize - chunk->compression.size();
  chunk->records = record.data + ChunkPreambleSize + chunk->compression.size();
}

mcap::Status McapReader::ParseMessageIndex(const mcap::Record& record,
                                           mcap::MessageIndex* messageIndex) {
  //
}

mcap::Status McapReader::ParseChunkIndex(const mcap::Record& record, mcap::ChunkIndex* chunkIndex) {
  //
}

mcap::Status McapReader::ParseAttachment(const mcap::Record& record, mcap::Attachment* attachment) {
  //
}

mcap::Status McapReader::ParseAttachmentIndex(const mcap::Record& record,
                                              mcap::AttachmentIndex* attachmentIndex) {
  //
}

mcap::Status McapReader::ParseStatistics(const mcap::Record& record, mcap::Statistics* statistics) {
  //
}

mcap::Status McapReader::ParseMetadata(const mcap::Record& record, mcap::Metadata* metadata) {
  //
}

mcap::Status McapReader::ParseMetadataIndex(const mcap::Record& record,
                                            mcap::MetadataIndex* metadataIndex) {
  //
}

mcap::Status McapReader::ParseSummaryOffset(const mcap::Record& record,
                                            mcap::SummaryOffset* summaryOffset) {
  //
}

mcap::Status McapReader::ParseDataEnd(const mcap::Record& record, mcap::DataEnd* dataEnd) {
  //
}

}  // namespace mcap
