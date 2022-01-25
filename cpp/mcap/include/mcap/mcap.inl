// Do not compile on systems with non-8-bit bytes
static_assert(std::numeric_limits<unsigned char>::digits == 8);

namespace mcap {

// Internal methods ////////////////////////////////////////////////////////////

namespace internal {

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

}  // namespace internal

// Public API //////////////////////////////////////////////////////////////////

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
  write(writer, Header{options.profile, options.library, options.metadata});
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

  uint64_t indexOffset = 0;
  uint32_t indexCrc = 0;

  if (indexing_) {
    // Get the offset of the End Of File section
    indexOffset = fileOutput.size();

    // Write all channel info records
    for (const auto& channel : channels_) {
      write(fileOutput, channel);
    }

    // Write chunk index records
    for (const auto& chunkIndexRecord : chunkIndex_) {
      write(fileOutput, chunkIndexRecord);
    }

    // Write attachment index records
    for (const auto& attachmentIndexRecord : attachmentIndex_) {
      write(fileOutput, attachmentIndexRecord);
    }

    // Write the statistics record
    write(fileOutput, statistics_);
  }

  // TODO: Calculate the index CRC

  // Write the footer and trailing magic
  write(fileOutput, mcap::Footer{indexOffset, indexCrc});
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
  chunkIndex_.clear();
  statistics_ = {};
  currentMessageIndex_.clear();
  currentChunkStart_ = std::numeric_limits<uint64_t>::max();
  currentChunkEnd_ = std::numeric_limits<uint64_t>::min();

  opened_ = false;
}

void McapWriter::addChannel(mcap::ChannelInfo& info) {
  info.channelId = uint16_t(channels_.size() + 1);
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
  const uint64_t chunkOffset = output.size();
  write(output, Chunk{uncompressedSize_, uncompressedCrc, compression, compressedSize, data});

  if (indexing_) {
    // Update statistics
    const uint64_t chunkSize = output.size() - chunkOffset;
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

    chunkIndexRecord.startTime = currentChunkStart_;
    chunkIndexRecord.endTime = currentChunkEnd_;
    chunkIndexRecord.chunkOffset = chunkOffset;
    chunkIndexRecord.messageIndexLength = messageIndexLength;
    chunkIndexRecord.compression = compression;
    chunkIndexRecord.compressedSize = compressedSize;
    chunkIndexRecord.uncompressedSized = uncompressedSize_;
    chunkIndexRecord.crc = 0;

    // Reset uncompressedSize and start/end times for the next chunk
    uncompressedSize_ = 0;
    currentChunkStart_ = std::numeric_limits<uint64_t>::max();
    currentChunkEnd_ = std::numeric_limits<uint64_t>::min();
  }

  chunkData.clear();
}

void McapWriter::writeMagic(mcap::IWritable& output) {
  write(output, reinterpret_cast<const std::byte*>(Magic), sizeof(Magic));
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Header& header) {
  const uint32_t metadataSize = internal::KeyValueMapSize(header.metadata);
  const uint64_t recordSize =
    4 + header.profile.size() + 4 + header.library.size() + 4 + metadataSize;

  write(output, OpCode::Header);
  write(output, recordSize);
  write(output, header.profile);
  write(output, header.library);
  write(output, header.metadata, metadataSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::Footer& footer) {
  const uint64_t recordSize = 12;

  write(output, OpCode::Footer);
  write(output, recordSize);
  write(output, footer.indexOffset);
  write(output, footer.indexCrc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(mcap::IWritable& output, const mcap::ChannelInfo& info) {
  const uint32_t userDataSize = internal::KeyValueMapSize(info.userData);
  const uint64_t recordSize = 2 + 4 + info.topicName.size() + 4 + info.encoding.size() + 4 +
                              info.schemaName.size() + 4 + info.schema.size() + 4 + userDataSize +
                              4;
  const uint32_t crc = 0;

  write(output, OpCode::ChannelInfo);
  write(output, recordSize);
  write(output, info.channelId);
  write(output, info.topicName);
  write(output, info.encoding);
  write(output, info.schemaName);
  write(output, info.schema);
  write(output, info.userData, userDataSize);
  write(output, crc);

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
  write(output, index.chunkOffset);

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

}  // namespace mcap
