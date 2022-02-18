#include <iostream>
#include <lz4.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zstd_errors.h>

namespace mcap {

// IWritable ///////////////////////////////////////////////////////////////////

inline void IWritable::write(const std::byte* data, uint64_t size) {
  handleWrite(data, size);
  if (crcEnabled) {
    crc_.Update(reinterpret_cast<const CryptoPP::byte*>(data), size);
  }
}

inline uint32_t IWritable::crc() {
  uint32_t crc32 = 0;
  if (crcEnabled) {
    crc_.Final(reinterpret_cast<CryptoPP::byte*>(&crc32));
  }
  return crc32;
}

inline void IWritable::resetCrc() {
  crc_ = {};
}

// StreamWriter ////////////////////////////////////////////////////////////////

inline StreamWriter::StreamWriter(std::ostream& stream)
    : stream_(stream)
    , size_(0) {}

inline void StreamWriter::handleWrite(const std::byte* data, uint64_t size) {
  stream_.write(reinterpret_cast<const char*>(data), std::streamsize(size));
  size_ += size;
}

inline void StreamWriter::end() {
  stream_.flush();
}

inline uint64_t StreamWriter::size() const {
  return size_;
}

// IChunkWriter ////////////////////////////////////////////////////////////////

inline void IChunkWriter::clear() {
  handleClear();
  resetCrc();
}

// BufferWriter //////////////////////////////////////////////////////////////

inline void BufferWriter::handleWrite(const std::byte* data, uint64_t size) {
  buffer_.insert(buffer_.end(), data, data + size);
}

inline void BufferWriter::end() {
  // no-op
}

inline uint64_t BufferWriter::size() const {
  return buffer_.size();
}

inline uint64_t BufferWriter::compressedSize() const {
  return buffer_.size();
}

inline bool BufferWriter::empty() const {
  return buffer_.empty();
}

inline void BufferWriter::handleClear() {
  buffer_.clear();
}

inline const std::byte* BufferWriter::data() const {
  return buffer_.data();
}

inline const std::byte* BufferWriter::compressedData() const {
  return buffer_.data();
}

// LZ4Writer ///////////////////////////////////////////////////////////////////

namespace internal {

inline int LZ4AccelerationLevel(CompressionLevel level) {
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

inline LZ4Writer::LZ4Writer(CompressionLevel compressionLevel, uint64_t chunkSize) {
  acceleration_ = internal::LZ4AccelerationLevel(compressionLevel);
  uncompressedBuffer_.reserve(chunkSize);
}

inline void LZ4Writer::handleWrite(const std::byte* data, uint64_t size) {
  uncompressedBuffer_.insert(uncompressedBuffer_.end(), data, data + size);
}

inline void LZ4Writer::end() {
  const auto dstCapacity = LZ4_compressBound(uncompressedBuffer_.size());
  compressedBuffer_.resize(dstCapacity);
  const int dstSize = LZ4_compress_fast(reinterpret_cast<const char*>(uncompressedBuffer_.data()),
                                        reinterpret_cast<char*>(compressedBuffer_.data()),
                                        uncompressedBuffer_.size(), dstCapacity, acceleration_);
  compressedBuffer_.resize(dstSize);
}

inline uint64_t LZ4Writer::size() const {
  return uncompressedBuffer_.size();
}

inline uint64_t LZ4Writer::compressedSize() const {
  return compressedBuffer_.size();
}

inline bool LZ4Writer::empty() const {
  return compressedBuffer_.empty() && uncompressedBuffer_.empty();
}

inline void LZ4Writer::handleClear() {
  uncompressedBuffer_.clear();
  compressedBuffer_.clear();
}

inline const std::byte* LZ4Writer::data() const {
  return uncompressedBuffer_.data();
}

inline const std::byte* LZ4Writer::compressedData() const {
  return compressedBuffer_.data();
}

// ZStdWriter //////////////////////////////////////////////////////////////////

namespace internal {

inline int ZStdCompressionLevel(CompressionLevel level) {
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

inline ZStdWriter::ZStdWriter(CompressionLevel compressionLevel, uint64_t chunkSize) {
  zstdContext_ = ZSTD_createCCtx();
  ZSTD_CCtx_setParameter(zstdContext_, ZSTD_c_compressionLevel,
                         internal::ZStdCompressionLevel(compressionLevel));
  uncompressedBuffer_.reserve(chunkSize);
}

inline ZStdWriter::~ZStdWriter() {
  ZSTD_freeCCtx(zstdContext_);
}

inline void ZStdWriter::handleWrite(const std::byte* data, uint64_t size) {
  uncompressedBuffer_.insert(uncompressedBuffer_.end(), data, data + size);
}

inline void ZStdWriter::end() {
  const auto dstCapacity = ZSTD_compressBound(uncompressedBuffer_.size());
  compressedBuffer_.resize(dstCapacity);
  const int dstSize = ZSTD_compress2(zstdContext_, compressedBuffer_.data(), dstCapacity,
                                     uncompressedBuffer_.data(), uncompressedBuffer_.size());
  if (ZSTD_isError(dstSize)) {
    const auto errCode = ZSTD_getErrorCode(dstSize);
    std::cerr << "ZSTD_compress2 failed: " << ZSTD_getErrorName(dstSize) << " ("
              << ZSTD_getErrorString(errCode) << ")\n";
    std::abort();
  }
  ZSTD_CCtx_reset(zstdContext_, ZSTD_reset_session_only);
  compressedBuffer_.resize(dstSize);
}

inline uint64_t ZStdWriter::size() const {
  return uncompressedBuffer_.size();
}

inline uint64_t ZStdWriter::compressedSize() const {
  return compressedBuffer_.size();
}

inline bool ZStdWriter::empty() const {
  return compressedBuffer_.empty() && uncompressedBuffer_.empty();
}

inline void ZStdWriter::handleClear() {
  uncompressedBuffer_.clear();
  compressedBuffer_.clear();
}

inline const std::byte* ZStdWriter::data() const {
  return uncompressedBuffer_.data();
}

inline const std::byte* ZStdWriter::compressedData() const {
  return compressedBuffer_.data();
}

// McapWriter //////////////////////////////////////////////////////////////////

inline McapWriter::~McapWriter() {
  close();
}

inline void McapWriter::open(IWritable& writer, const McapWriterOptions& options) {
  options_ = options;
  opened_ = true;
  chunkSize_ = options.noChunking ? 0 : options.chunkSize;
  compression_ = chunkSize_ > 0 ? options.compression : Compression::None;
  switch (compression_) {
    case Compression::None:
      uncompressedChunk_ = std::make_unique<BufferWriter>();
      break;
    case Compression::Lz4:
      lz4Chunk_ = std::make_unique<LZ4Writer>(options.compressionLevel, chunkSize_);
      break;
    case Compression::Zstd:
      zstdChunk_ = std::make_unique<ZStdWriter>(options.compressionLevel, chunkSize_);
      break;
  }
  auto* chunkWriter = getChunkWriter();
  if (chunkWriter) {
    chunkWriter->crcEnabled = !options.noCRC;
  }
  output_ = &writer;
  writeMagic(writer);
  write(writer, Header{options.profile, options.library});

  // FIXME: Write options.metadata
}

inline void McapWriter::open(std::ostream& stream, const McapWriterOptions& options) {
  streamOutput_ = std::make_unique<StreamWriter>(stream);
  open(*streamOutput_, options);
}

inline void McapWriter::close() {
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
  if (!options_.noCRC) {
    output_->crcEnabled = true;
    output_->resetCrc();
  }

  ByteOffset summaryStart = 0;
  ByteOffset summaryOffsetStart = 0;

  if (!options_.noSummary) {
    // Get the offset of the End Of File section
    summaryStart = fileOutput.size();

    ByteOffset schemaStart = fileOutput.size();
    if (!options_.noRepeatedSchemas) {
      // Write all schema records
      for (const auto& schema : schemas_) {
        write(fileOutput, schema);
      }
    }

    ByteOffset channelStart = fileOutput.size();
    if (!options_.noRepeatedChannels) {
      // Write all channel records
      for (const auto& channel : channels_) {
        write(fileOutput, channel);
      }
    }

    ByteOffset statisticsStart = fileOutput.size();
    if (!options_.noStatistics) {
      // Write the statistics record
      write(fileOutput, statistics_);
    }

    ByteOffset chunkIndexStart = fileOutput.size();
    if (!options_.noChunkIndex) {
      // Write chunk index records
      for (const auto& chunkIndexRecord : chunkIndex_) {
        write(fileOutput, chunkIndexRecord);
      }
    }

    ByteOffset attachmentIndexStart = fileOutput.size();
    if (!options_.noAttachmentIndex) {
      // Write attachment index records
      for (const auto& attachmentIndexRecord : attachmentIndex_) {
        write(fileOutput, attachmentIndexRecord);
      }
    }

    ByteOffset metadataIndexStart = fileOutput.size();
    if (!options_.noMetadataIndex) {
      // Write metadata index records
      for (const auto& metadataIndexRecord : metadataIndex_) {
        write(fileOutput, metadataIndexRecord);
      }
    }

    if (!options_.noSummaryOffsets) {
      // Write summary offset records
      summaryOffsetStart = fileOutput.size();
      if (!options_.noRepeatedSchemas && !schemas_.empty()) {
        write(fileOutput, SummaryOffset{OpCode::Schema, schemaStart, channelStart - schemaStart});
      }
      if (!options_.noRepeatedChannels && !channels_.empty()) {
        write(fileOutput,
              SummaryOffset{OpCode::Channel, channelStart, statisticsStart - channelStart});
      }
      if (!options_.noStatistics) {
        write(fileOutput, SummaryOffset{OpCode::Statistics, statisticsStart,
                                        chunkIndexStart - statisticsStart});
      }
      if (!options_.noChunkIndex && !chunkIndex_.empty()) {
        write(fileOutput, SummaryOffset{OpCode::ChunkIndex, chunkIndexStart,
                                        attachmentIndexStart - chunkIndexStart});
      }
      if (!options_.noAttachmentIndex && !attachmentIndex_.empty()) {
        write(fileOutput, SummaryOffset{OpCode::AttachmentIndex, attachmentIndexStart,
                                        metadataIndexStart - attachmentIndexStart});
      }
      if (!options_.noMetadataIndex && !metadataIndex_.empty()) {
        write(fileOutput, SummaryOffset{OpCode::MetadataIndex, metadataIndexStart,
                                        summaryOffsetStart - metadataIndexStart});
      }
    } else if (summaryStart == fileOutput.size()) {
      // No summary records were written
      summaryStart = 0;
    }
  }

  // Write the footer and trailing magic
  write(fileOutput, Footer{summaryStart, summaryOffsetStart}, !options_.noCRC);
  writeMagic(fileOutput);

  // Flush output
  fileOutput.end();

  terminate();
}

inline void McapWriter::terminate() {
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
  currentChunkStart_ = MaxTime;
  currentChunkEnd_ = 0;

  opened_ = false;
}

inline void McapWriter::addSchema(Schema& schema) {
  schema.id = uint16_t(schemas_.size() + 1);
  schemas_.push_back(schema);
}

inline void McapWriter::addChannel(Channel& channel) {
  channel.id = uint16_t(channels_.size() + 1);
  channels_.push_back(channel);
}

inline Status McapWriter::write(const Message& message) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& output = getOutput();
  auto& channelMessageCounts = statistics_.channelMessageCounts;

  // Write out Channel if we have not yet done so
  if (channelMessageCounts.find(message.channelId) == channelMessageCounts.end()) {
    const size_t channelIndex = message.channelId - 1;
    if (channelIndex >= channels_.size()) {
      const auto msg = StrFormat("invalid channel id {}", message.channelId);
      return Status{StatusCode::InvalidChannelId, msg};
    }

    const auto& channel = channels_[channelIndex];

    // Check if the Schema record needs to be written
    if (writtenSchemas_.find(channel.schemaId) == writtenSchemas_.end()) {
      const size_t schemaIndex = channel.schemaId - 1;
      if (schemaIndex >= schemas_.size()) {
        const auto msg = StrFormat("invalid schema id {}", channel.schemaId);
        return Status{StatusCode::InvalidSchemaId, msg};
      }

      // Write the Schema record
      uncompressedSize_ += write(output, schemas_[schemaIndex]);
      writtenSchemas_.insert(channel.schemaId);

      // Update schema statistics
      ++statistics_.schemaCount;
    }

    // Write the Channel record
    uncompressedSize_ += write(output, channel);

    // Update channel statistics
    channelMessageCounts.emplace(message.channelId, 0);
    ++statistics_.channelCount;
  }

  const uint64_t messageOffset = uncompressedSize_;

  // Write the message
  uncompressedSize_ += write(output, message);

  // Update message statistics
  if (!options_.noSummary) {
    if (statistics_.messageCount == 0) {
      statistics_.messageStartTime = message.logTime;
      statistics_.messageEndTime = message.logTime;
    } else {
      statistics_.messageStartTime = std::min(statistics_.messageStartTime, message.logTime);
      statistics_.messageEndTime = std::max(statistics_.messageEndTime, message.logTime);
    }
    ++statistics_.messageCount;
    channelMessageCounts[message.channelId] += 1;
  }

  auto* chunkWriter = getChunkWriter();
  if (chunkWriter) {
    if (!options_.noMessageIndex) {
      // Update the message index
      auto& messageIndex = currentMessageIndex_[message.channelId];
      messageIndex.channelId = message.channelId;
      messageIndex.records.emplace_back(message.logTime, messageOffset);
    }

    // Update the chunk index start/end times
    currentChunkStart_ = std::min(currentChunkStart_, message.logTime);
    currentChunkEnd_ = std::max(currentChunkEnd_, message.logTime);

    // Check if the current chunk is ready to close
    if (uncompressedSize_ >= chunkSize_) {
      auto& fileOutput = *output_;
      writeChunk(fileOutput, *chunkWriter);
    }
  }

  return StatusCode::Success;
}

inline Status McapWriter::write(Attachment& attachment) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& fileOutput = *output_;

  // Check if we have an open chunk that needs to be closed
  auto* chunkWriter = getChunkWriter();
  if (chunkWriter && !chunkWriter->empty()) {
    writeChunk(fileOutput, *chunkWriter);
  }

  if (!options_.noCRC) {
    // Calculate the CRC32 of the attachment
    uint32_t sizePrefix = 0;
    CryptoPP::CRC32 crc;
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(&attachment.logTime), 8);
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(&attachment.createTime), 8);
    sizePrefix = uint32_t(attachment.name.size());
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(&sizePrefix), 4);
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(attachment.name.data()), sizePrefix);
    sizePrefix = uint32_t(attachment.contentType.size());
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(&sizePrefix), 4);
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(attachment.contentType.data()), sizePrefix);
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(&attachment.dataSize), 8);
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(attachment.data), attachment.dataSize);
    crc.Final(reinterpret_cast<CryptoPP::byte*>(&attachment.crc));
  }

  const uint64_t fileOffset = fileOutput.size();

  // Write the attachment
  write(fileOutput, attachment);

  // Update statistics and attachment index
  if (!options_.noSummary) {
    ++statistics_.attachmentCount;
    if (!options_.noAttachmentIndex) {
      attachmentIndex_.emplace_back(attachment, fileOffset);
    }
  }

  return StatusCode::Success;
}

inline Status McapWriter::write(const Metadata& metadata) {
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
  if (!options_.noSummary) {
    ++statistics_.metadataCount;
    if (!options_.noMetadataIndex) {
      metadataIndex_.emplace_back(metadata, fileOffset);
    }
  }

  return StatusCode::Success;
}

// Private methods /////////////////////////////////////////////////////////////

inline IWritable& McapWriter::getOutput() {
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

inline IChunkWriter* McapWriter::getChunkWriter() {
  if (chunkSize_ == 0) {
    return nullptr;
  }

  switch (compression_) {
    case Compression::None:
      return uncompressedChunk_.get();
    case Compression::Lz4:
      return lz4Chunk_.get();
    case Compression::Zstd:
      return zstdChunk_.get();
  }
}

inline void McapWriter::writeChunk(IWritable& output, IChunkWriter& chunkData) {
  // Both LZ4 and ZSTD recommend ~1KB as the minimum size for compressed data
  constexpr uint64_t MIN_COMPRESSION_SIZE = 1024;
  // Throw away any compression results that save less than 2% of the original size
  constexpr double MIN_COMPRESSION_RATIO = 1.02;

  Compression compression = Compression::None;
  const uint64_t uncompressedSize = uncompressedSize_;
  uint64_t compressedSize = uncompressedSize;
  const std::byte* compressedData = chunkData.data();

  if (options_.forceCompression || uncompressedSize >= MIN_COMPRESSION_SIZE) {
    // Flush any in-progress compression stream
    chunkData.end();

    // Only use the compressed data if it is materially smaller than the
    // uncompressed data
    const double compressionRatio = double(uncompressedSize) / double(chunkData.compressedSize());
    if (options_.forceCompression || compressionRatio >= MIN_COMPRESSION_RATIO) {
      compression = compression_;
      compressedSize = chunkData.compressedSize();
      compressedData = chunkData.compressedData();
    }
  }

  const auto compressionStr = internal::CompressionString(compression);
  const uint32_t uncompressedCrc = chunkData.crc();

  // Write the chunk
  const uint64_t chunkStartOffset = output.size();
  write(output, Chunk{currentChunkStart_, currentChunkEnd_, uncompressedSize, uncompressedCrc,
                      compressionStr, compressedSize, compressedData});

  const uint64_t chunkLength = output.size() - chunkStartOffset;

  if (!options_.noChunkIndex) {
    // Create a chunk index record
    auto& chunkIndexRecord = chunkIndex_.emplace_back();

    const uint64_t messageIndexOffset = output.size();
    if (!options_.noMessageIndex) {
      // Write the message index records
      for (const auto& [channelId, messageIndex] : currentMessageIndex_) {
        chunkIndexRecord.messageIndexOffsets.emplace(channelId, output.size());
        write(output, messageIndex);
      }
      currentMessageIndex_.clear();
    }
    const uint64_t messageIndexLength = output.size() - messageIndexOffset;

    // Fill in the newly created chunk index record. This will be written into
    // the summary section when close() is called
    chunkIndexRecord.messageStartTime = currentChunkStart_;
    chunkIndexRecord.messageEndTime = currentChunkEnd_;
    chunkIndexRecord.chunkStartOffset = chunkStartOffset;
    chunkIndexRecord.chunkLength = chunkLength;
    chunkIndexRecord.messageIndexLength = messageIndexLength;
    chunkIndexRecord.compression = compressionStr;
    chunkIndexRecord.compressedSize = compressedSize;
    chunkIndexRecord.uncompressedSize = uncompressedSize;
  } else if (!options_.noMessageIndex) {
    // Write the message index records
    for (const auto& [channelId, messageIndex] : currentMessageIndex_) {
      write(output, messageIndex);
    }
    currentMessageIndex_.clear();
  }

  // Reset uncompressedSize and start/end times for the next chunk
  uncompressedSize_ = 0;
  currentChunkStart_ = MaxTime;
  currentChunkEnd_ = 0;

  // Update statistics
  ++statistics_.chunkCount;

  // Reset the chunk writer
  chunkData.clear();
}

inline void McapWriter::writeMagic(IWritable& output) {
  write(output, reinterpret_cast<const std::byte*>(Magic), sizeof(Magic));
}

inline uint64_t McapWriter::write(IWritable& output, const Header& header) {
  const uint64_t recordSize = 4 + header.profile.size() + 4 + header.library.size();

  write(output, OpCode::Header);
  write(output, recordSize);
  write(output, header.profile);
  write(output, header.library);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Footer& footer, const bool crcEnabled) {
  const uint64_t recordSize = /* summary_start */ 8 +
                              /* summary_offset_start */ 8 +
                              /* summary_crc */ 4;

  write(output, OpCode::Footer);
  write(output, recordSize);
  write(output, footer.summaryStart);
  write(output, footer.summaryOffsetStart);
  uint32_t summaryCrc = 0;
  if (footer.summaryStart != 0 && crcEnabled) {
    summaryCrc = output.crc();
  }
  write(output, summaryCrc);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Schema& schema) {
  const uint64_t recordSize = /* id */ 2 +
                              /* name */ 4 + schema.name.size() +
                              /* encoding */ 4 + schema.encoding.size() +
                              /* data */ 4 + schema.data.size();

  write(output, OpCode::Schema);
  write(output, recordSize);
  write(output, schema.id);
  write(output, schema.name);
  write(output, schema.encoding);
  write(output, schema.data);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Channel& channel) {
  const uint32_t metadataSize = internal::KeyValueMapSize(channel.metadata);
  const uint64_t recordSize = /* id */ 2 +
                              /* topic */ 4 + channel.topic.size() +
                              /* message_encoding */ 4 + channel.messageEncoding.size() +
                              /* schema_id */ 2 +
                              /* metadata */ 4 + metadataSize;

  write(output, OpCode::Channel);
  write(output, recordSize);
  write(output, channel.id);
  write(output, channel.schemaId);
  write(output, channel.topic);
  write(output, channel.messageEncoding);
  write(output, channel.metadata, metadataSize);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Message& message) {
  const uint64_t recordSize = 2 + 4 + 8 + 8 + message.dataSize;

  write(output, OpCode::Message);
  write(output, recordSize);
  write(output, message.channelId);
  write(output, message.sequence);
  write(output, message.logTime);
  write(output, message.publishTime);
  write(output, message.data, message.dataSize);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Attachment& attachment) {
  const uint64_t recordSize = 4 + attachment.name.size() + 8 + 8 + 4 +
                              attachment.contentType.size() + 8 + attachment.dataSize + 4;

  write(output, OpCode::Attachment);
  write(output, recordSize);
  write(output, attachment.logTime);
  write(output, attachment.createTime);
  write(output, attachment.name);
  write(output, attachment.contentType);
  write(output, attachment.dataSize);
  write(output, attachment.data, attachment.dataSize);
  write(output, attachment.crc);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Metadata& metadata) {
  const uint32_t metadataSize = internal::KeyValueMapSize(metadata.metadata);
  const uint64_t recordSize = 4 + metadata.name.size() + 4 + metadataSize;

  write(output, OpCode::Metadata);
  write(output, recordSize);
  write(output, metadata.name);
  write(output, metadata.metadata, metadataSize);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Chunk& chunk) {
  const uint64_t recordSize =
    8 + 8 + 8 + 4 + 4 + chunk.compression.size() + 8 + chunk.compressedSize;

  write(output, OpCode::Chunk);
  write(output, recordSize);
  write(output, chunk.messageStartTime);
  write(output, chunk.messageEndTime);
  write(output, chunk.uncompressedSize);
  write(output, chunk.uncompressedCrc);
  write(output, chunk.compression);
  write(output, chunk.compressedSize);
  write(output, chunk.records, chunk.compressedSize);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const MessageIndex& index) {
  const uint32_t recordsSize = index.records.size() * 16;
  const uint64_t recordSize = 2 + 4 + recordsSize;

  write(output, OpCode::MessageIndex);
  write(output, recordSize);
  write(output, index.channelId);

  write(output, recordsSize);
  for (const auto& [timestamp, offset] : index.records) {
    write(output, timestamp);
    write(output, offset);
  }

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const ChunkIndex& index) {
  const uint32_t messageIndexOffsetsSize = index.messageIndexOffsets.size() * 10;
  const uint64_t recordSize = /* start_time */ 8 +
                              /* end_time */ 8 +
                              /* chunk_start_offset */ 8 +
                              /* chunk_length */ 8 +
                              /* message_index_offsets */ 4 + messageIndexOffsetsSize +
                              /* message_index_length */ 8 +
                              /* compression */ 4 + index.compression.size() +
                              /* compressed_size */ 8 +
                              /* uncompressed_size */ 8;

  write(output, OpCode::ChunkIndex);
  write(output, recordSize);
  write(output, index.messageStartTime);
  write(output, index.messageEndTime);
  write(output, index.chunkStartOffset);
  write(output, index.chunkLength);

  write(output, messageIndexOffsetsSize);
  for (const auto& [channelId, offset] : index.messageIndexOffsets) {
    write(output, channelId);
    write(output, offset);
  }

  write(output, index.messageIndexLength);
  write(output, index.compression);
  write(output, index.compressedSize);
  write(output, index.uncompressedSize);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const AttachmentIndex& index) {
  const uint64_t recordSize = /* offset */ 8 +
                              /* length */ 8 +
                              /* log_time */ 8 +
                              /* create_time */ 8 +
                              /* data_size */ 8 +
                              /* name */ 4 + index.name.size() +
                              /* content_type */ 4 + index.contentType.size();

  write(output, OpCode::AttachmentIndex);
  write(output, recordSize);
  write(output, index.offset);
  write(output, index.length);
  write(output, index.logTime);
  write(output, index.createTime);
  write(output, index.dataSize);
  write(output, index.name);
  write(output, index.contentType);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const MetadataIndex& index) {
  const uint64_t recordSize = /* offset */ 8 +
                              /* length */ 8 +
                              /* name */ 4 + index.name.size();

  write(output, OpCode::MetadataIndex);
  write(output, recordSize);
  write(output, index.offset);
  write(output, index.length);
  write(output, index.name);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Statistics& stats) {
  const uint32_t channelMessageCountsSize = stats.channelMessageCounts.size() * 10;
  const uint64_t recordSize = /* message_count */ 8 +
                              /* schema_count */ 2 +
                              /* channel_count */ 4 +
                              /* attachment_count */ 4 +
                              /* metadata_count */ 4 +
                              /* chunk_count */ 4 +
                              /* message_start_time */ 8 +
                              /* message_end_time */ 8 +
                              /* channel_message_counts */ 4 + channelMessageCountsSize;

  write(output, OpCode::Statistics);
  write(output, recordSize);
  write(output, stats.messageCount);
  write(output, stats.schemaCount);
  write(output, stats.channelCount);
  write(output, stats.attachmentCount);
  write(output, stats.metadataCount);
  write(output, stats.chunkCount);
  write(output, stats.messageStartTime);
  write(output, stats.messageEndTime);

  write(output, channelMessageCountsSize);
  for (const auto& [channelId, messageCount] : stats.channelMessageCounts) {
    write(output, channelId);
    write(output, messageCount);
  }

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const SummaryOffset& summaryOffset) {
  const uint64_t recordSize = /* group_opcode */ 1 +
                              /* group_start */ 8 +
                              /* group_length */ 8;

  write(output, OpCode::SummaryOffset);
  write(output, recordSize);
  write(output, summaryOffset.groupOpCode);
  write(output, summaryOffset.groupStart);
  write(output, summaryOffset.groupLength);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const DataEnd& dataEnd) {
  const uint64_t recordSize = /* data_section_crc */ 4;

  write(output, OpCode::DataEnd);
  write(output, recordSize);
  write(output, dataEnd.dataSectionCrc);

  return 9 + recordSize;
}

inline uint64_t McapWriter::write(IWritable& output, const Record& record) {
  write(output, OpCode(record.opcode));
  write(output, record.dataSize);
  write(output, record.data, record.dataSize);

  return 9 + record.dataSize;
}

inline void McapWriter::write(IWritable& output, const std::string_view str) {
  write(output, uint32_t(str.size()));
  output.write(reinterpret_cast<const std::byte*>(str.data()), str.size());
}

inline void McapWriter::write(IWritable& output, const ByteArray bytes) {
  write(output, uint32_t(bytes.size()));
  output.write(bytes.data(), bytes.size());
}

inline void McapWriter::write(IWritable& output, OpCode value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

inline void McapWriter::write(IWritable& output, uint16_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

inline void McapWriter::write(IWritable& output, uint32_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

inline void McapWriter::write(IWritable& output, uint64_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

inline void McapWriter::write(IWritable& output, const std::byte* data, uint64_t size) {
  output.write(reinterpret_cast<const std::byte*>(data), size);
}

inline void McapWriter::write(IWritable& output, const KeyValueMap& map, uint32_t size) {
  // Create a vector of key-value pairs so we can lexicographically sort by key
  std::vector<std::pair<std::string, std::string>> pairs;
  pairs.reserve(map.size());
  for (const auto& [key, value] : map) {
    pairs.emplace_back(key, value);
  }
  std::sort(pairs.begin(), pairs.end());

  write(output, size > 0 ? size : internal::KeyValueMapSize(map));
  for (const auto& [key, value] : pairs) {
    write(output, key);
    write(output, value);
  }
}

}  // namespace mcap
