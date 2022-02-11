// Do not compile on systems with non-8-bit bytes
static_assert(std::numeric_limits<unsigned char>::digits == 8);

namespace mcap {

constexpr std::string_view OpCodeString(OpCode opcode) {
  switch (opcode) {
    case OpCode::Header:
      return "Header";
    case OpCode::Footer:
      return "Footer";
    case OpCode::Schema:
      return "Schema";
    case OpCode::Channel:
      return "Channel";
    case OpCode::Message:
      return "Message";
    case OpCode::Chunk:
      return "Chunk";
    case OpCode::MessageIndex:
      return "MessageIndex";
    case OpCode::ChunkIndex:
      return "ChunkIndex";
    case OpCode::Attachment:
      return "Attachment";
    case OpCode::AttachmentIndex:
      return "AttachmentIndex";
    case OpCode::Statistics:
      return "Statistics";
    case OpCode::Metadata:
      return "Metadata";
    case OpCode::MetadataIndex:
      return "MetadataIndex";
    case OpCode::SummaryOffset:
      return "SummaryOffset";
    case OpCode::DataEnd:
      return "DataEnd";
    default:
      return "Unknown";
  }
}

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

uint16_t ParseUint16(const std::byte* data) {
  return uint16_t(data[0]) | (uint16_t(data[1]) << 8);
}

uint32_t ParseUint32(const std::byte* data) {
  return uint32_t(data[0]) | (uint32_t(data[1]) << 8) | (uint32_t(data[2]) << 16) |
         (uint32_t(data[3]) << 24);
}

Status ParseUint32(const std::byte* data, uint64_t maxSize, uint32_t* output) {
  if (maxSize < 4) {
    const auto msg = StrFormat("cannot read uint32 from {} bytes", maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  *output = ParseUint32(data);
  return StatusCode::Success;
}

uint64_t ParseUint64(const std::byte* data) {
  return uint64_t(data[0]) | (uint64_t(data[1]) << 8) | (uint64_t(data[2]) << 16) |
         (uint64_t(data[3]) << 24) | (uint64_t(data[4]) << 32) | (uint64_t(data[5]) << 40) |
         (uint64_t(data[6]) << 48) | (uint64_t(data[7]) << 56);
}

Status ParseUint64(const std::byte* data, uint64_t maxSize, uint64_t* output) {
  if (maxSize < 8) {
    const auto msg = StrFormat("cannot read uint64 from {} bytes", maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  *output = ParseUint64(data);
  return StatusCode::Success;
}

Status ParseString(const std::byte* data, uint64_t maxSize, std::string_view* output) {
  uint32_t size;
  if (auto status = ParseUint32(data, maxSize, &size); !status.ok()) {
    const auto msg = StrFormat("cannot read string size: {}", status.message);
    return Status{StatusCode::InvalidRecord, msg};
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg = StrFormat("string size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }
  *output = std::string_view(reinterpret_cast<const char*>(data + 4), size);
  return StatusCode::Success;
}

Status ParseString(const std::byte* data, uint64_t maxSize, std::string* output) {
  uint32_t size;
  if (auto status = ParseUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg = StrFormat("string size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }
  *output = std::string(reinterpret_cast<const char*>(data + 4), size);
  return StatusCode::Success;
}

Status ParseByteArray(const std::byte* data, uint64_t maxSize, ByteArray* output) {
  uint32_t size;
  if (auto status = ParseUint32(data, maxSize, &size); !status.ok()) {
    return status;
  }
  if (uint64_t(size) > (maxSize - 4)) {
    const auto msg =
      StrFormat("byte array size {} exceeds remaining bytes {}", size, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }
  output->resize(size);
  std::memcpy(output->data(), data + 4, size);
  return StatusCode::Success;
}

Status ParseKeyValueMap(const std::byte* data, uint64_t maxSize, KeyValueMap* output) {
  uint32_t sizeInBytes;
  if (auto status = ParseUint32(data, maxSize, &sizeInBytes); !status.ok()) {
    return status;
  }
  if (sizeInBytes > (maxSize - 4)) {
    const auto msg =
      StrFormat("key-value map size {} exceeds remaining bytes {}", sizeInBytes, (maxSize - 4));
    return Status(StatusCode::InvalidRecord, msg);
  }

  // Account for the byte size prefix in sizeInBytes to make the bounds checking
  // below simpler
  sizeInBytes += 4;

  output->clear();
  uint64_t pos = 4;
  while (pos < sizeInBytes) {
    std::string_view key;
    if (auto status = ParseString(data + pos, sizeInBytes - pos, &key); !status.ok()) {
      const auto msg =
        StrFormat("cannot read key-value map key at pos {}: {}", pos, status.message);
      return Status{StatusCode::InvalidRecord, msg};
    }
    pos += 4 + key.size();
    std::string_view value;
    if (auto status = ParseString(data + pos, sizeInBytes - pos, &value); !status.ok()) {
      const auto msg = StrFormat("cannot read key-value map value for key \"{}\" at pos {}: {}",
                                 key, pos, status.message);
      return Status{StatusCode::InvalidRecord, msg};
    }
    pos += 4 + value.size();
    output->emplace(key, value);
  }
  return StatusCode::Success;
}

std::string MagicToHex(const std::byte* data) {
  return StrFormat("{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", data[0], data[1], data[2],
                   data[3], data[4], data[5], data[6], data[7]);
}

}  // namespace internal

MetadataIndex::MetadataIndex(const Metadata& metadata, ByteOffset fileOffset)
    : offset(fileOffset)
    , length(9 + 4 + metadata.name.size() + 4 + internal::KeyValueMapSize(metadata.metadata))
    , name(metadata.name) {}

// McapWriter //////////////////////////////////////////////////////////////////

McapWriter::~McapWriter() {
  close();
}

void McapWriter::open(IWritable& writer, const McapWriterOptions& options) {
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
  getChunkWriter()->crcEnabled = !options.noCRC;
  output_ = &writer;
  writeMagic(writer);
  write(writer, Header{options.profile, options.library});

  // FIXME: Write options.metadata
}

void McapWriter::open(std::ostream& stream, const McapWriterOptions& options) {
  streamOutput_ = std::make_unique<StreamWriter>(stream);
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

  ByteOffset summaryStart = 0;
  ByteOffset summaryOffsetStart = 0;
  uint32_t summaryCrc = 0;

  if (!options_.noSummary) {
    // Get the offset of the End Of File section
    summaryStart = fileOutput.size();

    // Write all schema records
    ByteOffset schemaStart = fileOutput.size();
    for (const auto& schema : schemas_) {
      write(fileOutput, schema);
    }

    // Write all channel records
    ByteOffset channelStart = fileOutput.size();
    for (const auto& channel : channels_) {
      write(fileOutput, channel);
    }

    // Write chunk index records
    ByteOffset chunkIndexStart = fileOutput.size();
    for (const auto& chunkIndexRecord : chunkIndex_) {
      write(fileOutput, chunkIndexRecord);
    }

    // Write attachment index records
    ByteOffset attachmentIndexStart = fileOutput.size();
    for (const auto& attachmentIndexRecord : attachmentIndex_) {
      write(fileOutput, attachmentIndexRecord);
    }

    // Write metadata index records
    ByteOffset metadataIndexStart = fileOutput.size();
    for (const auto& metadataIndexRecord : metadataIndex_) {
      write(fileOutput, metadataIndexRecord);
    }

    // Write the statistics record
    ByteOffset statisticsStart = fileOutput.size();
    write(fileOutput, statistics_);

    // Write summary offset records
    summaryOffsetStart = fileOutput.size();
    if (!schemas_.empty()) {
      write(fileOutput, SummaryOffset{OpCode::Schema, schemaStart, channelStart - schemaStart});
    }
    if (!channels_.empty()) {
      write(fileOutput,
            SummaryOffset{OpCode::Channel, channelStart, chunkIndexStart - channelStart});
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
  write(fileOutput, Footer{summaryStart, summaryOffsetStart, summaryCrc});
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
  currentChunkStart_ = MaxTime;
  currentChunkEnd_ = 0;

  opened_ = false;
}

void McapWriter::addSchema(Schema& schema) {
  schema.id = uint16_t(schemas_.size() + 1);
  schemas_.push_back(schema);
}

void McapWriter::addChannel(Channel& channel) {
  channel.id = uint16_t(channels_.size() + 1);
  channels_.push_back(channel);
}

Status McapWriter::write(const Message& message) {
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
    if (!options_.noSummary) {
      // Update the message index
      auto& messageIndex = currentMessageIndex_[message.channelId];
      messageIndex.channelId = message.channelId;
      messageIndex.records.emplace_back(message.logTime, messageOffset);

      // Update the chunk index start/end times
      currentChunkStart_ = std::min(currentChunkStart_, message.logTime);
      currentChunkEnd_ = std::max(currentChunkEnd_, message.logTime);
    }

    // Check if the current chunk is ready to close
    if (uncompressedSize_ >= chunkSize_) {
      auto& fileOutput = *output_;
      writeChunk(fileOutput, *chunkWriter);
    }
  }

  return StatusCode::Success;
}

Status McapWriter::write(Attachment& attachment) {
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
    CryptoPP::CRC32 crc;
    crc.Update(reinterpret_cast<const CryptoPP::byte*>(attachment.data), attachment.dataSize);
    crc.Final(reinterpret_cast<CryptoPP::byte*>(&attachment.crc));
  }

  const uint64_t fileOffset = fileOutput.size();

  // Write the attachment
  write(fileOutput, attachment);

  // Update statistics and attachment index
  if (!options_.noSummary) {
    ++statistics_.attachmentCount;
    attachmentIndex_.emplace_back(attachment, fileOffset);
  }

  return StatusCode::Success;
}

Status McapWriter::write(const Metadata& metadata) {
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
    metadataIndex_.emplace_back(metadata, fileOffset);
  }

  return StatusCode::Success;
}

// Private methods /////////////////////////////////////////////////////////////

IWritable& McapWriter::getOutput() {
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

IChunkWriter* McapWriter::getChunkWriter() {
  switch (compression_) {
    case Compression::None:
      return uncompressedChunk_.get();
    case Compression::Lz4:
      return lz4Chunk_.get();
    case Compression::Zstd:
      return zstdChunk_.get();
  }
}

void McapWriter::writeChunk(IWritable& output, IChunkWriter& chunkData) {
  const auto& compression = internal::CompressionString(compression_);

  // Flush any in-progress compression stream
  chunkData.end();

  const uint64_t compressedSize = chunkData.size();
  const std::byte* records = chunkData.data();
  const uint32_t uncompressedCrc = chunkData.crc();

  // Write the chunk
  const uint64_t chunkStartOffset = output.size();
  write(output, Chunk{currentChunkStart_, currentChunkEnd_, uncompressedSize_, uncompressedCrc,
                      compression, compressedSize, records});

  if (!options_.noSummary) {
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
    chunkIndexRecord.messageStartTime = currentChunkStart_;
    chunkIndexRecord.messageEndTime = currentChunkEnd_;
    chunkIndexRecord.chunkStartOffset = chunkStartOffset;
    chunkIndexRecord.chunkLength = chunkLength;
    chunkIndexRecord.messageIndexLength = messageIndexLength;
    chunkIndexRecord.compression = compression;
    chunkIndexRecord.compressedSize = compressedSize;
    chunkIndexRecord.uncompressedSize = uncompressedSize_;

    // Reset uncompressedSize and start/end times for the next chunk
    uncompressedSize_ = 0;
    currentChunkStart_ = MaxTime;
    currentChunkEnd_ = 0;
  }

  // Reset the chunk writer
  chunkData.clear();
}

void McapWriter::writeMagic(IWritable& output) {
  write(output, reinterpret_cast<const std::byte*>(Magic), sizeof(Magic));
}

uint64_t McapWriter::write(IWritable& output, const Header& header) {
  const uint64_t recordSize = 4 + header.profile.size() + 4 + header.library.size();

  write(output, OpCode::Header);
  write(output, recordSize);
  write(output, header.profile);
  write(output, header.library);

  return 9 + recordSize;
}

uint64_t McapWriter::write(IWritable& output, const Footer& footer) {
  const uint64_t recordSize = /* summary_start */ 8 +
                              /* summary_offset_start */ 8 +
                              /* summary_crc */ 4;

  write(output, OpCode::Footer);
  write(output, recordSize);
  write(output, footer.summaryStart);
  write(output, footer.summaryOffsetStart);
  write(output, footer.summaryCrc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(IWritable& output, const Schema& schema) {
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

uint64_t McapWriter::write(IWritable& output, const Channel& channel) {
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

uint64_t McapWriter::write(IWritable& output, const Message& message) {
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

uint64_t McapWriter::write(IWritable& output, const Attachment& attachment) {
  const uint64_t recordSize = 4 + attachment.name.size() + 8 + 8 + 4 +
                              attachment.contentType.size() + 8 + attachment.dataSize + 4;

  write(output, OpCode::Attachment);
  write(output, recordSize);
  write(output, attachment.name);
  write(output, attachment.logTime);
  write(output, attachment.createTime);
  write(output, attachment.contentType);
  write(output, attachment.dataSize);
  write(output, attachment.data, attachment.dataSize);
  write(output, attachment.crc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(IWritable& output, const Metadata& metadata) {
  const uint32_t metadataSize = internal::KeyValueMapSize(metadata.metadata);
  const uint64_t recordSize = 4 + metadata.name.size() + 4 + metadataSize;

  write(output, OpCode::Metadata);
  write(output, recordSize);
  write(output, metadata.name);
  write(output, metadata.metadata, metadataSize);

  return 9 + recordSize;
}

uint64_t McapWriter::write(IWritable& output, const Chunk& chunk) {
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

uint64_t McapWriter::write(IWritable& output, const MessageIndex& index) {
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

uint64_t McapWriter::write(IWritable& output, const ChunkIndex& index) {
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

uint64_t McapWriter::write(IWritable& output, const AttachmentIndex& index) {
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

uint64_t McapWriter::write(IWritable& output, const MetadataIndex& index) {
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

uint64_t McapWriter::write(IWritable& output, const Statistics& stats) {
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

uint64_t McapWriter::write(IWritable& output, const SummaryOffset& summaryOffset) {
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

uint64_t McapWriter::write(IWritable& output, const DataEnd& dataEnd) {
  const uint64_t recordSize = /* data_section_crc */ 4;

  write(output, OpCode::DataEnd);
  write(output, recordSize);
  write(output, dataEnd.dataSectionCrc);

  return 9 + recordSize;
}

uint64_t McapWriter::write(IWritable& output, const Record& record) {
  write(output, OpCode(record.opcode));
  write(output, record.dataSize);
  write(output, record.data, record.dataSize);

  return 9 + record.dataSize;
}

void McapWriter::write(IWritable& output, const std::string_view str) {
  write(output, uint32_t(str.size()));
  output.write(reinterpret_cast<const std::byte*>(str.data()), str.size());
}

void McapWriter::write(IWritable& output, const ByteArray bytes) {
  write(output, uint32_t(bytes.size()));
  output.write(bytes.data(), bytes.size());
}

void McapWriter::write(IWritable& output, OpCode value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(IWritable& output, uint16_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(IWritable& output, uint32_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(IWritable& output, uint64_t value) {
  output.write(reinterpret_cast<const std::byte*>(&value), sizeof(value));
}

void McapWriter::write(IWritable& output, const std::byte* data, uint64_t size) {
  output.write(reinterpret_cast<const std::byte*>(data), size);
}

void McapWriter::write(IWritable& output, const KeyValueMap& map, uint32_t size) {
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

// BufferReader ////////////////////////////////////////////////////////////////

void BufferReader::reset(const std::byte* data, uint64_t size, uint64_t uncompressedSize) {
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
  if (status != uncompressedSize_) {
    if (status < 0) {
      const auto msg =
        StrFormat("lz4 decompression of {} bytes into {} output bytes failed with error {}",
                  compressedSize_, uncompressedSize_, status);
      status_ = Status{StatusCode::DecompressionFailed, msg};
    } else {
      const auto msg =
        StrFormat("lz4 decompression of {} bytes into {} output bytes only produced {} bytes",
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
      const auto msg =
        StrFormat("zstd decompression of {} bytes into {} output bytes failed with error {}",
                  compressedSize_, uncompressedSize_, ZSTD_getErrorName(status));
      status_ = Status{StatusCode::DecompressionFailed, msg};
    } else {
      const auto msg =
        StrFormat("zstd decompression of {} bytes into {} output bytes only produced {} bytes",
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

// IChunkWriter ////////////////////////////////////////////////////////////////

void IChunkWriter::write(const std::byte* data, uint64_t size) {
  handleWrite(data, size);
  if (crcEnabled) {
    crc_.Update(reinterpret_cast<const CryptoPP::byte*>(data), size);
  }
}

void IChunkWriter::clear() {
  handleClear();
  crc_ = {};
}

uint32_t IChunkWriter::crc() const {
  uint32_t crc32 = 0;
  if (crcEnabled) {
    crc_.Final(reinterpret_cast<CryptoPP::byte*>(&crc32));
  }
  return crc32;
}

// BufferWriter //////////////////////////////////////////////////////////////

void BufferWriter::handleWrite(const std::byte* data, uint64_t size) {
  buffer_.insert(buffer_.end(), data, data + size);
}

void BufferWriter::end() {
  // no-op
}

uint64_t BufferWriter::size() const {
  return buffer_.size();
}

bool BufferWriter::empty() const {
  return buffer_.empty();
}

void BufferWriter::handleClear() {
  buffer_.clear();
}

const std::byte* BufferWriter::data() const {
  return buffer_.data();
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

void LZ4Writer::handleWrite(const std::byte* data, uint64_t size) {
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

void LZ4Writer::handleClear() {
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

void ZStdWriter::handleWrite(const std::byte* data, uint64_t size) {
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

void ZStdWriter::handleClear() {
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
      StrFormat(internal::ErrorMsgInvalidMagic, "Header", internal::MagicToHex(data));
    return Status{StatusCode::MagicMismatch, msg};
  }

  // Read the Header record
  Record record;
  if (auto status = ReadRecord(reader, sizeof(Magic), &record); !status.ok()) {
    return status;
  }
  if (record.opcode != OpCode::Header) {
    const auto msg = StrFormat(internal::ErrorMsgInvalidOpcode, "Header", uint8_t(record.opcode));
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
  const auto onProblem = [](const Status& problem) {};
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
      StrFormat("cannot read record at offset {}, {} bytes remaining", offset, maxSize);
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
    const auto msg =
      StrFormat("record type 0x{:02x} at offset {} has length {} but only {} bytes remaining",
                uint8_t(record->opcode), offset, record->dataSize, maxSize);
    return Status{StatusCode::InvalidRecord, msg};
  }
  bytesRead = reader.read(&record->data, offset + 9, record->dataSize);
  if (bytesRead != record->dataSize) {
    const auto msg = StrFormat(
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidMagic, "Footer",
                               internal::MagicToHex(data + internal::FooterLength - sizeof(Magic)));
    return Status{StatusCode::MagicMismatch, msg};
  }

  if (OpCode(data[0]) != OpCode::Footer) {
    const auto msg = StrFormat(internal::ErrorMsgInvalidOpcode, "Footer", data[0]);
    return Status{StatusCode::InvalidFile, msg};
  }

  // Sanity check the record length. This is just an additional safeguard, since the footer has a
  // fixed length
  const uint64_t length = internal::ParseUint64(data + 1);
  if (length != 8 + 8 + 4) {
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Footer", length);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Header", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Footer", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Schema", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Channel", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Message", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Chunk", record.dataSize);
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
      StrFormat(internal::ErrorMsgInvalidLength, "Chunk.records", chunk->compressedSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "MessageIndex", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  messageIndex->channelId = internal::ParseUint16(record.data);
  const uint32_t recordsSize = internal::ParseUint32(record.data + 2);

  if (recordsSize % 16 != 0 || recordsSize > record.dataSize - PreambleSize) {
    const auto msg =
      StrFormat(internal::ErrorMsgInvalidLength, "MessageIndex.records", recordsSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "ChunkIndex", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  chunkIndex->messageStartTime = internal::ParseUint64(record.data);
  chunkIndex->messageEndTime = internal::ParseUint64(record.data + 8);
  chunkIndex->chunkStartOffset = internal::ParseUint64(record.data + 8 + 8);
  chunkIndex->chunkLength = internal::ParseUint64(record.data + 8 + 8 + 8);
  const uint32_t messageIndexOffsetsSize = internal::ParseUint32(record.data + 8 + 8 + 8 + 8);

  if (messageIndexOffsetsSize % 10 != 0 ||
      messageIndexOffsetsSize > record.dataSize - PreambleSize) {
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "ChunkIndex.message_index_offsets",
                               messageIndexOffsetsSize);
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
  constexpr uint64_t MinSize = 4 + 8 + 8 + 4 + 8 + 4;

  assert(record.opcode == OpCode::Attachment);
  if (record.dataSize < MinSize) {
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Attachment", record.dataSize);
    return Status{StatusCode::InvalidRecord, msg};
  }

  uint32_t offset = 0;
  // name
  if (auto status = internal::ParseString(record.data, record.dataSize, &attachment->name);
      !status.ok()) {
    return status;
  }
  offset += 4 + attachment->name.size();
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
      StrFormat(internal::ErrorMsgInvalidLength, "Attachment.data", attachment->dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "AttachmentIndex", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Statistics", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Statistics.channelMessageCounts",
                               channelMessageCountsSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "Metadata", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "MetadataIndex", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "SummaryOffset", record.dataSize);
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
    const auto msg = StrFormat(internal::ErrorMsgInvalidLength, "DataEnd", record.dataSize);
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
    : dataSource_(&dataSource)
    , offset(startOffset)
    , endOffset(endOffset)
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
      const auto msg = StrFormat("record type {} cannot appear in Chunk", uint8_t(record.opcode));
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
            const auto msg = StrFormat("unrecognized compression \"{}\"", chunk.compression);
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
      onProblem_(
        Status{StatusCode::InvalidChannelId,
               StrFormat("message at log_time {} (seq {}) references missing channel id {}",
                         message.logTime, message.sequence, message.channelId)});
      return;
    }

    auto& channel = *maybeChannel;
    SchemaPtr maybeSchema;
    if (channel.schemaId != 0) {
      maybeSchema = mcapReader_.schema(channel.schemaId);
      if (!maybeSchema) {
        onProblem_(Status{StatusCode::InvalidSchemaId,
                          StrFormat("channel {} ({}) references missing schema id {}", channel.id,
                                    channel.topic, channel.schemaId)});
        return;
      }
    }

    curMessage_.emplace(message, maybeChannel, maybeSchema);
  };

  ++(*this);
}

LinearMessageView::Iterator::reference LinearMessageView::Iterator::operator*() const {
  return *curMessage_;
}

LinearMessageView::Iterator::pointer LinearMessageView::Iterator::operator->() const {
  return &*curMessage_;
}

LinearMessageView::Iterator& LinearMessageView::Iterator::operator++() {
  curMessage_ = std::nullopt;

  if (!recordReader_.has_value()) {
    return *this;
  }

  // Keep iterate through records until we find a message with a logTime >= startTime_
  while (!curMessage_.has_value() || curMessage_->message.logTime < startTime_) {
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
  if (curMessage_->message.logTime >= endTime_) {
    recordReader_ = std::nullopt;
  }
  return *this;
}

LinearMessageView::Iterator LinearMessageView::Iterator::operator++(int) {
  LinearMessageView::Iterator tmp = *this;
  ++(*this);
  return tmp;
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
