// Do not compile on systems with non-8-bit bytes
static_assert(std::numeric_limits<unsigned char>::digits == 8);

namespace mcap {

// Public API //////////////////////////////////////////////////////////////////

McapWriter::~McapWriter() {
  close();
}

void McapWriter::open(mcap::IWritable& writer, const McapWriterOptions& options) {
  chunkSize_ = options.noChunking ? 0 : options.chunkSize;
  indexing_ = !options.noIndexing;
  output_ = &writer;
  writeMagic(writer);
  write(writer, Header{options.profile, options.library, options.metadata});
}

void McapWriter::open(std::ostream& stream, const McapWriterOptions& options) {
  streamOutput_ = std::make_unique<mcap::StreamWriter>(stream);
  open(*streamOutput_, options);
}

void McapWriter::close() {
  if (!output_) {
    return;
  }
  auto& output = *output_;

  // Check if there is an open chunk that needs to be closed
  if (currentChunk_.size() > 0) {
    writeChunk(output, currentChunk_);
    currentChunk_.end();
  }

  uint64_t indexOffset = 0;
  uint32_t indexCrc = 0;

  if (indexing_) {
    // Get the offset of the End Of File section
    indexOffset = output.size();

    // Write all channel info records
    for (const auto& channel : channels_) {
      write(output, channel);
    }

    // Write chunk index records
    for (const auto& chunkIndexRecord : chunkIndex_) {
      write(output, chunkIndexRecord);
    }

    // Write attachment index records
    for (const auto& attachmentIndexRecord : attachmentIndex_) {
      write(output, attachmentIndexRecord);
    }

    // Write the statistics record
    write(output, statistics_);
  }

  // TODO: Calculate the index CRC

  // Write the footer and trailing magic
  write(output, mcap::Footer{indexOffset, indexCrc});
  writeMagic(output);

  output.end();
  output_ = nullptr;
  streamOutput_.reset();
}

void McapWriter::addChannel(mcap::ChannelInfo& info) {
  info.channelId = uint16_t(channels_.size() + 1);
  channels_.push_back(info);
}

mcap::Status McapWriter::write(const mcap::Message& message) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& output = chunkSize_ > 0 ? currentChunk_ : *output_;
  auto& channelMessageCounts = statistics_.channelMessageCounts;

  // Write out channel info if we have not yet done so
  if (channelMessageCounts.find(message.channelId) == channelMessageCounts.end()) {
    const size_t index = message.channelId - 1;
    if (index >= channels_.size()) {
      return StatusCode::InvalidChannelId;
    }

    write(output, channels_[index]);
    channelMessageCounts.emplace(message.channelId, 0);
    ++statistics_.channelCount;
  }

  const uint64_t messageOffset = output.size();

  // Write the message
  write(output, message);

  // Update statistics
  if (indexing_) {
    ++statistics_.messageCount;
    channelMessageCounts[message.channelId] += 1;
  }

  if (chunkSize_ > 0) {
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
    if (currentChunk_.size() >= chunkSize_) {
      writeChunk(*output_, currentChunk_);
      currentChunk_.end();
    }
  }

  return StatusCode::Success;
}

mcap::Status McapWriter::write(const mcap::Attachment& attachment) {
  if (!output_) {
    return StatusCode::NotOpen;
  }
  auto& output = *output_;

  // Check if we have an open chunk that needs to be closed
  if (currentChunk_.size() > 0) {
    writeChunk(output, currentChunk_);
    currentChunk_.end();
  }

  const uint64_t fileOffset = output.size();

  write(output, attachment);

  if (indexing_) {
    ++statistics_.attachmentCount;
    attachmentIndex_.emplace_back(attachment, fileOffset);
  }

  return StatusCode::Success;
}

// Private methods /////////////////////////////////////////////////////////////

namespace internal {

uint32_t KeyValueMapSize(const KeyValueMap& map) {
  uint32_t size = 0;
  for (const auto& [key, value] : map) {
    size += 4 + key.size() + 4 + value.size();
  }
  return size;
}

}  // namespace internal

void McapWriter::writeChunk(mcap::IWritable& output, const mcap::BufferedWriter& chunkData) {
  uint64_t uncompressedSize = chunkData.size();
  uint32_t uncompressedCrc = 0;
  std::string compression = "";
  uint64_t recordsSize = uncompressedSize;
  const std::byte* records = chunkData.data();

  // Write the chunk
  const uint64_t chunkOffset = output.size();
  write(output, Chunk{uncompressedSize, uncompressedCrc, compression, recordsSize, records});

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
    chunkIndexRecord.compressedSize = recordsSize;
    chunkIndexRecord.uncompressedSized = uncompressedSize;
    chunkIndexRecord.crc = 0;

    // Reset start/end times for the next chunk
    currentChunkStart_ = std::numeric_limits<uint64_t>::max();
    currentChunkEnd_ = std::numeric_limits<uint64_t>::min();
  }
}

void McapWriter::writeMagic(mcap::IWritable& output) {
  write(output, reinterpret_cast<const std::byte*>(Magic), sizeof(Magic));
}

void McapWriter::write(mcap::IWritable& output, const mcap::Header& header) {
  const uint32_t metadataSize = internal::KeyValueMapSize(header.metadata);
  const uint64_t recordSize =
    4 + header.profile.size() + 4 + header.library.size() + 4 + metadataSize;

  write(output, OpCode::Header);
  write(output, recordSize);
  write(output, header.profile);
  write(output, header.library);
  write(output, header.metadata, metadataSize);
}

void McapWriter::write(mcap::IWritable& output, const mcap::Footer& footer) {
  write(output, OpCode::Footer);
  write(output, uint64_t(12));
  write(output, footer.indexOffset);
  write(output, footer.indexCrc);
}

void McapWriter::write(mcap::IWritable& output, const mcap::ChannelInfo& info) {
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
}

void McapWriter::write(mcap::IWritable& output, const mcap::Message& message) {
  const uint64_t recordSize = 2 + 4 + 8 + 8 + message.dataSize;

  write(output, OpCode::Message);
  write(output, recordSize);
  write(output, message.channelId);
  write(output, message.sequence);
  write(output, message.publishTime);
  write(output, message.recordTime);
  write(output, message.data, message.dataSize);
}

void McapWriter::write(mcap::IWritable& output, const mcap::Attachment& attachment) {
  const uint64_t recordSize =
    4 + attachment.name.size() + 8 + 4 + attachment.contentType.size() + attachment.dataSize;

  write(output, OpCode::Attachment);
  write(output, recordSize);
  write(output, attachment.name);
  write(output, attachment.recordTime);
  write(output, attachment.contentType);
  write(output, attachment.data, attachment.dataSize);
}

void McapWriter::write(mcap::IWritable& output, const mcap::Chunk& chunk) {
  const uint64_t recordSize = 8 + 4 + 4 + chunk.compression.size() + chunk.recordsSize;

  write(output, OpCode::Chunk);
  write(output, recordSize);
  write(output, chunk.uncompressedSize);
  write(output, chunk.uncompressedCrc);
  write(output, chunk.compression);
  write(output, chunk.records, chunk.recordsSize);
}

void McapWriter::write(mcap::IWritable& output, const mcap::MessageIndex& index) {
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
}

void McapWriter::write(mcap::IWritable& output, const mcap::ChunkIndex& index) {
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
}

void McapWriter::write(mcap::IWritable& output, const mcap::AttachmentIndex& index) {
  const uint64_t recordSize = 8 + 8 + 4 + index.name.size() + 4 + index.contentType.size() + 8;

  write(output, OpCode::AttachmentIndex);
  write(output, recordSize);
  write(output, index.recordTime);
  write(output, index.attachmentSize);
  write(output, index.name);
  write(output, index.contentType);
  write(output, index.offset);
}

void McapWriter::write(mcap::IWritable& output, const mcap::Statistics& stats) {
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
}

void McapWriter::write(mcap::IWritable& output, const mcap::UnknownRecord& record) {
  write(output, mcap::OpCode(record.opcode));
  write(output, record.dataSize);
  write(output, record.data, record.dataSize);
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

uint64_t BufferedWriter::size() const {
  return buffer_.size();
}

void BufferedWriter::end() {
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

}  // namespace mcap
