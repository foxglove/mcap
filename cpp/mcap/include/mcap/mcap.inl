// Do not compile on systems with non-8-bit bytes
static_assert(CHAR_BIT == 8);

namespace mcap {

// Public API //////////////////////////////////////////////////////////////////

McapWriter::~McapWriter() {
  close();
}

void McapWriter::open(std::ostream& stream, const McapWriterOptions& options) {
  stream_ = &stream;
  writeMagic();
  write(Header{options.profile, options.library, options.metadata});
}

void McapWriter::close() {
  if (!stream_) {
    return;
  }
  write(mcap::Footer{0, 0});
  writeMagic();
  stream_->flush();
  stream_ = nullptr;
}

void McapWriter::registerChannel(mcap::ChannelInfo& info) {
  info.channelId = uint16_t(channels_.size() + 1);
  channels_.push_back(info);
}

std::optional<std::error_code> McapWriter::write(const mcap::Message& message) {
  if (!stream_) {
    return make_error_code(ErrorCode::NotOpen);
  }

  // Write out channel info if we have not yet done so
  if (writtenChannels_.find(message.channelId) == writtenChannels_.end()) {
    const size_t index = message.channelId - 1;
    if (index >= channels_.size()) {
      return make_error_code(ErrorCode::InvalidChannelId);
    }

    write(channels_[index]);
    writtenChannels_.insert(message.channelId);
  }

  const uint64_t recordSize = 2 + 4 + 8 + 8 + message.dataSize;

  write(OpCode::Message);
  write(recordSize);
  write(message.channelId);
  write(message.sequence);
  write(message.publishTime);
  write(message.recordTime);
  write(message.data, message.dataSize);

  return std::nullopt;
}

std::optional<std::error_code> McapWriter::write(const mcap::Attachment& attachment) {
  if (!stream_) {
    return make_error_code(ErrorCode::NotOpen);
  }

  const uint64_t recordSize =
    4 + attachment.name.size() + 8 + 4 + attachment.contentType.size() + attachment.dataSize;

  write(OpCode::Attachment);
  write(recordSize);
  write(attachment.name);
  write(attachment.recordTime);
  write(attachment.contentType);
  write(attachment.data, attachment.dataSize);

  return std::nullopt;
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

void McapWriter::writeMagic() {
  stream_->write(Magic, sizeof(Magic));
}

void McapWriter::write(const mcap::Header& header) {
  const uint32_t metadataSize = internal::KeyValueMapSize(header.metadata);
  const uint64_t recordSize =
    4 + header.profile.size() + 4 + header.library.size() + 4 + metadataSize;

  write(OpCode::Header);
  write(recordSize);
  write(header.profile);
  write(header.library);
  write(header.metadata, metadataSize);
}

void McapWriter::write(const mcap::Footer& footer) {
  write(OpCode::Footer);
  write(uint64_t(12));
  write(footer.indexOffset);
  write(footer.indexCrc);
}

void McapWriter::write(const mcap::ChannelInfo& info) {
  const uint32_t userDataSize = internal::KeyValueMapSize(info.userData);
  const uint64_t recordSize = 2 + 4 + info.topicName.size() + 4 + info.encoding.size() + 4 +
                              info.schemaName.size() + 4 + info.schema.size() + 4 + userDataSize +
                              4;
  const uint32_t crc = 0;

  write(OpCode::ChannelInfo);
  write(recordSize);
  write(info.channelId);
  write(info.topicName);
  write(info.encoding);
  write(info.schemaName);
  write(info.schema);
  write(info.userData, userDataSize);
  write(crc);
}

void McapWriter::write(const std::string_view str) {
  write(uint32_t(str.size()));
  stream_->write(str.data(), str.size());
}

void McapWriter::write(OpCode value) {
  stream_->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void McapWriter::write(uint16_t value) {
  stream_->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void McapWriter::write(uint32_t value) {
  stream_->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void McapWriter::write(uint64_t value) {
  stream_->write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void McapWriter::write(uint8_t* data, uint64_t size) {
  stream_->write(reinterpret_cast<const char*>(data), size);
}

void McapWriter::write(const KeyValueMap& map, uint32_t size) {
  write(size > 0 ? size : internal::KeyValueMapSize(map));
  for (const auto& [key, value] : map) {
    write(key);
    write(value);
  }
}

}  // namespace mcap
