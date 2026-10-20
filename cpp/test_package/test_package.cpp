// Smoke test: writes a file in memory with every compression and reads it back.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>

#include <cstdint>
#include <cstring>
#include <iostream>

int main() {
  std::cout << "mcap " << mcap::LibraryVersion << "\n";

  for (const auto compression :
       {mcap::Compression::None, mcap::Compression::Lz4, mcap::Compression::Zstd}) {
    mcap::BufferWriter buffer;
    mcap::McapWriter writer;
    mcap::McapWriterOptions options("");
    options.compression = compression;
    writer.open(buffer, options);

    mcap::Schema schema("test", "jsonschema", "{}");
    writer.addSchema(schema);
    mcap::Channel channel("/test", "json", schema.id);
    writer.addChannel(channel);

    const char payload[] = "{}";
    mcap::Message message;
    message.channelId = channel.id;
    message.logTime = 1;
    message.publishTime = 1;
    message.data = reinterpret_cast<const std::byte*>(payload);
    message.dataSize = sizeof(payload) - 1;
    const auto writeStatus = writer.write(message);
    if (!writeStatus.ok()) {
      std::cerr << "write failed: " << writeStatus.message << "\n";
      return 1;
    }
    writer.close();

    mcap::BufferReader reader;
    reader.reset(buffer.data(), buffer.size(), buffer.size());
    mcap::McapReader mcapReader;
    const auto openStatus = mcapReader.open(reader);
    if (!openStatus.ok()) {
      std::cerr << "open failed: " << openStatus.message << "\n";
      return 1;
    }
    size_t count = 0;
    auto messages = mcapReader.readMessages();
    for (const auto& view : messages) {
      (void)view;
      ++count;
    }
    if (!messages.status().ok()) {
      std::cerr << "read failed: " << messages.status().message << "\n";
      return 1;
    }
    if (count != 1) {
      std::cerr << "expected 1 message, read " << count << "\n";
      return 1;
    }
  }
  return 0;
}
