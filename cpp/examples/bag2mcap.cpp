#include <mcap/writer.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>

constexpr char StringSchema[] = "string data";

mcap::Timestamp now() {
  return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());
}

int main() {
  mcap::McapWriter writer;

  auto options = mcap::McapWriterOptions("ros1");
  options.compression = mcap::Compression::Zstd;

  std::ofstream out("output.mcap", std::ios::binary);
  writer.open(out, options);

  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  std::array<std::byte, 4 + 13> payload{};
  constexpr uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = now();
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  const auto res = writer.write(msg);
  if (!res.ok()) {
    std::cerr << "Failed to write message: " << res.message << "\n";
    writer.terminate();
    out.close();
    std::ignore = std::remove("output.mcap");
    return 1;
  }

  writer.close();

  return 0;
}
