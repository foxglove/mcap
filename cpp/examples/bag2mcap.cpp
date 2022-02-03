#include <mcap/mcap.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <fstream>

constexpr char StringSchema[] = "string data";

mcap::Timestamp now() {
  const auto timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::system_clock::now().time_since_epoch());
  return mcap::Timestamp(timestamp.count());
}

int main() {
  mcap::McapWriter writer;

  auto options = mcap::McapWriterOptions("ros1");
  options.compression = mcap::Compression::Zstd;

  std::ofstream out("output.mcap", std::ios::binary);
  writer.open(out, options);

  mcap::ChannelInfo topic("/chatter", "ros1", "ros1", "std_msgs/String", StringSchema);
  writer.addChannel(topic);

  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = now();
  msg.recordTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  const auto res = writer.write(msg);
  if (!res.ok()) {
    std::cerr << "Failed to write message: " << res.message << "\n";
    writer.terminate();
    out.close();
    std::remove("output.mcap");
    return 1;
  }

  writer.close();

  return 0;
}
