#include <mcap/mcap.hpp>

#include <array>
#include <chrono>
#include <fstream>

constexpr char StringSchema[] = "string data";

mcap::Timestamp now() {
  const auto timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::system_clock::now().time_since_epoch());
  return mcap::Timestamp(timestamp.count());
}

int main() {
  mcap::McapWriter writer;

  std::ofstream out("output.mcap", std::ios::binary);
  writer.open(out, mcap::McapWriterOptions("ros1"));

  mcap::ChannelInfo topic("/chatter", "ros1", "std_msgs/String", StringSchema);
  writer.registerChannel(topic);

  std::array<uint8_t, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  mcap::Message msg;
  msg.channelId = topic.channelId;
  msg.sequence = 0;
  msg.publishTime = now();
  msg.recordTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  writer.write(msg);
  writer.close();

  return 0;
}
