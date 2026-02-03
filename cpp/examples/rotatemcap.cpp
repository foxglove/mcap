#include <mcap/writer.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>

constexpr char StringSchema[] = "string data";
constexpr char NumberSchema[] = "number data";

mcap::Timestamp now() {
  return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());
}

// Use a single McapWriter to manage a "rotating" series of mcap files.
// Write some data to one mcap file, then switch to a new mcap file,
// while using the same set of schemas and channels.
int main() {
  mcap::McapWriter writer;

  auto options = mcap::McapWriterOptions("ros1");
  options.compression = mcap::Compression::Zstd;

  std::ofstream out("output.mcap", std::ios::binary);
  writer.open(out, options);

  // Here we add both all the schemas and channels up front, but we
  // could have also added stdMsgsNumber and topic2 before we used
  // them in the second file below.

  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);
  mcap::Schema stdMsgsNumber("std_msgs/Number", "ros1msg", NumberSchema);
  writer.addSchema(stdMsgsNumber);

  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);
  mcap::Channel topic2("/chatter2", "ros1", stdMsgsNumber.id);
  writer.addChannel(topic2);

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

  auto res = writer.write(msg);
  if (!res.ok()) {
    std::cerr << "Failed to write message: " << res.message << "\n";
    writer.terminate();
    std::ignore = std::remove("output.mcap");
    return 1;
  }

  // Rotate the mcap file to a new file, no need to call close()
  std::ofstream out2("output2.mcap", std::ios::binary);
  writer.open(out2, options);

  mcap::Message msg2;
  msg2.channelId = topic2.id;
  msg2.sequence = 0;
  msg2.publishTime = now();
  msg2.logTime = msg2.publishTime;
  msg2.data = reinterpret_cast<const std::byte*>("1234");
  msg2.dataSize = 4;

  res = writer.write(msg2);
  if (!res.ok()) {
    std::cerr << "Failed to write message: " << res.message << "\n";
    writer.terminate();
    std::ignore = std::remove("output2.mcap");
    return 1;
  }

  writer.close();

  return 0;
}
