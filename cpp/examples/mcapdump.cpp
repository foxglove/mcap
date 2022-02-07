#include <mcap/mcap.hpp>

#include <iostream>

// #include <array>
// #include <chrono>
// #include <cstring>
// #include <fstream>

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>\n";
    return 1;
  }

  const std::string inputFile = argv[1];

  mcap::McapReader reader;

  mcap::McapReaderOptions options{};

  std::ifstream input(inputFile, std::ios::binary);
  auto status = reader.open(input, options);
  if (!status.ok()) {
    std::cerr << "Failed to open input file: " << status.message << "\n";
    reader.close();
    return 1;
  }

  // mcap::ChannelInfo topic("/chatter", "ros1", "ros1", "std_msgs/String", StringSchema);
  // writer.addChannel(topic);

  // std::array<std::byte, 4 + 13> payload;
  // const uint32_t length = 13;
  // std::memcpy(payload.data(), &length, 4);
  // std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // mcap::Message msg;
  // msg.channelId = topic.id;
  // msg.sequence = 0;
  // msg.publishTime = now();
  // msg.recordTime = msg.publishTime;
  // msg.data = payload.data();
  // msg.dataSize = payload.size();

  // const auto res = writer.write(msg);
  // if (!res.ok()) {
  //   std::cerr << "Failed to write message: " << res.message << "\n";
  //   writer.terminate();
  //   out.close();
  //   std::remove("output.mcap");
  //   return 1;
  // }

  reader.close();
  return 0;
}
