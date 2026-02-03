// Example of writing JSON messages to an MCAP file in C++.
// Writes a topic of Point2 messages.
#include <mcap/writer.hpp>

#include <nlohmann/json.hpp>

#include <chrono>
#include <cmath>
#include <iostream>

#define NUM_FRAMES 100
#define NS_PER_MS 1000000

static const char* SCHEMA_NAME = "foxglove.Point2";
static const char* SCHEMA_TEXT = R"({
  "$comment": "Generated from Point2 by @foxglove/schemas", 
  "title": "Point2", 
  "description": "A point representing a position in 2D space", 
  "type": "object", 
  "properties": { 
    "x": { 
      "type": "number", 
      "description": "x coordinate position" 
    }, 
    "y": { 
      "type": "number", 
      "description": "y coordinate position" 
    } 
  } 
})";

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <output.mcap>" << std::endl;
    return 1;
  }
  const char* outputFilename = argv[1];

  mcap::McapWriter writer;
  {
    auto options = mcap::McapWriterOptions("");
    const auto res = writer.open(outputFilename, options);
    if (!res.ok()) {
      std::cerr << "Failed to open " << outputFilename << " for writing: " << res.message
                << std::endl;
      return 1;
    }
  }
  // Create a channel and schema for our messages.
  // A message's channel informs the reader which topic those messages were published on.
  // A channel's schema informs the reader of how to interpret the messages' content.
  // A schema can be used by multiple channels, and a channel can be used by multiple messages.
  mcap::ChannelId channelId;
  {
    mcap::Schema schema(SCHEMA_NAME, "jsonschema", SCHEMA_TEXT);
    writer.addSchema(schema);

    // choose an arbitrary topic name.
    mcap::Channel channel("point", "json", schema.id);
    writer.addChannel(channel);
    channelId = channel.id;
  }
  mcap::Timestamp startTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();

  for (uint32_t frame_index = 0; frame_index < NUM_FRAMES; ++frame_index) {
    mcap::Timestamp frameTime = startTime + (static_cast<uint64_t>(frame_index) * 100 * NS_PER_MS);

    auto contentJson = nlohmann::json::object();
    float p = static_cast<float>((2.0 * 2.0 * M_PI * static_cast<double>(frame_index)) /
                                 static_cast<double>(NUM_FRAMES));
    contentJson["x"] = sin(p);
    contentJson["y"] = cos(p);
    std::string serialized = contentJson.dump();

    mcap::Message msg;
    msg.channelId = channelId;
    msg.logTime = frameTime;
    msg.publishTime = frameTime;
    msg.sequence = frame_index;
    msg.data = reinterpret_cast<const std::byte*>(serialized.data());
    msg.dataSize = serialized.size();
    auto res = writer.write(msg);
    if (!res.ok()) {
      std::cerr << "failed to write message: " << res.message << std::endl;
      writer.close();
      return 1;
    }
  }
  std::cout << "wrote " << NUM_FRAMES << " " << SCHEMA_NAME << " messages to " << outputFilename
            << std::endl;
  writer.close();
  return 0;
}
