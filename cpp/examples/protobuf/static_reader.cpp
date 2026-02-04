// Example code for reading Protobuf foxglove.PointCloud messages from an MCAP file using generated
// protobuf headers.
// Try it out by generating some PointCloud messages with the protobuf writer example,
// and running this executable with the resulting MCAP file.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>

#include <foxglove/PointCloud.pb.h>

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input.mcap>" << std::endl;
    return 1;
  }
  const char* inputFilename = argv[1];

  mcap::McapReader reader;
  {
    const auto res = reader.open(inputFilename);
    if (!res.ok()) {
      std::cerr << "Failed to open " << inputFilename << " for reading: " << res.message
                << std::endl;
      return 1;
    }
  }

  auto messageView = reader.readMessages();
  std::cout << "topic\t\ttype\t\t\ttimestamp\t\tframe_id" << std::endl;
  for (auto it = messageView.begin(); it != messageView.end(); it++) {
    // skip any non-protobuf, non PointCloud messages.
    if ((it->schema->encoding != "protobuf") || it->schema->name != "foxglove.PointCloud") {
      continue;
    }
    if (it->channel->messageEncoding != "protobuf") {
      std::cerr << "expected message encoding 'protobuf', got " << it->channel->messageEncoding
                << std::endl;
      reader.close();
      return 1;
    }
    foxglove::PointCloud pointCloud;
    if (!pointCloud.ParseFromArray(it->message.data, static_cast<int>(it->message.dataSize))) {
      std::cerr << "could not parse pointcloud message" << std::endl;
      return 1;
    }
    // Read a field out from the message, to prove that we can.
    std::cout << it->channel->topic << "\t(" << it->schema->name << ")\t[" << it->message.logTime
              << "]:\t{ frame_id: " << pointCloud.frame_id() << " }" << std::endl;
  }
  reader.close();
  return 0;
}
