// Example code for reading Protobuf foxglove.PointCloud messages from an MCAP file using generated
// protobuf headers.
// Try it out by generating some PointCloud messages with the protobuf writer example,
// and running this executable with the resulting MCAP file.
#define MCAP_IMPLEMENTATION
#include "foxglove/PointCloud.pb.h"
#include "mcap/reader.hpp"

namespace gp = google::protobuf;

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
    foxglove::PointCloud pointCloud;
    if (!pointCloud.ParseFromArray(static_cast<const void*>(it->message.data),
                                   it->message.dataSize)) {
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
