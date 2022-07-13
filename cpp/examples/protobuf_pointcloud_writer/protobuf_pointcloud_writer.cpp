// Example code for writing Protobuf messages to an MCAP file. This executable
// writes a sequence of foxglove.PointCloud messages to an MCAP which should
// show an expanding sphere when viewed in Foxglove Studio.
#define MCAP_IMPLEMENTATION

#include <google/protobuf/descriptor.pb.h>

#include "foxglove/PointCloud.pb.h"
#include "mcap/writer.hpp"
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <queue>
#include <random>
#include <sstream>
#include <unordered_set>

#define NS_PER_MS 1000000
#define NS_PER_S 1000000000
#define POINTS_PER_CLOUD 1000
#define FIELDS_PER_POINT 3

// PointGenerator generates random points on a sphere.
class PointGenerator {
private:
  std::mt19937 _generator;
  std::uniform_real_distribution<float> _distribution;

public:
  PointGenerator(uint32_t seed = 0)
      : _generator(seed)
      , _distribution(0.0, 1.0) {}

  // next produces a random point on the unit sphere, scaled by `scale`.
  std::tuple<float, float, float> next(float scale) {
    float theta = 2 * M_PI * _distribution(_generator);
    float phi = acos(1.0 - (2.0 * _distribution(_generator)));
    float x = float((sin(phi) * cos(theta)) * scale);
    float y = float((sin(phi) * sin(theta)) * scale);
    float z = float(cos(phi) * scale);
    return {x, y, z};
  }
};

// Writes the FileDescriptor of this descriptor and all transitive dependencies
// to a string, for use as a channel schema.
std::string SerializeFdSet(const google::protobuf::Descriptor* toplevelDescriptor) {
  google::protobuf::FileDescriptorSet fdSet;
  std::queue<const google::protobuf::FileDescriptor*> toAdd;
  toAdd.push(toplevelDescriptor->file());
  std::unordered_set<std::string> added;
  while (!toAdd.empty()) {
    const auto& next = toAdd.front();
    toAdd.pop();
    next->CopyTo(fdSet.add_file());
    added.insert(next->name());
    for (int i = 0; i < next->dependency_count(); ++i) {
      const auto& dep = next->dependency(i);
      if (added.find(dep->name()) == added.end()) {
        toAdd.push(dep);
      }
    }
  }
  return fdSet.SerializeAsString();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <output.mcap>" << std::endl;
    return 1;
  }
  const char* outputFilename = argv[1];

  mcap::McapWriter writer;
  {
    auto options = mcap::McapWriterOptions("x-protobuf");
    const auto res = writer.open(outputFilename, options);
    if (!res.ok()) {
      std::cerr << "Failed to open " << outputFilename << " for writing: " << res.message
                << std::endl;
      return 1;
    }
  }

  // set up the schema and channel.
  mcap::ChannelId channelId;
  {
    mcap::Schema schema("foxglove.PointCloud", "protobuf",
                        SerializeFdSet(foxglove::PointCloud::descriptor()));

    writer.addSchema(schema);

    mcap::Channel channel("/pointcloud", "protobuf", schema.id);
    writer.addChannel(channel);
    channelId = channel.id;
  }

  // Set up the fields in the point cloud that don't need to change.
  foxglove::PointCloud pcl;
  {
    auto* pose = pcl.mutable_pose();
    auto* position = pose->mutable_position();
    position->set_x(0);
    position->set_y(0);
    position->set_z(0);
    auto* orientation = pose->mutable_orientation();
    orientation->set_x(0);
    orientation->set_y(0);
    orientation->set_z(0);
    orientation->set_w(1);

    pcl.set_point_stride(sizeof(float) * FIELDS_PER_POINT);
    const char* const fieldNames[] = {"x", "y", "z"};
    int fieldOffset = 0;
    for (const auto& name : fieldNames) {
      auto field = pcl.add_fields();
      field->set_name(name);
      field->set_offset(fieldOffset);
      field->set_type(foxglove::PackedElementField_NumericType_FLOAT32);
      fieldOffset += sizeof(float);
    }
    pcl.mutable_data()->append(POINTS_PER_CLOUD * FIELDS_PER_POINT * sizeof(float), '\0');
    pcl.set_frame_id("pointcloud");
  }

  mcap::Timestamp startTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
  PointGenerator pointGenerator;
  // write 100 pointcloud messages into the output MCAP file.
  for (uint64_t frameIndex = 0; frameIndex < 100; ++frameIndex) {
    mcap::Timestamp cloudTime = startTime + (frameIndex * 100 * NS_PER_MS);
    float cloudScale = 1.0 + (float(frameIndex) / 50.0);

    auto timestamp = pcl.mutable_timestamp();
    timestamp->set_seconds(cloudTime / NS_PER_S);
    timestamp->set_nanos(cloudTime % NS_PER_S);

    size_t offset = 0;
    for (int pointIndex = 0; pointIndex < POINTS_PER_CLOUD; ++pointIndex) {
      auto [x, y, z] = pointGenerator.next(cloudScale);
      char* data = pcl.mutable_data()->data();
      std::memcpy(&data[offset], reinterpret_cast<const char*>(&x), sizeof(x));
      offset += sizeof(x);
      std::memcpy(&data[offset], reinterpret_cast<const char*>(&y), sizeof(y));
      offset += sizeof(y);
      std::memcpy(&data[offset], reinterpret_cast<const char*>(&z), sizeof(z));
      offset += sizeof(z);
    }
    std::string serialized = pcl.SerializeAsString();
    mcap::Message msg;
    msg.channelId = channelId;
    msg.sequence = frameIndex;
    msg.publishTime = cloudTime;
    msg.logTime = cloudTime;
    msg.data = reinterpret_cast<const std::byte*>(serialized.data());
    msg.dataSize = serialized.size();
    const auto res = writer.write(msg);
    if (!res.ok()) {
      std::cerr << "Failed to write message: " << res.message << "\n";
      writer.terminate();
      writer.close();
      std::remove(outputFilename);
      return 1;
    }
  }
  writer.close();
  std::cerr << "wrote 100 pointcloud messages to " << outputFilename << std::endl;
  return 0;
}
