#define MCAP_IMPLEMENTATION

#include "descriptor.pb.h"
#include "foxglove/PointCloud.pb.h"
#include "mcap/writer.hpp"
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <random>

#define NS_PER_MS 1000000
#define NS_PER_S 1000000000
#define POINTS_PER_CLOUD 1000
#define FIELDS_PER_POINT 3

// Point represents a point in 3D space.
struct Point {
  float x;
  float y;
  float z;
};

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
  Point next(float scale) {
    float theta = 2 * M_PI * _distribution(_generator);
    float phi = acos(1 - 2 * _distribution(_generator));
    Point point;
    point.x = float((sin(phi) * cos(theta)) * scale);
    point.y = float((sin(phi) * sin(theta)) * scale);
    point.z = float(cos(phi) * scale);
    return point;
  }
};

// WriteLittleEndianFloat writes a little-endian float into the `output` string of bytes at
// `offset`.
void WriteLittleEndianFloat(float input, std::string* output, size_t offset) {
  static_assert(sizeof(uint32_t) == sizeof(float));
  uint32_t asInt = *reinterpret_cast<uint32_t*>(&input);
  if (output->size() <= offset + 3) {
    std::cerr << "tried to write 4 bytes at offset " << offset << " into buffer of size "
              << output->size() << std::endl;
    abort();
  }
  // write in little-endian order, no matter what endian-ness the host uses.
  // the LSB goes into the first byte.
  (*output)[offset + 0] = asInt & 0xFF;
  (*output)[offset + 1] = (asInt >> 8) & 0xFF;
  (*output)[offset + 2] = (asInt >> 16) & 0xFF;
  (*output)[offset + 3] = (asInt >> 24) & 0xFF;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <output.mcap>\n";
    return 1;
  }
  const char* outputFilename = argv[1];

  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("x-protobuf");
  std::ofstream out(outputFilename, std::ios::binary);
  writer.open(out, options);
  // set up the schema and channel.
  mcap::Schema schema("foxglove.PointCloud", "protobuf",
                      std::string_view((char*)(descriptor_pb_bin), descriptor_pb_bin_len));
  writer.addSchema(schema);

  mcap::Channel channel("/pointcloud", "protobuf", schema.id);
  writer.addChannel(channel);

  mcap::Timestamp startTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();

  PointGenerator pointGenerator;
  foxglove::PointCloud pcl;
  foxglove::Pose* pose = pcl.mutable_pose();
  foxglove::Vector3* position = pose->mutable_position();
  position->set_x(0);
  position->set_y(0);
  position->set_z(0);
  foxglove::Quaternion* orientation = pose->mutable_orientation();
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

  // write 100 pointcloud messages into the output MCAP file.
  for (uint64_t frameIndex = 0; frameIndex < 100; ++frameIndex) {
    mcap::Timestamp cloudTime = startTime + (frameIndex * 100 * NS_PER_MS);
    float cloudScale = 1.0 + (float(frameIndex) / 50.0);

    google::protobuf::Timestamp* timestamp = pcl.mutable_timestamp();
    timestamp->set_seconds(cloudTime / NS_PER_S);
    timestamp->set_nanos(cloudTime % NS_PER_S);

    std::string* data = pcl.mutable_data();
    for (int pointIndex = 0; pointIndex < POINTS_PER_CLOUD; ++pointIndex) {
      auto point = pointGenerator.next(cloudScale);
      auto baseOffset = (pointIndex * sizeof(float) * FIELDS_PER_POINT);
      WriteLittleEndianFloat(point.x, data, baseOffset);
      WriteLittleEndianFloat(point.y, data, baseOffset + sizeof(float));
      WriteLittleEndianFloat(point.z, data, baseOffset + (2 * sizeof(float)));
    }
    std::string serialized;
    pcl.SerializeToString(&serialized);

    mcap::Message msg;
    msg.channelId = channel.id;
    msg.sequence = frameIndex;
    msg.publishTime = cloudTime;
    msg.logTime = cloudTime;
    msg.data = (std::byte*)(serialized.data());
    msg.dataSize = serialized.size();
    const auto res = writer.write(msg);
    if (!res.ok()) {
      std::cerr << "Failed to write message: " << res.message << "\n";
      writer.terminate();
      out.close();
      std::remove(outputFilename);
      return 1;
    }
  }
  writer.close();
  std::cerr << "wrote 100 pointcloud messages to " << outputFilename << std::endl;
  return 0;
}
