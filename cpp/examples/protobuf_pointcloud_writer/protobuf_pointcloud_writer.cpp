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

struct Point {
  float x;
  float y;
  float z;
};
// Utility class to generate random points on a sphere on demand.
class PointGenerator {
private:
  std::mt19937 _generator;
  std::uniform_real_distribution<float> _distribution;

public:
  PointGenerator(uint32_t seed = 0)
      : _generator(seed)
      , _distribution(0.0, 1.0) {}

  Point next(float scale) {
    float theta = 2 * M_PI * _distribution(_generator);
    float phi = acos(1 - 2 * _distribution(_generator));
    Point point;
    point.x = float((sin(phi) * cos(theta)) * scale);
    point.y = float((sin(phi) * sin(theta)) * scale);
    point.z = float(cos(phi) * scale);
    // std::cout << "x: " << point.x << " y: " << point.y << " z: " << point.z << std::endl;
    return point;
  }
};

mcap::Timestamp now() {
  return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());
}

void write_float_little_endian(float input, std::string* output, size_t offset) {
  static_assert(sizeof(uint32_t) == sizeof(float));
  uint32_t as_int = *reinterpret_cast<uint32_t*>(&input);
  // little-endian means the LSB gets encoded first.
  assert(output->size() > offset + 3);
  (*output)[offset + 0] = as_int & 0xFF;
  (*output)[offset + 1] = (as_int >> 8) & 0xFF;
  (*output)[offset + 2] = (as_int >> 16) & 0xFF;
  (*output)[offset + 3] = (as_int >> 24) & 0xFF;
}

int main(int, char**) {
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("x-protobuf");
  const char OutputFilename[] = "sphere.mcap";
  std::ofstream out(OutputFilename, std::ios::binary);
  writer.open(out, options);

  mcap::Schema schema("foxglove.PointCloud", "protobuf",
                      std::string_view((char*)(descriptor_pb_bin), descriptor_pb_bin_len));
  writer.addSchema(schema);

  mcap::Channel channel("/pointcloud", "protobuf", schema.id);
  writer.addChannel(channel);

  mcap::Timestamp start_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();

  PointGenerator point_gen;
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

  pcl.set_point_stride(12);
  auto field_x = pcl.add_fields();
  field_x->set_name("x");
  field_x->set_offset(0);
  field_x->set_type(foxglove::PackedElementField_NumericType_FLOAT32);
  auto field_y = pcl.add_fields();
  field_y->set_name("y");
  field_y->set_offset(1 * sizeof(float));
  field_y->set_type(foxglove::PackedElementField_NumericType_FLOAT32);
  auto field_z = pcl.add_fields();
  field_z->set_name("z");
  field_z->set_offset(2 * sizeof(float));
  field_z->set_type(foxglove::PackedElementField_NumericType_FLOAT32);

  pcl.mutable_data()->append(POINTS_PER_CLOUD * FIELDS_PER_POINT * sizeof(float), '\0');
  pcl.set_frame_id("/pointcloud");

  for (uint64_t frame_index = 0; frame_index < 100; ++frame_index) {
    mcap::Timestamp cloud_time = start_time + (frame_index * 100 * NS_PER_MS);
    google::protobuf::Timestamp* timestamp = pcl.mutable_timestamp();
    timestamp->set_seconds(cloud_time / NS_PER_S);
    timestamp->set_nanos(cloud_time % NS_PER_S);
    std::string* data = pcl.mutable_data();
    for (int point_index = 0; point_index < POINTS_PER_CLOUD; ++point_index) {
      auto point = point_gen.next(1.0 + (float(frame_index) / 50.0));
      auto base_offset = (point_index * sizeof(float) * FIELDS_PER_POINT);
      write_float_little_endian(point.x, data, base_offset);
      write_float_little_endian(point.y, data, base_offset + sizeof(float));
      write_float_little_endian(point.z, data, base_offset + (2 * sizeof(float)));
    }

    std::string serialized;
    pcl.SerializeToString(&serialized);

    mcap::Message msg;
    msg.channelId = channel.id;
    msg.sequence = frame_index;
    msg.publishTime = cloud_time;
    msg.logTime = cloud_time;
    msg.data = (std::byte*)(serialized.data());
    msg.dataSize = serialized.size();
    const auto res = writer.write(msg);
    if (!res.ok()) {
      std::cerr << "Failed to write message: " << res.message << "\n";
      writer.terminate();
      out.close();
      std::remove(OutputFilename);
      return 1;
    }
  }
  writer.close();
  return 0;
}