#define MCAP_IMPLEMENTATION

#include "descriptor.pb.h"
#include "foxglove/PointCloud.pb.h"
#include "mcap/writer.hpp"
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <random>

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

  Point write(float scale) {
    float theta = 2 * M_PI * _distribution(_generator);
    float phi = acos(1 - 2 * _distribution(_generator));
    Point point;
    point.x = float((sin(phi) * cos(theta)) * scale);
    point.y = float((sin(phi) * sin(theta)) * scale);
    point.z = float(cos(phi) * scale);
    return point;
  }
};

mcap::Timestamp now() {
  return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());
}

void write_float_little_endian(float input, char* output) {
  static_assert(sizeof(uint32_t) == sizeof(float));
  uint32_t as_int = *reinterpret_cast<uint32_t*>(&input);
  // little-endian means the LSB gets encoded first.
  output[0] = as_int & 0xFF;
  output[1] = (as_int >> 8) & 0xFF;
  output[2] = (as_int >> 16) & 0xFF;
  output[3] = (as_int >> 24) & 0xFF;
}

int main(int, char**) {
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("x-protobuf");
  const char OutputFilename[] = "sphere.mcap";
  std::ofstream out(OutputFilename, std::ios::binary);

  mcap::Schema schema("foxglove.PointCloud", "protobuf",
                      std::string_view((char*)(descriptor_pb_bin), descriptor_pb_bin_len));
  writer.addSchema(schema);

  mcap::Channel channel("/pointcloud", "protobuf", schema.id);
  writer.addChannel(channel);

  auto start_time = now();

  PointGenerator point_gen;

  for (uint64_t i = 0; i < 100; ++i) {
    mcap::Timestamp cloud_time = start_time + (i * 1000 * 1000);

    foxglove::PointCloud pcl;
    google::protobuf::Timestamp timestamp;
    timestamp.set_seconds(cloud_time / 1000000000);
    timestamp.set_nanos(cloud_time % 1000000000);
    pcl.set_allocated_timestamp(&timestamp);

    foxglove::Pose pose;
    foxglove::Vector3 position;
    position.set_x(0);
    position.set_y(0);
    position.set_z(0);
    foxglove::Quaternion orientation;
    orientation.set_x(0);
    orientation.set_y(0);
    orientation.set_z(0);
    orientation.set_w(1);
    pose.set_allocated_orientation(&orientation);
    pose.set_allocated_position(&position);
    pcl.set_allocated_pose(&pose);

    pcl.set_point_stride(12);
    auto field_x = pcl.add_fields();
    field_x->set_name("x");
    field_x->set_offset(0);
    field_x->set_type(foxglove::PackedElementField_NumericType_FLOAT32);
    auto field_y = pcl.add_fields();
    field_y->set_name("y");
    field_y->set_offset(4);
    field_y->set_type(foxglove::PackedElementField_NumericType_FLOAT32);
    auto field_z = pcl.add_fields();
    field_z->set_name("z");
    field_z->set_offset(8);
    field_z->set_type(foxglove::PackedElementField_NumericType_FLOAT32);

    std::string data(1000 * 12, '\0');
    for (int point_index = 0; point_index < 1000; ++point_index) {
      auto point = point_gen.write(float(i) / 100.0);
      write_float_little_endian(point.x, &data[point_index * 12]);
      write_float_little_endian(point.y, &data[(point_index * 12) + 4]);
      write_float_little_endian(point.z, &data[(point_index * 12) + 8]);
    }
    pcl.set_allocated_data(&data);
    pcl.set_frame_id("/pointcloud");

    std::string serialized;
    pcl.SerializeToString(&serialized);

    mcap::Message msg;
    msg.channelId = channel.id;
    msg.sequence = i;
    msg.publishTime = start_time + (i * 1e6);
    msg.logTime = msg.publishTime;
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
  return 0;
}