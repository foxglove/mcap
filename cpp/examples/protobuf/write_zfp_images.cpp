// Example code for writing ZFP-compressed images to Protobuf messages in an
// MCAP file. This executable writes a sequence of foxglove.CompressedImage,
// foxglove.CameraCalibration, and foxglove.ImageAnnotations messages to an
// MCAP which should render in 2D and 3D panes in Foxglove Studio.
#define MCAP_IMPLEMENTATION
#include "BuildFileDescriptorSet.h"
#include "foxglove/CameraCalibration.pb.h"
#include "foxglove/CompressedImage.pb.h"
#include "foxglove/ImageAnnotations.pb.h"
#include "mcap/writer.hpp"
#include "zfp.h"
#include <cmath>

constexpr int64_t NS_PER_S = 1000000000;
constexpr int64_t NS_PER_MS = 1000000;

void timestamp(google::protobuf::Timestamp* ts, int64_t time_ns) {
  ts->set_seconds(time_ns / NS_PER_S);
  ts->set_nanos(int32_t(time_ns % NS_PER_S));
}

void rgba(foxglove::Color* color, double r, double g, double b, double a) {
  color->set_r(r);
  color->set_g(g);
  color->set_b(b);
  color->set_a(a);
}

void writeMessage(mcap::McapWriter& writer, int i, mcap::ChannelId channelId,
                  const std::string& serialized) {
  mcap::Message msg;
  msg.channelId = channelId;
  msg.sequence = i;
  msg.publishTime = i * NS_PER_MS * 100;
  msg.logTime = msg.publishTime;
  msg.data = reinterpret_cast<const std::byte*>(serialized.data());
  msg.dataSize = serialized.size();

  const auto res = writer.write(msg);
  if (!res.ok()) {
    std::cerr << "Failed to write message: " << res.message << "\n";
    writer.terminate();
    writer.close();
    std::exit(1);
  }
}

void writeFrame(mcap::McapWriter& writer, int i, mcap::ChannelId imageChannelId,
                mcap::ChannelId calChannelId, mcap::ChannelId annotationsChannelId) {
  constexpr int width = 1280;
  constexpr int height = 720;

  int64_t time_ns = i * 100000000;  // step time by 0.1 seconds

  // foxglove.CompressedImage
  {
    // Create a `width`x`height` floating-point depth image with a moving gradient
    float maxDist = std::sqrt(float(width * width + height * height));
    std::vector<float> rgb8Data(width * height);
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < width; x++) {
        int dx = x - (i * 10);
        int dy = y - (i * 10);
        float dist = std::sqrt(float(dx * dx + dy * dy));
        rgb8Data[y * width + x] = float(dist / maxDist);
      }
    }

    // Compress 2D data with ZFP (lossy) and a full header
    zfp_type type = zfp_type_float;                                      // float32 data
    zfp_field* field = zfp_field_2d(&rgb8Data[0], type, width, height);  // must be 2D
    zfp_field_set_pointer(field, &rgb8Data[0]);     // set pointer to our float32 data
    zfp_stream* stream = zfp_stream_open(nullptr);  // allocate a new zfp stream
    zfp_stream_set_accuracy(stream, 0.01);          // lossy: set tolerance (max error) to 0.01
    size_t zfpSize = zfp_stream_maximum_size(stream, field);  // compressed data size upper bound
    std::vector<uint8_t> zfpData(zfpSize);                    // allocate buffer for compressed data
    bitstream* bs = stream_open(&zfpData[0], zfpSize);        // wrap output buffer in a bitstream
    zfp_stream_set_bit_stream(stream, bs);                    // attach bitstream to zfp stream
    zfp_stream_rewind(stream);                                // rewind bitstream to begin writing
    zfp_write_header(stream, field, ZFP_HEADER_FULL);     // prefix with a full header (required)
    size_t compressedSize = zfp_compress(stream, field);  // compress 2D float32 data
    zfp_stream_flush(stream);                             // flush, deallocate, and close streams
    zfp_field_free(field);
    zfp_stream_close(stream);

    // Create a foxglove.CompressedImage message with the ZFP data
    foxglove::CompressedImage rgb8;
    timestamp(rgb8.mutable_timestamp(), time_ns);
    rgb8.set_frame_id("camera");
    rgb8.set_format("zfp");
    rgb8.set_data(&zfpData[0], compressedSize);

    // Write the message to the MCAP file
    writeMessage(writer, i, imageChannelId, rgb8.SerializeAsString());
  }

  // foxglove.CameraCalibration
  {
    constexpr double focal_length_mm = 12.0;
    constexpr double sensor_width_mm = 12.0;
    constexpr double sensor_height_mm = sensor_width_mm * height / width;

    double fx = width * (focal_length_mm / sensor_width_mm);
    double fy = height * (focal_length_mm / sensor_height_mm);
    double cx = width / 2;
    double cy = height / 2;

    foxglove::CameraCalibration cal;
    timestamp(cal.mutable_timestamp(), time_ns);
    cal.set_frame_id("camera");
    cal.set_distortion_model("plumb_bob");
    cal.set_width(width);
    cal.set_height(height);
    cal.mutable_d()->Reserve(5);
    cal.mutable_k()->Reserve(9);
    cal.mutable_r()->Reserve(9);
    cal.mutable_p()->Reserve(12);
    // clang-format off
    /* D */ cal.add_d(0); cal.add_d(0); cal.add_d(0); cal.add_d(0); cal.add_d(0);
    /* K */ cal.add_k(fx); cal.add_k(0); cal.add_k(cx);
            cal.add_k(0); cal.add_k(fy); cal.add_k(cy);
            cal.add_k(0); cal.add_k(0); cal.add_k(1);
    /* R */ cal.add_r(1); cal.add_r(0); cal.add_r(0);
            cal.add_r(0); cal.add_r(1); cal.add_r(0);
            cal.add_r(0); cal.add_r(0); cal.add_r(1);
    /* P */ cal.add_p(fx); cal.add_p(0); cal.add_p(cx); cal.add_p(0);
            cal.add_p(0); cal.add_p(fy); cal.add_p(cy); cal.add_p(0);
            cal.add_p(0); cal.add_p(0); cal.add_p(1); cal.add_p(0);
    // clang-format on

    writeMessage(writer, i, calChannelId, cal.SerializeAsString());
  }

  // foxglove.ImageAnnotations
  {
    double x = i * 10;
    double y = i * 10;
    foxglove::ImageAnnotations ann;
    auto* circle = ann.add_circles();
    timestamp(circle->mutable_timestamp(), time_ns);
    circle->mutable_position()->set_x(x);
    circle->mutable_position()->set_y(y);
    circle->set_diameter(30);
    circle->set_thickness(1);
    rgba(circle->mutable_fill_color(), 0, 0, 0, 0);
    rgba(circle->mutable_outline_color(), 1, 0, 0, 1);

    writeMessage(writer, i, annotationsChannelId, ann.SerializeAsString());
  }
}

mcap::ChannelId addTopic(mcap::McapWriter& writer, const std::string_view topicName,
                         const std::string_view schemaName,
                         const google::protobuf::Descriptor* descriptor) {
  mcap::Schema schema{schemaName, "protobuf",
                      foxglove::BuildFileDescriptorSet(descriptor).SerializeAsString()};
  writer.addSchema(schema);

  mcap::Channel channel{topicName, "protobuf", schema.id};
  writer.addChannel(channel);
  return channel.id;
}

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

  auto imageChannelId = addTopic(writer, "/camera/image", "foxglove.CompressedImage",
                                 foxglove::CompressedImage::descriptor());
  auto cameraInfoChannelId = addTopic(writer, "/camera/calibration", "foxglove.CameraCalibration",
                                      foxglove::CameraCalibration::descriptor());
  auto imageAnnotationsChannelId =
    addTopic(writer, "/camera/annotations", "foxglove.ImageAnnotations",
             foxglove::ImageAnnotations::descriptor());

  // Write a sequence of frames to the file
  for (int i = 0; i < 100; i++) {
    writeFrame(writer, i, imageChannelId, cameraInfoChannelId, imageAnnotationsChannelId);
  }

  writer.close();
  return 0;
}
