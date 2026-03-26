#define MCAP_IMPLEMENTATION
#include "mcap/writer.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unordered_map>
#include <utility>
#include <vector>

static void fill_payload(std::vector<std::byte>& buf, bool varied) {
  for (size_t i = 0; i < buf.size(); i++) {
    buf[i] = varied ? std::byte((i * 137 + 43) & 0xff) : std::byte{0x42};
  }
}

int main(int argc, char* argv[])
{
  if (argc != 6) {
    fprintf(stderr, "Usage: %s <output_file> <mode> <num_messages> <payload_size> <uniform|varied>\n", argv[0]);
    fprintf(stderr, "  mode: unchunked | chunked | zstd | lz4\n");
    return 1;
  }

  const char* filename = argv[1];
  const char* mode = argv[2];
  bool mixed_mode = (strcmp(argv[4], "mixed") == 0);
  long num_messages = mixed_mode ? 0 : strtol(argv[3], nullptr, 10);
  long payload_size = mixed_mode ? 0 : strtol(argv[4], nullptr, 10);
  bool varied_fill = (strcmp(argv[5], "varied") == 0);

  if (!mixed_mode && (num_messages <= 0 || payload_size <= 0)) {
    fprintf(stderr, "num_messages and payload_size must be positive\n");
    return 1;
  }

  mcap::McapWriterOptions opts("bench");
  opts.library = "cpp-bench";

  if (strcmp(mode, "unchunked") == 0) {
    opts.noChunking = true;
    opts.compression = mcap::Compression::None;
  } else if (strcmp(mode, "chunked") == 0) {
    opts.chunkSize = 786432;
    opts.compression = mcap::Compression::None;
  } else if (strcmp(mode, "zstd") == 0) {
    opts.chunkSize = 786432;
    opts.compression = mcap::Compression::Zstd;
  } else if (strcmp(mode, "lz4") == 0) {
    opts.chunkSize = 786432;
    opts.compression = mcap::Compression::Lz4;
  } else {
    fprintf(stderr, "Unknown mode: %s\n", mode);
    return 1;
  }

  mcap::McapWriter writer;
  auto res = writer.open(filename, opts);
  if (!res.ok()) {
    fprintf(stderr, "Failed to open writer: %s\n", res.message.c_str());
    return 1;
  }

  if (mixed_mode) {
    /* Mixed payload mode: simulate a 10-second robot recording */

    /* Channel definitions: topic, schema_name, payload_size(s), period_ns, count */
    struct ChannelDef {
      const char* topic;
      const char* schema_name;
      std::vector<size_t> payload_sizes;
      uint64_t period_ns;
      long count;
    };

    ChannelDef channel_defs[] = {
      {"/imu",                "IMU",             {96},                           5000000ULL,   2000},
      {"/odom",               "Odometry",        {296},                         20000000ULL,    500},
      {"/tf",                 "TFMessage",       {80, 160, 320, 800, 1600},    10000000ULL,   1000},
      {"/lidar",              "PointCloud2",     {230400},                     100000000ULL,    100},
      {"/camera/compressed",  "CompressedImage", {524288},                     66666667ULL,    150},
    };
    const int num_channels = 5;

    /* Register schemas and channels (not timed) */
    mcap::Schema schemas[5];
    mcap::Channel channels[5];
    for (int c = 0; c < num_channels; c++) {
      schemas[c] = mcap::Schema(channel_defs[c].schema_name, "jsonschema", "{\"type\":\"object\"}");
      writer.addSchema(schemas[c]);
      channels[c] = mcap::Channel(channel_defs[c].topic, "json", schemas[c].id);
      writer.addChannel(channels[c]);
    }

    /* Pre-generate sorted message schedule: (timestamp, channel_index) */
    struct ScheduleEntry {
      uint64_t timestamp;
      int channel_index;
    };

    std::vector<ScheduleEntry> schedule;
    schedule.reserve(3750);
    for (int c = 0; c < num_channels; c++) {
      for (long i = 0; i < channel_defs[c].count; i++) {
        ScheduleEntry e;
        e.timestamp = static_cast<uint64_t>(i) * channel_defs[c].period_ns;
        e.channel_index = c;
        schedule.push_back(e);
      }
    }
    std::sort(schedule.begin(), schedule.end(), [](const ScheduleEntry& a, const ScheduleEntry& b) {
      if (a.timestamp != b.timestamp) return a.timestamp < b.timestamp;
      return a.channel_index < b.channel_index;
    });

    /* Pre-allocate payload buffers keyed by size */
    size_t unique_sizes[] = {96, 296, 80, 160, 320, 800, 1600, 230400, 524288};
    std::unordered_map<size_t, std::vector<std::byte>> payload_bufs;
    for (int i = 0; i < 9; i++) {
      auto& buf = payload_bufs[unique_sizes[i]];
      buf.resize(unique_sizes[i]);
      fill_payload(buf, varied_fill);
    }

    num_messages = 3750;

    /* Time the message-writing loop + close */
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* Track per-channel sequence numbers for tf cycling */
    long chan_seq[5] = {0, 0, 0, 0, 0};

    for (size_t i = 0; i < schedule.size(); i++) {
      const auto& entry = schedule[i];
      int c = entry.channel_index;
      const auto& cdef = channel_defs[c];

      /* Determine payload buffer */
      size_t psize;
      if (cdef.payload_sizes.size() == 1) {
        psize = cdef.payload_sizes[0];
      } else {
        psize = cdef.payload_sizes[static_cast<size_t>(chan_seq[c]) % cdef.payload_sizes.size()];
      }

      auto& pbuf = payload_bufs[psize];

      mcap::Message msg;
      msg.channelId = channels[c].id;
      msg.sequence = static_cast<uint32_t>(chan_seq[c]);
      msg.logTime = entry.timestamp;
      msg.publishTime = entry.timestamp;
      msg.data = pbuf.data();
      msg.dataSize = psize;
      auto wres = writer.write(msg);
      if (!wres.ok()) {
        fprintf(stderr, "Failed to write message %zu: %s\n", i,
                wres.message.c_str());
        writer.close();
        return 1;
      }

      chan_seq[c]++;
    }

    writer.close();

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    struct stat st;
    if (stat(filename, &st) != 0) {
      fprintf(stderr, "Failed to stat file\n");
      return 1;
    }
    long file_size = static_cast<long>(st.st_size);

    long long elapsed_ns = (long long)(t_end.tv_sec - t_start.tv_sec) * 1000000000LL
                          + (long long)(t_end.tv_nsec - t_start.tv_nsec);
    double wall_sec = static_cast<double>(elapsed_ns) / 1e9;

    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);

    /* TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb */
    printf("write\tcpp\t%s\t%ld\t%s\t%ld\t%lld\t%.6f\t%ld\n",
           mode, num_messages, "mixed", file_size, elapsed_ns, wall_sec, ru.ru_maxrss);

  } else {
    /* Fixed payload mode (original code path) */

    /* Create payload buffer */
    std::vector<std::byte> payload(static_cast<size_t>(payload_size));
    fill_payload(payload, varied_fill);

    /* Schema (not timed) */
    mcap::Schema schema("BenchMsg", "jsonschema", "{\"type\":\"object\"}");
    writer.addSchema(schema);

    /* Channel (not timed) */
    mcap::Channel channel("/bench", "json", schema.id);
    writer.addChannel(channel);

    /* Time the message-writing loop + close */
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (long i = 0; i < num_messages; i++) {
      mcap::Message msg;
      msg.channelId = channel.id;
      msg.sequence = static_cast<uint32_t>(i);
      msg.logTime = static_cast<uint64_t>(i) * 1000;
      msg.publishTime = msg.logTime;
      msg.data = payload.data();
      msg.dataSize = payload.size();
      auto wres = writer.write(msg);
      if (!wres.ok()) {
        fprintf(stderr, "Failed to write message %ld: %s\n", i,
                wres.message.c_str());
        writer.close();
        return 1;
      }
    }

    writer.close();

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    struct stat st;
    if (stat(filename, &st) != 0) {
      fprintf(stderr, "Failed to stat file\n");
      return 1;
    }
    long file_size = static_cast<long>(st.st_size);

    long long elapsed_ns = (long long)(t_end.tv_sec - t_start.tv_sec) * 1000000000LL
                          + (long long)(t_end.tv_nsec - t_start.tv_nsec);
    double wall_sec = static_cast<double>(elapsed_ns) / 1e9;

    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);

    /* TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb */
    printf("write\tcpp\t%s\t%ld\t%ld\t%ld\t%lld\t%.6f\t%ld\n",
           mode, num_messages, payload_size, file_size, elapsed_ns, wall_sec, ru.ru_maxrss);
  }

  return 0;
}
