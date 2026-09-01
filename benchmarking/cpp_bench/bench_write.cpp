#define MCAP_IMPLEMENTATION
#include "mcap/writer.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <utility>
#include <vector>

/* Shared payload blob parameters; must match gen_blob.py and the other
 * language benches. Message i's payload is the window of the blob starting
 * at (i * kStride) % kWindowSpan, so all implementations feed identical
 * bytes to their writers. */
static const size_t kBlobSize = 16777216;
static const size_t kMaxPayload = 524288;
static const size_t kWindowSpan = kBlobSize - kMaxPayload;
static const uint64_t kStride = 7919;

static size_t payload_offset(uint64_t msg_index)
{
  return static_cast<size_t>((msg_index * kStride) % kWindowSpan);
}

static bool load_blob(const char* path, std::vector<std::byte>& blob)
{
  FILE* f = fopen(path, "rb");
  if (f == nullptr) {
    fprintf(stderr, "Failed to open blob file: %s\n", path);
    return false;
  }
  blob.resize(kBlobSize);
  size_t nread = fread(blob.data(), 1, kBlobSize, f);
  bool at_eof = (fgetc(f) == EOF);
  fclose(f);
  if (nread != kBlobSize || !at_eof) {
    fprintf(stderr, "Blob file %s is not exactly %zu bytes\n", path, kBlobSize);
    return false;
  }
  return true;
}

/* CRC-32 (IEEE, zlib-compatible) over the payload stream, used by
 * run_bench.sh to verify all languages fed identical bytes. */
static uint32_t crc32_update(uint32_t crc, const std::byte* data, size_t len)
{
  static uint32_t table[256];
  static bool table_init = false;
  if (!table_init) {
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int k = 0; k < 8; k++) {
        c = (c & 1) ? 0xEDB88320U ^ (c >> 1) : c >> 1;
      }
      table[i] = c;
    }
    table_init = true;
  }
  crc ^= 0xFFFFFFFFU;
  for (size_t i = 0; i < len; i++) {
    crc = table[(crc ^ static_cast<uint32_t>(data[i])) & 0xFFU] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFU;
}

/* ru_maxrss is KB on Linux but bytes on macOS; normalize to KB. */
static long peak_rss_kb(void)
{
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
#ifdef __APPLE__
  return ru.ru_maxrss / 1024;
#else
  return ru.ru_maxrss;
#endif
}

int main(int argc, char* argv[])
{
  if (argc != 6) {
    fprintf(stderr, "Usage: %s <output_file> <mode> <num_messages> <payload_size> <blob_file>\n", argv[0]);
    fprintf(stderr, "  mode: unchunked | chunked | zstd | lz4\n");
    return 1;
  }

  const char* filename = argv[1];
  const char* mode = argv[2];
  bool mixed_mode = (strcmp(argv[4], "mixed") == 0);
  long num_messages = mixed_mode ? 0 : strtol(argv[3], nullptr, 10);
  long payload_size = mixed_mode ? 0 : strtol(argv[4], nullptr, 10);

  if (!mixed_mode && (num_messages <= 0 || payload_size <= 0)) {
    fprintf(stderr, "num_messages and payload_size must be positive\n");
    return 1;
  }
  if (!mixed_mode && static_cast<size_t>(payload_size) > kMaxPayload) {
    fprintf(stderr, "payload_size must be <= %zu\n", kMaxPayload);
    return 1;
  }

  std::vector<std::byte> blob;
  if (!load_blob(argv[5], blob)) {
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

    num_messages = 3750;

    /* Not timed: CRC of the payload stream for cross-language verification */
    uint32_t payload_crc = 0;
    {
      long crc_seq[5] = {0, 0, 0, 0, 0};
      for (size_t i = 0; i < schedule.size(); i++) {
        int c = schedule[i].channel_index;
        const auto& cdef = channel_defs[c];
        size_t psize = cdef.payload_sizes[static_cast<size_t>(crc_seq[c]) % cdef.payload_sizes.size()];
        crc_seq[c]++;
        payload_crc = crc32_update(payload_crc, blob.data() + payload_offset(i), psize);
      }
    }

    /* Time the message-writing loop + close */
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* Track per-channel sequence numbers for tf cycling */
    long chan_seq[5] = {0, 0, 0, 0, 0};

    for (size_t i = 0; i < schedule.size(); i++) {
      const auto& entry = schedule[i];
      int c = entry.channel_index;
      const auto& cdef = channel_defs[c];

      /* Determine payload size (cycles for /tf) and blob window */
      size_t psize;
      if (cdef.payload_sizes.size() == 1) {
        psize = cdef.payload_sizes[0];
      } else {
        psize = cdef.payload_sizes[static_cast<size_t>(chan_seq[c]) % cdef.payload_sizes.size()];
      }

      mcap::Message msg;
      msg.channelId = channels[c].id;
      msg.sequence = static_cast<uint32_t>(chan_seq[c]);
      msg.logTime = entry.timestamp;
      msg.publishTime = entry.timestamp;
      msg.data = blob.data() + payload_offset(i);
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

    /* TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb payload_crc32 */
    printf("write\tcpp\t%s\t%ld\t%s\t%ld\t%lld\t%.6f\t%ld\t%u\n",
           mode, num_messages, "mixed", file_size, elapsed_ns, wall_sec, peak_rss_kb(),
           payload_crc);

  } else {
    /* Fixed payload mode (original code path) */

    /* Schema (not timed) */
    mcap::Schema schema("BenchMsg", "jsonschema", "{\"type\":\"object\"}");
    writer.addSchema(schema);

    /* Channel (not timed) */
    mcap::Channel channel("/bench", "json", schema.id);
    writer.addChannel(channel);

    /* Not timed: CRC of the payload stream for cross-language verification */
    uint32_t payload_crc = 0;
    for (long i = 0; i < num_messages; i++) {
      payload_crc = crc32_update(payload_crc, blob.data() + payload_offset(static_cast<uint64_t>(i)),
                                 static_cast<size_t>(payload_size));
    }

    /* Time the message-writing loop + close */
    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (long i = 0; i < num_messages; i++) {
      mcap::Message msg;
      msg.channelId = channel.id;
      msg.sequence = static_cast<uint32_t>(i);
      msg.logTime = static_cast<uint64_t>(i) * 1000;
      msg.publishTime = msg.logTime;
      msg.data = blob.data() + payload_offset(static_cast<uint64_t>(i));
      msg.dataSize = static_cast<size_t>(payload_size);
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

    /* TSV output: op lang mode num_msgs payload_size file_size elapsed_ns wall_sec peak_rss_kb payload_crc32 */
    printf("write\tcpp\t%s\t%ld\t%ld\t%ld\t%lld\t%.6f\t%ld\t%u\n",
           mode, num_messages, payload_size, file_size, elapsed_ns, wall_sec, peak_rss_kb(),
           payload_crc);
  }

  return 0;
}
