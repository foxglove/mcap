// Chunk-size study benchmark for the MCAP C++ writer/reader.
//
// One invocation performs exactly one operation (a single write or a single
// read of one access pattern) so that wall-clock timing and peak RSS are
// isolated per measurement. The surrounding run.sh drives the full sweep.
//
// The message-payload model (channel rates/sizes for the "mixed" corpus) is
// adapted from the cross-language benchmark proposed in foxglove/mcap#1611.
// This study extends it with (a) a configurable chunk size, (b) distinct
// message-size classes with realistic compressibility, (c) several read
// access patterns, and (d) a counting IReadable that records bytes fetched
// and read() calls so remote-storage cost can be modeled analytically.

#define MCAP_IMPLEMENTATION
#include "mcap/reader.hpp"
#include "mcap/writer.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <vector>

namespace {

constexpr double kPi = 3.14159265358979323846;

double now_sec() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
}

long peak_rss_kb() {
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  return ru.ru_maxrss;  // KiB on Linux
}

long file_size_bytes(const char* path) {
  struct stat st;
  if (stat(path, &st) != 0) {
    return -1;
  }
  return static_cast<long>(st.st_size);
}

// ---------------------------------------------------------------------------
// Payload generators with realistic compressibility per content type.
// All write into a caller-owned, reused buffer (no per-message allocation).
// ---------------------------------------------------------------------------

// Small structured telemetry (IMU-like): a constant header region (frame id,
// flags) followed by slowly-varying fixed-point fields. Small-magnitude
// fixed-point values keep their high bytes near-constant and change little
// between consecutive messages, so zstd exploits the cross-message redundancy
// -- which is why real IMU/odom/tf streams compress several-fold.
void fill_small(std::vector<std::byte>& buf, uint64_t seq) {
  constexpr size_t kSize = 100;
  buf.resize(kSize);
  std::memset(buf.data(), 0, kSize);
  static const char tag[16] = {'i', 'm', 'u', '_', 'l', 'i', 'n', 'k', 0, 0, 1, 0, 0, 0, 0, 0};
  std::memcpy(buf.data(), tag, 16);
  // 20 int32 fixed-point fields (scale 1000) of slowly varying signals.
  double t = static_cast<double>(seq) * 0.005;
  int32_t fields[20];
  for (int i = 0; i < 20; i++) {
    double v = std::sin(t * (0.1 + 0.02 * i) + i) * (i < 4 ? 1.0 : 9.81);
    fields[i] = static_cast<int32_t>(std::lround(v * 1000.0));
  }
  std::memcpy(buf.data() + 16, fields, sizeof(fields));
  uint32_t s = static_cast<uint32_t>(seq);
  std::memcpy(buf.data() + 96, &s, 4);
}

// Already-compressed image (JPEG-like): pseudo-random bytes. Entropy is high,
// so zstd cannot shrink it -- this is the "compression can't help you" regime.
void fill_incompressible(std::vector<std::byte>& buf, size_t size, uint64_t seq) {
  buf.resize(size);
  uint64_t state = 0x9e3779b97f4a7c15ULL ^ (seq * 0x100000001b3ULL);
  auto* out = reinterpret_cast<uint8_t*>(buf.data());
  for (size_t i = 0; i < size; i++) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    out[i] = static_cast<uint8_t>(state >> 24);
  }
}

// Realistic point cloud (PointCloud2-like): N points of float32 x,y,z,intensity.
// Points lie on a smooth swept surface with mild quantization and a little
// noise, giving the partial redundancy (~1.5-2x) typical of lidar scans.
void fill_pointcloud(std::vector<std::byte>& buf, size_t size, uint64_t seq) {
  buf.resize(size);
  const size_t num_points = size / 16;
  auto* f = reinterpret_cast<float*>(buf.data());
  double phase = static_cast<double>(seq) * 0.01;
  uint64_t state = 0x2545f4914f6cdd1dULL ^ seq;
  for (size_t i = 0; i < num_points; i++) {
    double az = (static_cast<double>(i) / static_cast<double>(num_points)) * 2.0 * kPi * 16.0;
    double el = std::sin(static_cast<double>(i) * 0.001 + phase) * 0.3;
    double r = 10.0 + 4.0 * std::sin(az * 0.5 + phase);
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    double jitter = (static_cast<double>(state & 0xff) / 255.0 - 0.5) * 0.05;
    // Quantize to a few cm so the low mantissa bits are stable/compressible.
    float x = std::round((r * std::cos(az) * std::cos(el) + jitter) * 100.0f) / 100.0f;
    float y = std::round((r * std::sin(az) * std::cos(el) + jitter) * 100.0f) / 100.0f;
    float z = std::round((r * std::sin(el)) * 100.0f) / 100.0f;
    float intensity = static_cast<float>((state >> 8) & 0x3f);
    f[i * 4 + 0] = x;
    f[i * 4 + 1] = y;
    f[i * 4 + 2] = z;
    f[i * 4 + 3] = intensity;
  }
}

// ---------------------------------------------------------------------------
// Mixed corpus channel schedule (adapted from foxglove/mcap#1611).
// ---------------------------------------------------------------------------
struct MixedChannel {
  const char* topic;
  const char* schema;
  std::vector<size_t> payload_sizes;  // cycles per message
  uint64_t period_ns;
  const char* content;  // "small" | "incompressible" | "pointcloud"
};

const std::vector<MixedChannel>& mixed_channels() {
  static const std::vector<MixedChannel> defs = {
    {"/imu", "IMU", {100}, 5000000ULL, "small"},
    {"/odom", "Odometry", {296}, 20000000ULL, "small"},
    {"/tf", "TFMessage", {80, 160, 320, 800, 1600}, 10000000ULL, "small"},
    {"/lidar", "PointCloud2", {1500000}, 100000000ULL, "pointcloud"},
    {"/camera/compressed", "CompressedImage", {150000}, 66666667ULL, "incompressible"},
  };
  return defs;
}

void fill_content(const std::string& content, std::vector<std::byte>& buf, size_t size,
                  uint64_t seq) {
  if (content == "small") {
    if (size == 100) {
      fill_small(buf, seq);
    } else {
      // odom/tf: structured-ish, reuse small generator tiled to size.
      buf.resize(size);
      std::vector<std::byte> unit;
      fill_small(unit, seq);
      for (size_t off = 0; off < size; off += unit.size()) {
        size_t n = std::min(unit.size(), size - off);
        std::memcpy(buf.data() + off, unit.data(), n);
      }
    }
  } else if (content == "pointcloud") {
    fill_pointcloud(buf, size, seq);
  } else {
    fill_incompressible(buf, size, seq);
  }
}

void apply_compression(mcap::McapWriterOptions& opts, const std::string& comp) {
  if (comp == "zstd") {
    opts.compression = mcap::Compression::Zstd;
  } else if (comp == "lz4") {
    opts.compression = mcap::Compression::Lz4;
  } else {
    opts.compression = mcap::Compression::None;
  }
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------
int do_write(const char* file, const std::string& cls, uint64_t chunk_bytes, uint64_t target_bytes,
             const std::string& comp) {
  mcap::McapWriterOptions opts("chunkstudy");
  opts.library = "cpp-chunkstudy";
  opts.chunkSize = chunk_bytes;
  apply_compression(opts, comp);

  mcap::McapWriter writer;
  auto res = writer.open(file, opts);
  if (!res.ok()) {
    fprintf(stderr, "open writer failed: %s\n", res.message.c_str());
    return 1;
  }

  uint64_t num_msgs = 0;
  uint64_t total_payload = 0;
  std::vector<std::byte> buf;

  double t_start = now_sec();

  if (cls == "mixed") {
    const auto& defs = mixed_channels();
    struct Reg {
      mcap::Schema schema;
      mcap::Channel channel;
    };
    std::vector<Reg> regs;
    regs.reserve(defs.size());
    for (const auto& d : defs) {
      Reg r{mcap::Schema(d.schema, "jsonschema", "{\"type\":\"object\"}"), mcap::Channel()};
      writer.addSchema(r.schema);
      r.channel = mcap::Channel(d.topic, "cdr", r.schema.id);
      writer.addChannel(r.channel);
      regs.push_back(r);
    }
    // Rate-driven schedule: every channel runs concurrently at its own period
    // over a single recording duration, like a real robot log. The duration is
    // chosen so the total uncompressed payload is ~target_bytes.
    struct Entry {
      uint64_t ts;
      uint32_t ch;
      uint32_t seq;
      size_t size;
    };
    double bytes_per_ns = 0.0;
    for (const auto& d : defs) {
      double avg = 0.0;
      for (auto s : d.payload_sizes) avg += static_cast<double>(s);
      avg /= static_cast<double>(d.payload_sizes.size());
      bytes_per_ns += avg / static_cast<double>(d.period_ns);
    }
    uint64_t duration_ns = static_cast<uint64_t>(static_cast<double>(target_bytes) / bytes_per_ns);
    std::vector<Entry> schedule;
    for (uint32_t c = 0; c < defs.size(); c++) {
      const auto& d = defs[c];
      uint64_t k = 0;
      for (uint64_t ts = 0; ts < duration_ns; ts += d.period_ns, k++) {
        size_t size = d.payload_sizes[k % d.payload_sizes.size()];
        schedule.push_back({ts, c, static_cast<uint32_t>(k), size});
        total_payload += size;
      }
    }
    std::sort(schedule.begin(), schedule.end(),
              [](const Entry& a, const Entry& b) { return a.ts < b.ts; });
    for (const auto& e : schedule) {
      fill_content(defs[e.ch].content, buf, e.size, e.seq);
      mcap::Message m;
      m.channelId = regs[e.ch].channel.id;
      m.sequence = e.seq;
      m.logTime = e.ts;
      m.publishTime = e.ts;
      m.data = buf.data();
      m.dataSize = buf.size();
      auto wr = writer.write(m);
      if (!wr.ok()) {
        fprintf(stderr, "write failed: %s\n", wr.message.c_str());
        writer.close();
        return 1;
      }
      num_msgs++;
    }
  } else {
    // Single-class corpus on one channel.
    std::string topic, schema_name, content;
    size_t payload = 0;
    uint64_t period_ns = 0;
    if (cls == "small") {
      topic = "/imu";
      schema_name = "IMU";
      content = "small";
      payload = 100;
      period_ns = 5000000ULL;  // 200 Hz
    } else if (cls == "jpeg") {
      topic = "/camera/compressed";
      schema_name = "CompressedImage";
      content = "incompressible";
      payload = 150000;
      period_ns = 50000000ULL;  // 20 Hz
    } else if (cls == "pointcloud") {
      topic = "/lidar";
      schema_name = "PointCloud2";
      content = "pointcloud";
      payload = 1500000;
      period_ns = 100000000ULL;  // 10 Hz
    } else {
      fprintf(stderr, "unknown class: %s\n", cls.c_str());
      return 1;
    }
    mcap::Schema schema(schema_name, "jsonschema", "{\"type\":\"object\"}");
    writer.addSchema(schema);
    mcap::Channel channel(topic, "cdr", schema.id);
    writer.addChannel(channel);

    uint64_t seq = 0;
    while (total_payload < target_bytes) {
      fill_content(content, buf, payload, seq);
      mcap::Message m;
      m.channelId = channel.id;
      m.sequence = static_cast<uint32_t>(seq);
      m.logTime = seq * period_ns;
      m.publishTime = m.logTime;
      m.data = buf.data();
      m.dataSize = buf.size();
      auto wr = writer.write(m);
      if (!wr.ok()) {
        fprintf(stderr, "write failed: %s\n", wr.message.c_str());
        writer.close();
        return 1;
      }
      total_payload += payload;
      num_msgs++;
      seq++;
    }
  }

  writer.close();
  double wall = now_sec() - t_start;
  long fsize = file_size_bytes(file);

  // Write rows pad the read-only columns with 0 so every row has equal width.
  // op class chunk comp pattern msgs payload_bytes chunks_touched
  // chunk_fetched_bytes summary_bytes raw_fetched raw_reads file_size wall rss
  printf("write\t%s\t%llu\t%s\t-\t%llu\t%llu\t0\t0\t0\t0\t0\t%ld\t%.6f\t%ld\n", cls.c_str(),
         (unsigned long long)chunk_bytes, comp.c_str(), (unsigned long long)num_msgs,
         (unsigned long long)total_payload, fsize, wall, peak_rss_kb());
  return 0;
}

// ---------------------------------------------------------------------------
// Counting IReadable: wraps a FILE* like mcap::FileReader but records the
// number of read() calls (remote round-trips) and bytes fetched (transfer
// volume). These feed an analytic remote-latency model in analyze.py.
// ---------------------------------------------------------------------------
class CountingReadable final : public mcap::IReadable {
public:
  explicit CountingReadable(std::FILE* f)
      : inner_(f) {}
  uint64_t size() const override {
    return inner_.size();
  }
  uint64_t read(std::byte** output, uint64_t offset, uint64_t size) override {
    uint64_t n = inner_.read(output, offset, size);
    reads_++;
    bytes_ += n;
    return n;
  }
  uint64_t reads() const {
    return reads_;
  }
  uint64_t bytes() const {
    return bytes_;
  }

private:
  mcap::FileReader inner_;
  uint64_t reads_ = 0;
  uint64_t bytes_ = 0;
};

int do_read(const char* file, const std::string& cls, uint64_t chunk_bytes, const std::string& comp,
            const std::string& pattern) {
  std::FILE* fp = std::fopen(file, "rb");
  if (!fp) {
    fprintf(stderr, "fopen failed: %s\n", file);
    return 1;
  }
  CountingReadable source(fp);

  mcap::McapReader reader;
  auto res = reader.open(source);
  if (!res.ok()) {
    fprintf(stderr, "open reader failed: %s\n", res.message.c_str());
    std::fclose(fp);
    return 1;
  }
  auto sres = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
  if (!sres.ok()) {
    fprintf(stderr, "read summary failed: %s\n", sres.message.c_str());
    reader.close();
    std::fclose(fp);
    return 1;
  }

  mcap::Timestamp t0 = 0, t1 = mcap::MaxTime;
  uint64_t count = 1;
  const auto& stats = reader.statistics();
  if (stats) {
    t0 = stats->messageStartTime;
    t1 = stats->messageEndTime;
    count = stats->messageCount > 0 ? stats->messageCount : 1;
  }
  uint64_t duration = t1 > t0 ? t1 - t0 : 1;
  uint64_t avg_spacing = duration / (count > 1 ? count - 1 : 1);
  if (avg_spacing == 0) avg_spacing = 1;
  uint64_t mid = t0 + duration / 2;

  mcap::ReadMessageOptions opts;
  opts.readOrder = mcap::ReadMessageOptions::ReadOrder::FileOrder;
  mcap::Timestamp win_start = 0, win_end = mcap::MaxTime;
  if (pattern == "full") {
    win_start = 0;
    win_end = mcap::MaxTime;
  } else if (pattern == "point") {
    win_start = mid;
    win_end = mid + avg_spacing;
  } else if (pattern == "range") {
    uint64_t half = duration / 200;  // 1% window
    win_start = mid > half ? mid - half : 0;
    win_end = mid + half;
  } else if (pattern == "streaming") {
    uint64_t half = (duration * 15) / 200;  // 15% window
    win_start = mid > half ? mid - half : 0;
    win_end = mid + half;
  } else if (pattern == "topic") {
    win_start = 0;
    win_end = mcap::MaxTime;
    opts.topicFilter = [](std::string_view t) { return t == "/imu"; };
  } else {
    fprintf(stderr, "unknown pattern: %s\n", pattern.c_str());
    reader.close();
    std::fclose(fp);
    return 1;
  }
  opts.startTime = win_start;
  opts.endTime = win_end;

  // Reader-independent remote cost inputs, derived from the chunk index: how
  // many chunks overlap the query window (= ranged GETs an ideal object-store
  // reader issues) and the compressed bytes it must transfer for them. A topic
  // filter without a time bound still touches every chunk.
  uint64_t chunks_touched = 0;
  uint64_t chunk_fetched_bytes = 0;
  for (const auto& ci : reader.chunkIndexes()) {
    bool overlaps = !(ci.messageEndTime < win_start || ci.messageStartTime >= win_end);
    if (overlaps) {
      chunks_touched++;
      chunk_fetched_bytes += ci.compressedSize;
    }
  }
  // Summary/index section size: fetched once per query to enable indexed reads.
  uint64_t summary_bytes = 0;
  const auto& footer = reader.footer();
  long fsize_pre = file_size_bytes(file);
  if (footer && footer->summaryStart != 0 && fsize_pre > 0) {
    summary_bytes = static_cast<uint64_t>(fsize_pre) - footer->summaryStart;
  }

  double t_start = now_sec();
  uint64_t msgs = 0;
  uint64_t bytes_decoded = 0;
  auto onProblem = [](const mcap::Status& s) { fprintf(stderr, "reader problem: %s\n", s.message.c_str()); };
  auto view = reader.readMessages(onProblem, opts);
  for (auto it = view.begin(); it != view.end(); ++it) {
    msgs++;
    bytes_decoded += it->message.dataSize;
    if (it->message.dataSize == 0) {
      fprintf(stderr, "empty message\n");
    }
  }
  double wall = now_sec() - t_start;
  long rss = peak_rss_kb();
  long fsize = file_size_bytes(file);

  reader.close();
  std::fclose(fp);

  // TSV columns (see run.sh header): op class chunk comp pattern msgs
  // payload_bytes chunks_touched chunk_fetched_bytes summary_bytes raw_fetched
  // raw_reads file_size wall rss
  printf("read\t%s\t%llu\t%s\t%s\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%ld\t%.6f\t%ld\n",
         cls.c_str(), (unsigned long long)chunk_bytes, comp.c_str(), pattern.c_str(),
         (unsigned long long)msgs, (unsigned long long)bytes_decoded,
         (unsigned long long)chunks_touched, (unsigned long long)chunk_fetched_bytes,
         (unsigned long long)summary_bytes, (unsigned long long)source.bytes(),
         (unsigned long long)source.reads(), fsize, wall, rss);
  return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc < 2) {
    fprintf(stderr,
            "Usage:\n"
            "  %s write <file> <class> <chunk_bytes> <target_bytes> <zstd|lz4|none>\n"
            "  %s read  <file> <class> <chunk_bytes> <zstd|lz4|none> <full|point|range|streaming|topic>\n"
            "  class: small | jpeg | pointcloud | mixed\n",
            argv[0], argv[0]);
    return 1;
  }
  std::string op = argv[1];
  if (op == "write") {
    if (argc != 7) {
      fprintf(stderr, "write needs 5 args\n");
      return 1;
    }
    return do_write(argv[2], argv[3], strtoull(argv[4], nullptr, 10), strtoull(argv[5], nullptr, 10),
                    argv[6]);
  } else if (op == "read") {
    if (argc != 7) {
      fprintf(stderr, "read needs 5 args\n");
      return 1;
    }
    return do_read(argv[2], argv[3], strtoull(argv[4], nullptr, 10), argv[5], argv[6]);
  }
  fprintf(stderr, "unknown op: %s\n", op.c_str());
  return 1;
}
