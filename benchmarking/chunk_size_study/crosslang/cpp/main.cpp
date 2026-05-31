// Cross-language correlation check (C++). Writes then reads a fixed-payload
// MCAP file and reports write/read throughput. Payload bytes are generated
// identically across all languages so the files are byte-comparable.
#define MCAP_IMPLEMENTATION
#include "mcap/reader.hpp"
#include "mcap/writer.hpp"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <time.h>
#include <vector>

static double now_sec() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void fill(std::vector<std::byte>& b, size_t n, uint64_t seq) {
  b.resize(n);
  auto* p = reinterpret_cast<uint8_t*>(b.data());
  for (size_t i = 0; i < n; i++) p[i] = (uint8_t)((i + seq) & 0xff);
}

int main(int argc, char** argv) {
  if (argc != 7) {
    fprintf(stderr, "usage: %s <write|read> <file> <num> <size> <chunk> <none|zstd>\n", argv[0]);
    return 1;
  }
  std::string op = argv[1], file = argv[2], comp = argv[6];
  long num = strtol(argv[3], nullptr, 10);
  long size = strtol(argv[4], nullptr, 10);
  uint64_t chunk = strtoull(argv[5], nullptr, 10);

  if (op == "write") {
    mcap::McapWriterOptions opts("xl");
    opts.library = "cpp";
    opts.chunkSize = chunk;
    opts.compression = comp == "zstd" ? mcap::Compression::Zstd : mcap::Compression::None;
    mcap::McapWriter w;
    if (!w.open(file.c_str(), opts).ok()) {
      fprintf(stderr, "open failed\n");
      return 1;
    }
    mcap::Schema schema("Bench", "jsonschema", "{}");
    w.addSchema(schema);
    mcap::Channel ch("/bench", "json", schema.id);
    w.addChannel(ch);
    std::vector<std::byte> buf;
    fill(buf, (size_t)size, 0);  // one reusable payload, generated outside timing
    double t = now_sec();
    for (long i = 0; i < num; i++) {
      mcap::Message m;
      m.channelId = ch.id;
      m.sequence = (uint32_t)i;
      m.logTime = (uint64_t)i * 1000;
      m.publishTime = m.logTime;
      m.data = buf.data();
      m.dataSize = buf.size();
      if (!w.write(m).ok()) {
        fprintf(stderr, "write failed\n");
        return 1;
      }
    }
    w.close();
    double wall = now_sec() - t;
    FILE* f = fopen(file.c_str(), "rb");
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fclose(f);
    printf("cpp\twrite\t%s\t%ld\t%ld\t%ld\t%.6f\n", comp.c_str(), num, num * size, fsize, wall);
  } else {
    mcap::McapReader r;
    if (!r.open(file.c_str()).ok()) {
      fprintf(stderr, "open failed\n");
      return 1;
    }
    double t = now_sec();
    uint64_t count = 0, bytes = 0;
    auto onProblem = [](const mcap::Status&) {};
    auto view = r.readMessages(onProblem);
    for (auto it = view.begin(); it != view.end(); ++it) {
      count++;
      bytes += it->message.dataSize;
    }
    double wall = now_sec() - t;
    r.close();
    printf("cpp\tread\t%s\t%llu\t%llu\t0\t%.6f\n", comp.c_str(), (unsigned long long)count,
           (unsigned long long)bytes, wall);
  }
  return 0;
}
