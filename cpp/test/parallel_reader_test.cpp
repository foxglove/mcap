#define MCAP_IMPLEMENTATION
#include <mcap/mcap.hpp>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace mcap;
using Order = ReadMessageOptions::ReadOrder;

namespace {

// ---- synthetic file generation ------------------------------------------------

struct Spec {
  std::string name;
  Compression compression;
  uint64_t chunkSize;
  int channels;
  uint32_t perChannel;
  size_t payload;
  bool overlap;      // interleave channels so chunk time-ranges overlap
  bool tie = false;  // all channels share identical timestamps (RecordOffset tie-break)
};

std::string writeFile(const std::filesystem::path& dir, const Spec& s) {
  const auto path = (dir / (s.name + ".mcap")).string();
  McapWriterOptions o{"parallel-test"};
  o.compression = s.compression;
  o.chunkSize = s.chunkSize;
  o.forceCompression = (s.compression != Compression::None);
  McapWriter w;
  REQUIRE(w.open(path, o).ok());
  Schema sc{"m", "raw", std::string_view{"x"}};
  w.addSchema(sc);
  std::vector<Channel> ch;
  for (int c = 0; c < s.channels; c++) {
    Channel k{"/t" + std::to_string(c), "raw", sc.id};
    w.addChannel(k);
    ch.push_back(k);
  }
  std::vector<std::byte> payload(s.payload);
  auto one = [&](int c, uint32_t seq) {
    const Timestamp t = s.tie
                          ? Timestamp(uint64_t(seq) * 1000)
                          : Timestamp((uint64_t(seq) * uint64_t(s.channels) + uint64_t(c)) * 1000);
    std::memcpy(payload.data(), &t, sizeof(t));  // identity-encode for divergence diagnostics
    Message m;
    m.channelId = ch[size_t(c)].id;
    m.sequence = seq;
    m.logTime = t;
    m.publishTime = t;
    m.dataSize = payload.size();
    m.data = payload.data();
    REQUIRE(w.write(m).ok());
  };
  if (s.overlap) {
    for (int c = 0; c < s.channels; c++)
      for (uint32_t seq = 0; seq < s.perChannel; seq++) one(c, seq);
  } else {
    for (uint32_t seq = 0; seq < s.perChannel; seq++)
      for (int c = 0; c < s.channels; c++) one(c, seq);
  }
  w.close();
  return path;
}

// Portable unique temp directory (avoids POSIX getpid(), which is undeclared on MSVC).
std::filesystem::path uniqueTempDir(const std::string& tag) {
  static std::atomic<uint64_t> counter{0};
  std::random_device rd;
  const uint64_t unique = (uint64_t(rd()) << 32) ^ uint64_t(rd()) ^ (counter.fetch_add(1) << 1);
  auto dir = std::filesystem::temp_directory_path() / (tag + std::to_string(unique));
  std::filesystem::create_directories(dir);
  return dir;
}

// ---- canonical message + comparison -------------------------------------------

struct Canonical {
  Timestamp logTime;
  uint16_t channelId;
  uint32_t sequence;
  std::string data;
};

Canonical canonical(const MessageView& mv) {
  return Canonical{
    mv.message.logTime, uint16_t(mv.message.channelId), mv.message.sequence,
    std::string(reinterpret_cast<const char*>(mv.message.data), mv.message.dataSize)};
}

bool sameSequence(const std::vector<Canonical>& a, const std::vector<Canonical>& b,
                  std::string& detail) {
  if (a.size() != b.size()) {
    detail = "size " + std::to_string(a.size()) + " != " + std::to_string(b.size());
    return false;
  }
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i].logTime != b[i].logTime || a[i].channelId != b[i].channelId ||
        a[i].sequence != b[i].sequence || a[i].data != b[i].data) {
      detail = "first divergence at index " + std::to_string(i);
      return false;
    }
  }
  return true;
}

std::vector<Canonical> serialMessages(const std::string& path, const ReadMessageOptions& opts) {
  std::FILE* fp = std::fopen(path.c_str(), "rb");
  REQUIRE(fp != nullptr);
  FileReader src(fp);
  McapReader reader;
  REQUIRE(reader.open(src).ok());
  REQUIRE(reader.readSummary(ReadSummaryMethod::AllowFallbackScan).ok());
  std::vector<Canonical> out;
  auto view = reader.readMessages([](const Status&) {}, opts);
  for (const auto& mv : view) out.push_back(canonical(mv));
  std::fclose(fp);
  return out;
}

std::vector<Canonical> parallelMessages(const std::string& path, const ParallelReadOptions& opts) {
  ParallelReader pr;
  REQUIRE(pr.open(path).ok());
  std::vector<Canonical> out;
  auto view = pr.readMessages([](const Status&) {}, opts);
  for (const auto& mv : view) out.push_back(canonical(mv));
  REQUIRE(view.status().ok());
  return out;
}

ParallelReadOptions parallelOpts(Order order, unsigned threads) {
  ParallelReadOptions o;
  o.read.readOrder = order;
  o.threadCount = threads;
  o.maxBytesInFlight = 16ull * 1024 * 1024;
  o.lookaheadBytes = 16ull * 1024 * 1024;
  return o;
}

}  // namespace

TEST_CASE("ParallelReader matches the serial reader", "[parallel][parity]") {
  auto dir = uniqueTempDir("mcap_par_");

  std::vector<Spec> specs;
  specs.push_back({"none_overlap", Compression::None, 8 * 1024, 6, 300, 256, true});
  specs.push_back({"none_disjoint", Compression::None, 8 * 1024, 6, 300, 256, false});
#ifndef MCAP_COMPRESSION_NO_ZSTD
  specs.push_back({"zstd_overlap", Compression::Zstd, 8 * 1024, 8, 300, 256, true});
  specs.push_back({"zstd_disjoint", Compression::Zstd, 8 * 1024, 8, 300, 256, false});
  specs.push_back({"zstd_single_chunk", Compression::Zstd, 64 * 1024 * 1024, 8, 300, 256, true});
  specs.push_back({"zstd_many_small", Compression::Zstd, 4 * 1024, 12, 500, 64, true});
  specs.push_back({"zstd_ties", Compression::Zstd, 8 * 1024, 8, 300, 256, true, /*tie*/ true});
#endif
#ifndef MCAP_COMPRESSION_NO_LZ4
  specs.push_back({"lz4_overlap", Compression::Lz4, 8 * 1024, 6, 300, 256, true});
  specs.push_back({"lz4_ties", Compression::Lz4, 8 * 1024, 6, 300, 256, true, /*tie*/ true});
#endif

  const std::vector<Order> orders = {Order::LogTimeOrder, Order::ReverseLogTimeOrder};
  const std::vector<unsigned> threadCounts = {1, 2, 4, 8};

  for (const auto& spec : specs) {
    const auto path = writeFile(dir, spec);
    for (Order order : orders) {
      ReadMessageOptions sOpts;
      sOpts.readOrder = order;
      const auto expected = serialMessages(path, sOpts);
      for (unsigned threads : threadCounts) {
        DYNAMIC_SECTION(spec.name << " order=" << int(order) << " threads=" << threads) {
          const auto got = parallelMessages(path, parallelOpts(order, threads));
          std::string detail;
          const bool equal = sameSequence(expected, got, detail);
          INFO(detail);
          REQUIRE(equal);
        }
      }
    }
  }
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST_CASE("ParallelReader is deterministic across repeated runs", "[parallel][parity]") {
  auto dir = uniqueTempDir("mcap_par_det_");
  const auto path = writeFile(dir, {"det", Compression::None, 8 * 1024, 8, 400, 256, true});
  const auto base = parallelMessages(path, parallelOpts(Order::LogTimeOrder, 8));
  for (int run = 0; run < 8; run++) {
    const auto again = parallelMessages(path, parallelOpts(Order::LogTimeOrder, 8));
    std::string detail;
    INFO("run " << run << ": " << detail);
    REQUIRE(sameSequence(base, again, detail));
  }
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST_CASE("ParallelReader honors time-range and topic filters", "[parallel][parity]") {
  auto dir = uniqueTempDir("mcap_par_flt_");
  const auto path = writeFile(dir, {"flt", Compression::None, 8 * 1024, 8, 400, 256, true});

  ReadMessageOptions allOpts;
  allOpts.readOrder = Order::LogTimeOrder;
  const auto all = serialMessages(path, allOpts);
  REQUIRE_FALSE(all.empty());
  const Timestamp mid = all[all.size() / 2].logTime;

  SECTION("time range") {
    ReadMessageOptions sOpts;
    sOpts.readOrder = Order::LogTimeOrder;
    sOpts.endTime = mid;
    const auto expected = serialMessages(path, sOpts);

    auto pOpts = parallelOpts(Order::LogTimeOrder, 4);
    pOpts.read.endTime = mid;
    const auto got = parallelMessages(path, pOpts);

    std::string detail;
    INFO(detail);
    REQUIRE(sameSequence(expected, got, detail));
  }

  SECTION("topic filter") {
    auto topic0 = [](std::string_view t) {
      return t == "/t0";
    };
    ReadMessageOptions sOpts;
    sOpts.readOrder = Order::LogTimeOrder;
    sOpts.topicFilter = topic0;
    const auto expected = serialMessages(path, sOpts);

    auto pOpts = parallelOpts(Order::LogTimeOrder, 4);
    pOpts.read.topicFilter = topic0;
    const auto got = parallelMessages(path, pOpts);

    std::string detail;
    INFO(detail);
    REQUIRE(sameSequence(expected, got, detail));
    REQUIRE(got.size() < all.size());
  }

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST_CASE("ParallelReader rejects a non-concurrent source", "[parallel][robust]") {
  auto dir = uniqueTempDir("mcap_par_nc_");
  const auto path = writeFile(dir, {"nc", Compression::None, 8 * 1024, 4, 100, 256, true});

  // A FileReader is single-cursor (supportsConcurrentRead() == false), so the view
  // must surface an error and yield nothing rather than read unsafely.
  std::FILE* fp = std::fopen(path.c_str(), "rb");
  REQUIRE(fp != nullptr);
  FileReader src(fp);
  ParallelReader pr;
  REQUIRE(pr.open(src).ok());  // open succeeds (summary read serially)
  ParallelReadOptions opts = parallelOpts(Order::LogTimeOrder, 4);
  size_t n = 0;
  auto view = pr.readMessages([](const Status&) {}, opts);
  for (auto it = view.begin(); it != view.end(); ++it) n++;
  CHECK(n == 0);
  CHECK_FALSE(view.status().ok());
  std::fclose(fp);

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST_CASE("ParallelReader early teardown is clean", "[parallel][robust]") {
  auto dir = uniqueTempDir("mcap_par_et_");
  const auto path = writeFile(dir, {"et", Compression::None, 8 * 1024, 8, 400, 256, true});
  {
    ParallelReader pr;
    REQUIRE(pr.open(path).ok());
    auto opts = parallelOpts(Order::LogTimeOrder, 8);
    auto view = pr.readMessages([](const Status&) {}, opts);
    auto it = view.begin();
    for (int i = 0; i < 5 && it != view.end(); i++) ++it;
    // pr destroyed mid-iteration here: workers cancelled, pool joined, no hang.
  }
  SUCCEED("early teardown completed");
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

// ---- memory-cap unit tests ----------------------------------------------------

namespace {

ChunkIndex mkChunk(Timestamp start, Timestamp end, uint64_t uncompressed, ChannelId ch = 1) {
  ChunkIndex c;
  c.messageStartTime = start;
  c.messageEndTime = end;
  c.uncompressedSize = uncompressed;
  c.compressedSize = uncompressed / 2;
  c.messageIndexOffsets[ch] = 0;
  return c;
}

struct BruteResult {
  uint64_t maxChunks = 0;
  uint64_t maxBytes = 0;
};

BruteResult bruteForce(const std::vector<ChunkIndex>& cs) {
  std::vector<Timestamp> instants;
  for (const auto& c : cs) {
    instants.push_back(c.messageStartTime);
    instants.push_back(c.messageEndTime);
  }
  BruteResult r;
  for (Timestamp t : instants) {
    uint64_t n = 0, b = 0;
    for (const auto& c : cs) {
      if (c.messageStartTime <= t && t <= c.messageEndTime) {
        n++;
        b += c.uncompressedSize;
      }
    }
    r.maxChunks = std::max(r.maxChunks, n);
    r.maxBytes = std::max(r.maxBytes, b);
  }
  return r;
}

}  // namespace

TEST_CASE("residency profile matches a brute-force oracle", "[parallel][cap]") {
  std::mt19937 rng(12345);
  std::uniform_int_distribution<uint64_t> startD(0, 1000);
  std::uniform_int_distribution<uint64_t> lenD(0, 200);
  std::uniform_int_distribution<uint64_t> sizeD(1, 5000);
  for (int trial = 0; trial < 200; trial++) {
    std::vector<ChunkIndex> cs;
    const int n = int(rng() % 40);
    for (int i = 0; i < n; i++) {
      const Timestamp s = startD(rng);
      cs.push_back(mkChunk(s, s + lenD(rng), sizeD(rng)));
    }
    const auto p = computeResidencyProfile(cs);
    const auto b = bruteForce(cs);
    INFO("trial " << trial << " n=" << n);
    REQUIRE(p.maxDepthChunks == b.maxChunks);
    REQUIRE(p.maxDepthBytes == b.maxBytes);
  }
}

TEST_CASE("byte semaphore tryAcquire never exceeds capacity under concurrency",
          "[parallel][cap]") {
  const uint64_t cap = 1u << 20;
  internal::ByteSemaphore sem(cap);
  std::atomic<int64_t> live{0};
  std::atomic<bool> violated{false};
  std::vector<std::thread> threads;
  for (unsigned t = 0; t < 8; t++) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(t + 1);
      std::uniform_int_distribution<uint64_t> sizeD(1, cap);
      for (int i = 0; i < 2000; i++) {
        const uint64_t k = sizeD(rng);
        if (!sem.tryAcquire(k)) continue;
        const int64_t now = live.fetch_add(int64_t(k)) + int64_t(k);
        if (now > int64_t(cap)) violated.store(true);
        live.fetch_sub(int64_t(k));
        sem.release(k);
      }
    });
  }
  for (auto& th : threads) th.join();
  REQUIRE_FALSE(violated.load());
}

TEST_CASE("byte semaphore forceAcquire admits an oversized chunk", "[parallel][cap]") {
  internal::ByteSemaphore sem(100);
  sem.forceAcquire(250);  // oversized: drives available negative, never blocks
  CHECK(sem.outstanding() == 250);
  CHECK_FALSE(sem.tryAcquire(50));  // no room while the oversized chunk is held
  sem.release(250);
  CHECK(sem.tryAcquire(50));
  sem.release(50);
}

TEST_CASE("budget resolver enforces the log-time overlap floor", "[parallel][cap]") {
  ResidencyProfile prof;
  prof.consideredChunks = 200;
  prof.maxDepthChunks = 30;
  prof.maxDepthBytes = 30ull << 20;
  prof.uMaxBytes = 1ull << 20;
  prof.totalUncompressed = 150ull << 20;
  const uint64_t MiB = 1ull << 20;

  SECTION("file order floor is the largest chunk; log-time floor is the overlap depth") {
    const auto file = resolveBudget(prof, Order::FileOrder, /*cap*/ 0);
    const auto log = resolveBudget(prof, Order::LogTimeOrder, /*cap*/ 0);
    CHECK(file.floorBytes == prof.uMaxBytes);
    CHECK(log.floorBytes == prof.maxDepthBytes);
    CHECK(log.floorBytes > file.floorBytes);
  }

  SECTION("cap above floor is honored and used for look-ahead") {
    const auto d =
      resolveBudget(prof, Order::LogTimeOrder, 64 * MiB, MemoryCapPolicy::Adapt, 8 * MiB);
    CHECK(d.feasibleWithoutEviction);
    CHECK_FALSE(d.exceedsUserCap);
    CHECK(d.effectiveBudgetBytes == prof.maxDepthBytes + 8 * MiB);
  }

  SECTION("Adapt raises a sub-floor cap to the floor and flags it") {
    const auto d = resolveBudget(prof, Order::LogTimeOrder, 8 * MiB, MemoryCapPolicy::Adapt);
    CHECK(d.effectiveBudgetBytes == prof.maxDepthBytes);
    CHECK(d.exceedsUserCap);
    CHECK(d.feasibleWithoutEviction);
    CHECK_FALSE(d.note.empty());
  }

  SECTION("Strict reports infeasible without exceeding the cap") {
    const auto d = resolveBudget(prof, Order::LogTimeOrder, 8 * MiB, MemoryCapPolicy::Strict);
    CHECK(d.effectiveBudgetBytes == 8 * MiB);
    CHECK_FALSE(d.feasibleWithoutEviction);
  }

  SECTION("FallBackToSerial signals the caller when the cap is below floor") {
    const auto d =
      resolveBudget(prof, Order::LogTimeOrder, 8 * MiB, MemoryCapPolicy::FallBackToSerial);
    CHECK(d.fallBackToSerial);
  }
}
