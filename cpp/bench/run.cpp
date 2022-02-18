#include <mcap/writer.hpp>

#include <benchmark/benchmark.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>

constexpr char StringSchema[] = "string data";
constexpr size_t WriteIterations = 10000;

static void BM_McapWriterBufferWriterUnchunkedUnindexed(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create an unchunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.noChunking = true;
  options.noSummary = true;

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterUnchunked(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create an unchunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.noChunking = true;

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterChunked(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.chunkSize = uint64_t(state.range(0));

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterChunkedNoCRC(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.noCRC = true;
  options.chunkSize = uint64_t(state.range(0));

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterChunkedUnindexed(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.noSummary = true;
  options.chunkSize = uint64_t(state.range(0));

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterLZ4(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.chunkSize = uint64_t(state.range(0));
  options.compression = mcap::Compression::Lz4;
  options.compressionLevel = mcap::CompressionLevel(state.range(1));

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterZStd(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.chunkSize = uint64_t(state.range(0));
  options.compression = mcap::Compression::Zstd;
  options.compressionLevel = mcap::CompressionLevel(state.range(1));

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterBufferWriterZStdNoCRC(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.noCRC = true;
  options.chunkSize = uint64_t(state.range(0));
  options.compression = mcap::Compression::Zstd;
  options.compressionLevel = mcap::CompressionLevel(state.range(1));

  // Open an output memory buffer and write the file header
  mcap::BufferWriter out{};
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file to memory
  writer.close();
}

static void BM_McapWriterStreamWriterUnchunked(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create an unchunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.noChunking = true;

  // Open an output file stream and write the file header
  std::ofstream out("benchmark.mcap", std::ios::binary);
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file and delete it
  writer.close();
  std::remove("benchmark.mcap");
}

static void BM_McapWriterStreamWriterChunked(benchmark::State& state) {
  // Create a message payload
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Create a chunked writer using the ros1 profile
  mcap::McapWriter writer;
  auto options = mcap::McapWriterOptions("ros1");
  options.chunkSize = uint64_t(state.range(0));

  // Open an output file stream and write the file header
  std::ofstream out("benchmark.mcap", std::ios::binary);
  writer.open(out, options);

  // Register a Schema record
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", StringSchema);
  writer.addSchema(stdMsgsString);

  // Register a Channel record
  mcap::Channel topic("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(topic);

  // Create a message
  mcap::Message msg;
  msg.channelId = topic.id;
  msg.sequence = 0;
  msg.publishTime = 0;
  msg.logTime = msg.publishTime;
  msg.data = payload.data();
  msg.dataSize = payload.size();

  while (state.KeepRunning()) {
    for (size_t i = 0; i < WriteIterations; i++) {
      writer.write(msg);
      benchmark::ClobberMemory();
    }
  }

  // Finish writing the file and delete it
  writer.close();
  std::remove("benchmark.mcap");
}

int main(int argc, char* argv[]) {
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterUnchunkedUnindexed",
                               BM_McapWriterBufferWriterUnchunkedUnindexed);
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterUnchunked",
                               BM_McapWriterBufferWriterUnchunked);
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterChunked", BM_McapWriterBufferWriterChunked)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->Arg(10000000);
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterChunkedNoCRC",
                               BM_McapWriterBufferWriterChunkedNoCRC)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->Arg(10000000);
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterChunkedUnindexed",
                               BM_McapWriterBufferWriterChunkedUnindexed)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->Arg(10000000);
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterLZ4", BM_McapWriterBufferWriterLZ4)
    ->Args({1, 0})
    ->Args({1, 1})
    ->Args({1, 2})
    ->Args({mcap::DefaultChunkSize, 0})
    ->Args({mcap::DefaultChunkSize, 1})
    ->Args({mcap::DefaultChunkSize, 2});
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterZStd", BM_McapWriterBufferWriterZStd)
    ->Args({1, 0})
    ->Args({1, 1})
    ->Args({1, 2})
    ->Args({1, 3})
    ->Args({1, 4})
    ->Args({mcap::DefaultChunkSize, 0})
    ->Args({mcap::DefaultChunkSize, 1})
    ->Args({mcap::DefaultChunkSize, 2})
    ->Args({mcap::DefaultChunkSize, 3})
    ->Args({mcap::DefaultChunkSize, 4});
  benchmark::RegisterBenchmark("BM_McapWriterBufferWriterZStdNoCRC",
                               BM_McapWriterBufferWriterZStdNoCRC)
    ->Args({mcap::DefaultChunkSize, 0})
    ->Args({mcap::DefaultChunkSize, 1})
    ->Args({mcap::DefaultChunkSize, 2})
    ->Args({mcap::DefaultChunkSize, 3})
    ->Args({mcap::DefaultChunkSize, 4});
  benchmark::RegisterBenchmark("BM_McapWriterStreamWriterUnchunked",
                               BM_McapWriterStreamWriterUnchunked);
  benchmark::RegisterBenchmark("BM_McapWriterStreamWriterChunked", BM_McapWriterStreamWriterChunked)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->Arg(10000000);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
