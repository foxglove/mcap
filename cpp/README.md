# C++ implementation of the MCAP file format

## Example writer

<!-- cspell: disable -->

```cpp
#include <mcap/writer.hpp>

#include <chrono>
#include <cstring>
#include <iostream>

// Returns the system time in nanoseconds. std::chrono is used here, but any
// high resolution clock API (such as clock_gettime) can be used.
mcap::Timestamp now() {
  return mcap::Timestamp(std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());
}

int main() {
  // Initialize an MCAP writer with the "ros1" profile and write the file header
  mcap::McapWriter writer;
  auto status = writer.open("output.mcap", mcap::McapWriterOptions("ros1"));
  if (!status.ok()) {
    std::cerr << "Failed to open MCAP file for writing: " << status.message << "\n";
    return 1;
  }

  // Register a Schema
  mcap::Schema stdMsgsString("std_msgs/String", "ros1msg", "string data");
  writer.addSchema(stdMsgsString);

  // Register a Channel
  mcap::Channel chatterPublisher("/chatter", "ros1", stdMsgsString.id);
  writer.addChannel(chatterPublisher);

  // Create a message payload. This would typically be done by your own
  // serialiation library. In this example, we manually create ROS1 binary data
  std::array<std::byte, 4 + 13> payload;
  const uint32_t length = 13;
  std::memcpy(payload.data(), &length, 4);
  std::memcpy(payload.data() + 4, "Hello, world!", 13);

  // Write our message
  mcap::Message msg;
  msg.channelId = chatterPublisher.id;
  msg.sequence = 1; // Optional
  msg.logTime = now(); // Required nanosecond timestamp
  msg.publishTime = msg.logTime; // Set to logTime if not available
  msg.data = payload.data();
  msg.dataSize = payload.size();
  writer.write(msg);

  // Finish writing the file
  writer.close();
}
```

<!-- cspell: enable -->

## Building

Run `make` to build the library using a Docker container. This requires Docker
to be installed, and will produce Linux (ELF) binaries compiled in an Ubuntu
container with the clang compiler. If you want to build binaries for another
platform or using your own compiler, run `make build-host`. This requires a
working C++ compiler toolchain and [Conan](https://conan.io/) to be installed.

Output binaries can be found in:

- `cpp/bench/build/Release/bin/`,
- `cpp/examples/build/Release/bin/`
- `cpp/test/build/Debug/bin/`.

## Including in your project

The C++ implementation of MCAP is maintained as a header-only library with the
following dependencies:

- [cryptopp](https://cryptopp.com/) (tested with [cryptopp/8.5.0](https://conan.io/center/cryptopp))
- [fmt](https://github.com/fmtlib/fmt) (tested with [fmt/8.1.1](https://conan.io/center/fmt))
- [lz4](https://lz4.github.io/lz4/) (tested with [lz4/1.9.3](https://conan.io/center/lz4))
- [zstd](https://facebook.github.io/zstd/) (tested with [zstd/1.5.2](https://conan.io/center/zstd))

To simplify installation of dependencies, the [Conan](https://conan.io/) package
manager can be used with the included
[conanfile.py](https://github.com/foxglove/mcap/blob/main/cpp/mcap/conanfile.py).
Alternatively, you can link against system libraries. On Ubuntu/Debian systems,
use the following command to install the dependencies:

```bash
sudo apt install libcrypto++-dev libfmt-dev liblz4-dev libzstd-dev
```

## Usage

Refer to the API documentation in
[mcap/mcap.hpp](https://github.com/foxglove/mcap/blob/main/cpp/mcap/include/mcap/mcap.hpp)
for full details. The high-level interfaces for reading and writing are
`McapReader` and `McapWriter`.
