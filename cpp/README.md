# C++ implementation of the MCAP file format

## Example writer

<!-- cspell: disable -->

```cpp
#define MCAP_IMPLEMENTATION  // Define this in exactly one .cpp file
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
  // serialization library. In this example, we manually create ROS1 binary data
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

- [lz4](https://lz4.github.io/lz4/) (tested with [lz4/1.9.3](https://conan.io/center/lz4))
- [zstd](https://facebook.github.io/zstd/) (tested with [zstd/1.5.2](https://conan.io/center/zstd))

If your project does not need `lz4` or `zstd` support, you can optionally disable these by defining
`MCAP_COMPRESSION_NO_LZ4` or `MCAP_COMPRESSION_NO_ZSTD` respectively.

### Conan

To simplify installation of dependencies, the [Conan](https://conan.io/) package
manager can be used with the included
[conanfile.py](https://github.com/foxglove/mcap/blob/main/cpp/mcap/conanfile.py).

### CMake

For using MCAP with CMake, the third-party [olympus-robotics/mcap_builder](https://github.com/olympus-robotics/mcap_builder) repository provides a helpful wrapper.

There is also a third party-maintained [vcpkg](https://vcpkg.io/en/) package for [`mcap`](https://vcpkg.io/en/package/mcap), which provides a CMake package.

### Alternatives

If you use an alternative approach, such as CMake's FetchContent or directly
vendoring the dependencies, make sure you use versions equal or greater than the
versions listed above.

## Usage

Refer to the API documentation in
[mcap/mcap.hpp](https://github.com/foxglove/mcap/blob/main/cpp/mcap/include/mcap/mcap.hpp)
for full details. The high-level interfaces for reading and writing are
`McapReader` and `McapWriter`.

### Visibility

By default, the MCAP library will attempt to export its symbols from the translation unit where
`MCAP_IMPLEMENTATION` is defined, and import them elsewhere. See `mcap/visibility.hpp` for exact
semantics. If your application requires something different, you can define the `MCAP_PUBLIC` macro
before including the library.

```cpp
// use the MCAP library internally but keep all symbols hidden
#define MCAP_IMPLEMENTATION
#define MCAP_PUBLIC __attribute__((visibility("hidden")))
#include <mcap/writer.hpp>
```

## Releasing new versions

1. Update the `#define MCAP_LIBRARY_VERSION` and all other occurrences of the same version number, e.g. in `conanfile.py`, `build.sh`, and others.
1. Once the version number has been updated, create and push a git tag named `releases/cpp/vX.Y.Z` matching the new version number.
1. Make a pull request to [conan-io/conan-center-index](https://github.com/conan-io/conan-center-index) to update the [mcap recipe](https://github.com/conan-io/conan-center-index/tree/master/recipes/mcap):
   - Update [`config.yml`](https://github.com/conan-io/conan-center-index/blob/master/recipes/mcap/config.yml) to add the new version.
   - Update [`all/conandata.yml`](https://github.com/conan-io/conan-center-index/blob/master/recipes/mcap/all/conandata.yml) to add an entry pointing at the tarball from the new release tag. <!-- cspell: word conandata -->
   - Follow the instructions for [developing recipes locally](https://github.com/conan-io/conan-center-index/blob/master/docs/developing_recipes_locally.md) to test the recipe.
   - Examples of previous changes to the mcap recipe: https://github.com/conan-io/conan-center-index/commits/master/recipes/mcap

## Changes to APIs

This project uses a [semantic version](https://semver.org) number to notify users of changes to public APIs.
This semantic version can be read from `types.hpp`, defined as `MCAP_LIBRARY_VERSION`.

The public API includes names that can be included from the `.hpp` files in `include/mcap`, excluding anything namespaced under `mcap::internal`.

This API version does not cover the compiled ABI of the library. Projects including `mcap` are expected
to compile it from source as part of their build process.

Build rules in CMake or `conanfile.py` files are not covered as part of this public API.
