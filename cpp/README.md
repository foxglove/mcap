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

The library, its tests, examples, and benchmarks are one CMake project rooted
at `cpp/CMakeLists.txt`. The `conanfile.py` next to it drives that project
through [Conan](https://conan.io/) 2.x, which supplies the dependencies.

Run `make` to build everything using a Docker container. This requires Docker
to be installed, and will produce Linux (ELF) binaries compiled in an Ubuntu
container with the clang compiler. If you want to build binaries for another
platform or using your own compiler, run `make build-host`. This requires a
working C++ compiler toolchain and Conan 2.x to be installed.

Output binaries can be found in:

- `cpp/build/Debug/bin/` for the unit and conformance tests
- `cpp/build/Release/bin/` for the examples and benchmarks

The developer targets are selected by the Conan options `build_tests`,
`build_examples`, `build_bench` and `build_docs`, which map onto the CMake
options `MCAP_BUILD_TESTS`, `MCAP_BUILD_EXAMPLES`, `MCAP_BUILD_BENCH` and
`MCAP_BUILD_DOCS`. `build.sh` shows how they are passed.

The CMake project can also be configured without Conan if lz4 and zstd are
installed on the system, for example to install the library into a prefix:

```
cmake -S cpp -B build -DCMAKE_INSTALL_PREFIX=/opt/mcap
cmake --install build
```

## Including in your project

The C++ implementation of MCAP is maintained as a header-only library with the
following dependencies:

- [lz4](https://lz4.github.io/lz4/) (tested with [lz4/1.10.0](https://conan.io/center/lz4))
- [zstd](https://facebook.github.io/zstd/) (tested with [zstd/1.5.2](https://conan.io/center/zstd))

If your project does not need `lz4` or `zstd` support, you can optionally disable these by defining
`MCAP_COMPRESSION_NO_LZ4` or `MCAP_COMPRESSION_NO_ZSTD` respectively.

### Conan

To simplify installation of dependencies, the [Conan](https://conan.io/) package
manager can be used with the included
[conanfile.py](https://github.com/foxglove/mcap/blob/main/cpp/conanfile.py).

### CMake

`cpp/CMakeLists.txt` defines the `mcap::mcap` target, which carries the include
directory, the C++17 requirement and the lz4 and zstd dependencies. It can be
consumed in three ways:

- With `add_subdirectory` or `FetchContent` pointing at the `cpp` directory of
  this repository. Only the library is built unless the `MCAP_BUILD_*` options
  are turned on.
- With `find_package(mcap)` against an installed copy (see Building above).
- With `find_package(mcap)` against the Conan package, which generates its own
  CMake config.

The project's CMakeLists.txt lives in `cpp`, not at the repository root, so a
`FetchContent` consumer must point `SOURCE_SUBDIR` at it. This requires CMake
3.18 or newer, which is also the minimum the project itself declares:

```cmake
include(FetchContent)
FetchContent_Declare(mcap
  GIT_REPOSITORY https://github.com/foxglove/mcap.git
  GIT_TAG        releases/cpp/vX.Y.Z  # must be >= 2.3.0
  SOURCE_SUBDIR  cpp)
FetchContent_MakeAvailable(mcap)

target_link_libraries(my_app PRIVATE mcap::mcap)
```

lz4 and zstd are located by the `FindLZ4.cmake` and `FindZstd.cmake` modules in
`cpp/cmake`, which accept a CMake package config (such as one generated by
Conan), pkg-config, or a plain header and library search. Define
`MCAP_COMPRESSION_NO_LZ4` or `MCAP_COMPRESSION_NO_ZSTD` when configuring to
drop the corresponding dependency.

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

1. Update the `#define MCAP_LIBRARY_VERSION` in `mcap/include/mcap/types.hpp`. This is the only place the version is written down; `CMakeLists.txt` and `conanfile.py` read it from there.
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
