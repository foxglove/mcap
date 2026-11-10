cmake_minimum_required(VERSION 3.18)
project(mcap_cpp_bench CXX)

# -O2 -g without the -DNDEBUG RelWithDebInfo would add, so the library's
# asserts stay enabled as before.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

# -std=c++17, not gnu++17.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# run_bench.sh expects the binaries next to the sources.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")

# Consume the mcap project directly, so header changes rebuild the benches.
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../../cpp" mcap)

add_library(bench_flags INTERFACE)
# -fno-omit-frame-pointer keeps perf/hotspot profiles readable.
# Measured cost of C++ frame pointers on the fixed-payload path: below
# the suite's run-to-run noise floor (<±3%). The Rust bench deliberately
# does NOT force frame pointers — measured at a real 5-9% penalty there —
# so profile Rust with perf's --call-graph dwarf instead. Go keeps frame
# pointers by default on amd64/arm64.
target_compile_options(bench_flags INTERFACE
  -fno-omit-frame-pointer -fvisibility=hidden
  -Wall -Wextra -Wpedantic -Wconversion
  -Wundef -Wshadow -Walloca -Wcast-qual
  -Wdisabled-optimization -Wdouble-promotion -Wfloat-equal
  -Winit-self -Wmissing-include-dirs -Wmultichar
  -Wpacked -Wpointer-arith -Wredundant-decls
  -Wswitch-default -Wwrite-strings
  $<$<CXX_COMPILER_ID:GNU>:-Wformat-signedness>)
target_link_libraries(bench_flags INTERFACE mcap::mcap)

add_executable(bench_write bench_write.cpp)
target_link_libraries(bench_write PRIVATE bench_flags)

add_executable(bench_read bench_read.cpp)
target_link_libraries(bench_read PRIVATE bench_flags)
