cmake_minimum_required(VERSION 3.11)
project(McapTests CXX)

Include(FetchContent)

set(BENCHMARK_ENABLE_TESTING OFF)

FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.6.1)
FetchContent_MakeAvailable(googlebenchmark)

add_executable(bench-tests run.cpp)
target_link_libraries(bench-tests PRIVATE mcap benchmark::benchmark)
