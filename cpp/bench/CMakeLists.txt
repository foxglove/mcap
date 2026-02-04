cmake_minimum_required(VERSION 3.16)
project(McapBenchmarks CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(mcap REQUIRED CONFIG)
find_package(benchmark REQUIRED CONFIG)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

add_executable(bench-tests run.cpp)
target_link_libraries(bench-tests mcap::mcap benchmark::benchmark_main)
