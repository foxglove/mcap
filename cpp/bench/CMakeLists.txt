cmake_minimum_required(VERSION 3.10)
project(McapBenchmarks CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

get_filename_component(MCAP_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${MCAP_ROOT}/cmake")
include(McapDependencies)

find_package(benchmark CONFIG REQUIRED)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

add_executable(bench-tests run.cpp)
target_link_libraries(bench-tests PRIVATE mcap::mcap benchmark::benchmark)
