cmake_minimum_required(VERSION 3.16)
project(McapTest CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

find_package(mcap REQUIRED CONFIG)
find_package(Catch2 REQUIRED CONFIG)
find_package(nlohmann_json REQUIRED CONFIG)

add_executable(streamed-reader-conformance streamed_reader_conformance.cpp)
target_link_libraries(streamed-reader-conformance mcap::mcap nlohmann_json::nlohmann_json)

add_executable(indexed-reader-conformance indexed_reader_conformance.cpp)
target_link_libraries(indexed-reader-conformance mcap::mcap nlohmann_json::nlohmann_json)

add_executable(streamed-writer-conformance streamed_writer_conformance.cpp)
target_link_libraries(streamed-writer-conformance mcap::mcap nlohmann_json::nlohmann_json)

add_executable(unit-tests unit_tests.cpp test_odr.cpp)
target_link_libraries(unit-tests mcap::mcap Catch2::Catch2)
