cmake_minimum_required(VERSION 3.10)
project(McapTest CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

get_filename_component(MCAP_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${MCAP_ROOT}/cmake")
include(McapDependencies)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

add_executable(streamed-reader-conformance streamed_reader_conformance.cpp)
target_link_libraries(streamed-reader-conformance PRIVATE mcap::mcap mcap::vendor_nlohmann)

add_executable(indexed-reader-conformance indexed_reader_conformance.cpp)
target_link_libraries(indexed-reader-conformance PRIVATE mcap::mcap mcap::vendor_nlohmann)

add_executable(streamed-writer-conformance streamed_writer_conformance.cpp)
target_link_libraries(streamed-writer-conformance PRIVATE mcap::mcap mcap::vendor_nlohmann)

add_executable(unit-tests unit_tests.cpp)
target_link_libraries(unit-tests PRIVATE mcap::mcap mcap::vendor_catch2)

add_executable(unit-tests-nocompress unit_tests.cpp)
target_link_libraries(unit-tests-nocompress PRIVATE mcap::mcap mcap::vendor_catch2)
target_compile_definitions(unit-tests-nocompress PRIVATE MCAP_COMPRESSION_NO_LZ4 MCAP_COMPRESSION_NO_ZSTD)
