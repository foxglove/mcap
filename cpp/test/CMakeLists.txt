cmake_minimum_required(VERSION 3.15)
project(McapTest CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
if (CMAKE_CONFIGURATION_TYPES)
  foreach(OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/bin)
  endforeach()
endif()

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR}/generators)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

find_package(Catch2 REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(mcap REQUIRED)

set(MCAP_TEST_LIBS Catch2::Catch2 nlohmann_json::nlohmann_json mcap::mcap)

# The parallel reader uses a thread pool.
find_package(Threads REQUIRED)

add_executable(streamed-reader-conformance streamed_reader_conformance.cpp)
target_link_libraries(streamed-reader-conformance ${MCAP_TEST_LIBS})

add_executable(indexed-reader-conformance indexed_reader_conformance.cpp)
target_link_libraries(indexed-reader-conformance ${MCAP_TEST_LIBS})

add_executable(streamed-writer-conformance streamed_writer_conformance.cpp)
target_link_libraries(streamed-writer-conformance ${MCAP_TEST_LIBS})

add_executable(unit-tests unit_tests.cpp)
target_link_libraries(unit-tests ${MCAP_TEST_LIBS})

add_executable(unit-tests-nocompress unit_tests.cpp)
target_link_libraries(unit-tests-nocompress ${MCAP_TEST_LIBS})
target_compile_definitions(unit-tests-nocompress PRIVATE MCAP_COMPRESSION_NO_LZ4 MCAP_COMPRESSION_NO_ZSTD)

# Proves the serial API still builds with the parallel reader compiled out.
add_executable(unit-tests-noparallel unit_tests.cpp)
target_link_libraries(unit-tests-noparallel ${CONAN_LIBS})
target_compile_definitions(unit-tests-noparallel PRIVATE MCAP_NO_PARALLEL)

add_executable(parallel-reader-test parallel_reader_test.cpp)
target_link_libraries(parallel-reader-test ${CONAN_LIBS} Threads::Threads)

add_executable(parallel-reader-test-nocompress parallel_reader_test.cpp)
target_link_libraries(parallel-reader-test-nocompress ${CONAN_LIBS} Threads::Threads)
target_compile_definitions(parallel-reader-test-nocompress PRIVATE MCAP_COMPRESSION_NO_LZ4 MCAP_COMPRESSION_NO_ZSTD)
