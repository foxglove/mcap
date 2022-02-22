cmake_minimum_required(VERSION 3.1)
project(Mcap CXX)

set(MAIN_PROJECT OFF)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(MAIN_PROJECT ON)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (MSVC)
  # warning level 4 and all warnings as errors
  add_compile_options(/W4 /WX)
else()
  # lots of warnings and all warnings as errors
  add_compile_options(-Wall -Wextra -pedantic -Werror)
endif()

option(BUILD_SHARED OFF)
option(BUILD_STATIC ON)
option(BUILD_TESTING OFF)
set(BUILD_SHARED OFF)
set(BUILD_STATIC ON)
set(BUILD_TESTING OFF)
add_subdirectory(vendor/cryptopp-8.6.0)

option(BUILD_SHARED_LIBS OFF)
add_subdirectory(vendor/fmt-8.1.1)

option(LZ4_BUILD_CLI OFF)
option(LZ4_BUILD_LEGACY_LZ4C OFF)
add_subdirectory(vendor/lz4-1.9.3/build/cmake)

set(ZSTD_BUILD_STATIC ON)
set(ZSTD_BUILD_SHARED OFF)
set(ZSTD_BUILD_PROGRAMS OFF)
add_subdirectory(vendor/zstd-1.5.2/build/cmake)

add_subdirectory(mcap)

if(MAIN_PROJECT)
  add_subdirectory(bench)
  add_subdirectory(examples)
  add_subdirectory(test)
endif()
