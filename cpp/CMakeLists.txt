cmake_minimum_required(VERSION 3.18)

# The version is defined once, as MCAP_LIBRARY_VERSION in types.hpp. This file
# and conanfile.py both read it from there.
set(_mcap_types_hpp "${CMAKE_CURRENT_SOURCE_DIR}/mcap/include/mcap/types.hpp")
file(STRINGS "${_mcap_types_hpp}" _mcap_version_line
     REGEX "^#define MCAP_LIBRARY_VERSION \"[0-9]+\\.[0-9]+\\.[0-9]+\"")
string(REGEX REPLACE "^#define MCAP_LIBRARY_VERSION \"([0-9]+\\.[0-9]+\\.[0-9]+)\".*" "\\1"
       MCAP_VERSION "${_mcap_version_line}")
if(NOT MCAP_VERSION MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
  message(FATAL_ERROR "Could not read MCAP_LIBRARY_VERSION from ${_mcap_types_hpp}")
endif()
unset(_mcap_types_hpp)
unset(_mcap_version_line)

project(mcap
  VERSION ${MCAP_VERSION}
  DESCRIPTION "C++ implementation of the MCAP file format"
  HOMEPAGE_URL "https://github.com/foxglove/mcap"
  LANGUAGES CXX)

# Off when pulled in with add_subdirectory() or FetchContent.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(MCAP_IS_TOP_LEVEL ON)
else()
  set(MCAP_IS_TOP_LEVEL OFF)
endif()

option(MCAP_BUILD_TESTS "Build the mcap unit and conformance tests" OFF)
option(MCAP_BUILD_EXAMPLES "Build the mcap examples" OFF)
option(MCAP_BUILD_BENCH "Build the mcap Google Benchmark microbenchmarks" OFF)
option(MCAP_BUILD_DOCS "Build the translation unit used to generate the API docs" OFF)
option(MCAP_INSTALL "Generate install rules for mcap" ${MCAP_IS_TOP_LEVEL})
option(MCAP_COMPRESSION_NO_LZ4 "Build mcap without LZ4 support" OFF)
option(MCAP_COMPRESSION_NO_ZSTD "Build mcap without zstd support" OFF)

# Prepend so a parent project's FindLZ4/FindZstd cannot shadow ours.
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# ---------------------------------------------------------------------------
# The library: header-only, so an INTERFACE target carrying the include
# directory, the language standard, and the compression dependencies.
# ---------------------------------------------------------------------------
add_library(mcap INTERFACE)
add_library(mcap::mcap ALIAS mcap)

target_include_directories(mcap INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/mcap/include>
  $<INSTALL_INTERFACE:include>)
target_compile_features(mcap INTERFACE cxx_std_17)

# MODULE keeps CMAKE_FIND_PACKAGE_PREFER_CONFIG from bypassing cmake/Find*.cmake.
if(MCAP_COMPRESSION_NO_LZ4)
  target_compile_definitions(mcap INTERFACE MCAP_COMPRESSION_NO_LZ4)
else()
  find_package(LZ4 MODULE REQUIRED)
  target_link_libraries(mcap INTERFACE LZ4::LZ4)
endif()

if(MCAP_COMPRESSION_NO_ZSTD)
  target_compile_definitions(mcap INTERFACE MCAP_COMPRESSION_NO_ZSTD)
else()
  find_package(Zstd MODULE REQUIRED)
  target_link_libraries(mcap INTERFACE Zstd::Zstd)
endif()

# ---------------------------------------------------------------------------
# Install rules and the CMake package config.
# ---------------------------------------------------------------------------
if(MCAP_INSTALL)
  include(GNUInstallDirs)
  include(CMakePackageConfigHelpers)

  set(MCAP_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/mcap"
      CACHE STRING "Install directory for the mcap CMake package files")

  install(TARGETS mcap EXPORT mcapTargets)
  install(DIRECTORY mcap/include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  install(EXPORT mcapTargets
    NAMESPACE mcap::
    DESTINATION "${MCAP_INSTALL_CMAKEDIR}")

  configure_package_config_file(
    cmake/mcapConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/mcapConfig.cmake"
    INSTALL_DESTINATION "${MCAP_INSTALL_CMAKEDIR}")
  write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/mcapConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
    ARCH_INDEPENDENT)

  install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/mcapConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/mcapConfigVersion.cmake"
    cmake/FindLZ4.cmake
    cmake/FindZstd.cmake
    DESTINATION "${MCAP_INSTALL_CMAKEDIR}")
endif()

# ---------------------------------------------------------------------------
# Developer targets, built only on request so consumers get just the library.
# ---------------------------------------------------------------------------
if(NOT (MCAP_BUILD_TESTS OR MCAP_BUILD_EXAMPLES OR MCAP_BUILD_BENCH OR MCAP_BUILD_DOCS))
  return()
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Every binary goes in <build>/bin on every generator, so scripts and CI
# share one path.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
if(CMAKE_CONFIGURATION_TYPES)
  foreach(_config ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER "${_config}" _config)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${_config} "${CMAKE_BINARY_DIR}/bin")
  endforeach()
endif()

# Warnings shared by every developer target.
add_library(mcap_warnings INTERFACE)
if(MSVC)
  target_compile_options(mcap_warnings INTERFACE /W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  target_compile_definitions(mcap_warnings INTERFACE _CRT_SECURE_NO_WARNINGS)
else()
  target_compile_options(mcap_warnings INTERFACE
    -Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

if(MCAP_BUILD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

if(MCAP_BUILD_EXAMPLES)
  enable_testing()
  add_subdirectory(examples)
endif()

if(MCAP_BUILD_BENCH)
  add_subdirectory(bench)
endif()

if(MCAP_BUILD_DOCS)
  add_subdirectory(docs)
endif()
