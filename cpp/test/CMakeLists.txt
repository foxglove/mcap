cmake_minimum_required(VERSION 3.11)
project(McapTests CXX)

Include(FetchContent)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.0.0-preview4
)
FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(json
  URL https://github.com/nlohmann/json/releases/download/v3.10.5/json.tar.xz
)

FetchContent_GetProperties(json)
if(NOT json_POPULATED)
  FetchContent_Populate(json)
  add_subdirectory(${json_SOURCE_DIR} ${json_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

add_executable(streamed-reader-conformance streamed-reader-conformance.cpp)
target_link_libraries(streamed-reader-conformance PRIVATE mcap nlohmann_json::nlohmann_json)

add_executable(streamed-writer-conformance streamed-writer-conformance.cpp)
target_link_libraries(streamed-writer-conformance PRIVATE mcap nlohmann_json::nlohmann_json)

add_executable(unit-tests unit-tests.cpp)
target_link_libraries(unit-tests PRIVATE mcap Catch2::Catch2WithMain)
