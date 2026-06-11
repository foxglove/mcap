cmake_minimum_required(VERSION 3.15)
project(McapDocs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR}/generators)

find_package(mcap REQUIRED)

add_library(docs-entrypoint docs-entrypoint.cpp)
target_link_libraries(docs-entrypoint mcap::mcap)
