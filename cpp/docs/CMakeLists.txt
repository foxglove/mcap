cmake_minimum_required(VERSION 3.10)
project(McapDocs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(mcap CONFIG REQUIRED)

add_library(docs-entrypoint docs-entrypoint.cpp)
target_link_libraries(docs-entrypoint PRIVATE mcap::mcap)
