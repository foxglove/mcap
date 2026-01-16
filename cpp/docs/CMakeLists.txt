cmake_minimum_required(VERSION 3.10)
project(McapDocs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

get_filename_component(MCAP_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${MCAP_ROOT}/cmake")
include(McapDependencies)

add_library(docs-entrypoint docs-entrypoint.cpp)
target_link_libraries(docs-entrypoint PRIVATE mcap::mcap)
