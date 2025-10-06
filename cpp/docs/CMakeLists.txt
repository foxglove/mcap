cmake_minimum_required(VERSION 3.10)
project(McapDocs CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CONAN_SYSTEM_INCLUDES ON)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_library(docs-entrypoint docs-entrypoint.cpp)
target_link_libraries(docs-entrypoint ${CONAN_LIBS})
