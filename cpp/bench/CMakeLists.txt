cmake_minimum_required(VERSION 3.1)
project(McapBenchmarks CXX)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(bench-tests run.cpp)
target_link_libraries(bench-tests ${CONAN_LIBS})
