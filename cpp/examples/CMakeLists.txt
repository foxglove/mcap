cmake_minimum_required(VERSION 3.1)
project(McapExamples CXX)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(bag2mcap bag2mcap.cpp)
target_link_libraries(bag2mcap ${CONAN_LIBS})
