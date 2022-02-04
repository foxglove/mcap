cmake_minimum_required(VERSION 3.1)
project(McapTest CXX)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(unit-tests test.cpp)
target_link_libraries(unit-tests ${CONAN_LIBS})
