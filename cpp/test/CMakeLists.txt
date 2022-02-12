cmake_minimum_required(VERSION 3.1)
project(McapTest CXX)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(streamed-reader-conformance streamed-reader-conformance.cpp)
target_link_libraries(streamed-reader-conformance ${CONAN_LIBS})

add_executable(streamed-writer-conformance streamed-writer-conformance.cpp)
target_link_libraries(streamed-writer-conformance ${CONAN_LIBS})

add_executable(unit-tests unit-tests.cpp)
target_link_libraries(unit-tests ${CONAN_LIBS})
