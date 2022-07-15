cmake_minimum_required(VERSION 3.10)
project(McapExamples CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (MSVC)
  add_compile_options(/W4 /WX)
else()
  add_compile_options(-Wall -Wextra -pedantic -Werror)
endif()

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

add_executable(bag2mcap bag2mcap.cpp)
target_link_libraries(bag2mcap ${CONAN_LIBS})

add_executable(mcapdump mcapdump.cpp)
target_link_libraries(mcapdump ${CONAN_LIBS})

add_subdirectory(protobuf)
