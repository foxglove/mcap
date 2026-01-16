cmake_minimum_required(VERSION 3.10)
project(McapExamples CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

get_filename_component(MCAP_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${MCAP_ROOT}/cmake")
include(McapDependencies)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

add_executable(bag2mcap bag2mcap.cpp)
target_link_libraries(bag2mcap PRIVATE mcap::mcap)

add_executable(mcapdump mcapdump.cpp)
target_link_libraries(mcapdump PRIVATE mcap::mcap)

add_executable(rotatemcap rotatemcap.cpp)
target_link_libraries(rotatemcap PRIVATE mcap::mcap)

add_subdirectory(protobuf)
add_subdirectory(jsonschema)
