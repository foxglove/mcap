cmake_minimum_required(VERSION 3.10)
project(McapExamples CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Wshadow -Wpointer-arith -Werror)
endif()

find_package(mcap REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(protobuf REQUIRED)

add_executable(bag2mcap bag2mcap.cpp)
target_link_libraries(bag2mcap mcap::mcap nlohmann_json::nlohmann_json protobuf::protobuf)

add_executable(mcapdump mcapdump.cpp)
target_link_libraries(mcapdump mcap::mcap nlohmann_json::nlohmann_json protobuf::protobuf)

add_executable(rotatemcap rotatemcap.cpp)
target_link_libraries(rotatemcap mcap::mcap nlohmann_json::nlohmann_json protobuf::protobuf)

add_subdirectory(protobuf)
add_subdirectory(jsonschema)
