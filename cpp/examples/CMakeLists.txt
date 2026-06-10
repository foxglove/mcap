cmake_minimum_required(VERSION 3.15)
project(McapExamples CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR}/generators)

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

set(MCAP_EXAMPLE_LIBS mcap::mcap nlohmann_json::nlohmann_json)

add_executable(bag2mcap bag2mcap.cpp)
target_link_libraries(bag2mcap ${MCAP_EXAMPLE_LIBS})

add_executable(mcapdump mcapdump.cpp)
target_link_libraries(mcapdump ${MCAP_EXAMPLE_LIBS})

add_executable(rotatemcap rotatemcap.cpp)
target_link_libraries(rotatemcap ${MCAP_EXAMPLE_LIBS})

add_subdirectory(protobuf)
add_subdirectory(jsonschema)
