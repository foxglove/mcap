cmake_minimum_required(VERSION 3.27.3)

set(MCAP_LIBRARY_VERSION 1.3.0)
project(mcap LANGUAGES CXX VERSION ${MCAP_LIBRARY_VERSION})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(mcap)

# add_subdirectory(examples)
# add_subdirectory(test)
