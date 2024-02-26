cmake_minimum_required(VERSION 3.16.3)

set(MCAP_LIBRARY_VERSION 1.3.0)
project(mcap LANGUAGES CXX VERSION ${MCAP_LIBRARY_VERSION})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(mcap)

# In oreder to compile examples and tests with the main CMake we need to stop using conan inside.
# Is this something we want?
# add_subdirectory(examples)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(test)
endif()
