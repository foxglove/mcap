cmake_minimum_required(VERSION 3.16)
project(Mcap CXX)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

option(MCAP_COMPRESSION_LZ4 "Build mcap with lz4 support" OFF)
option(MCAP_COMPRESSION_ZSTD "Build mcap with zstd support" OFF)

file(GLOB_RECURSE private_sources "include/mcap/*.inl")
file(GLOB_RECURSE public_sources "include/mcap/*.hpp")

add_library(mcap)
target_sources(mcap
    PRIVATE
        ${public_sources}
        ${private_sources}
        mcap.cpp
)

target_include_directories(mcap
    PUBLIC
         $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/include>
         $<INSTALL_INTERFACE:include>
)

if(MCAP_COMPRESSION_LZ4)
    find_package(lz4 REQUIRED CONFIG)
    target_link_libraries(mcap PRIVATE lz4::lz4)
else()
    target_compile_definitions(mcap PUBLIC MCAP_COMPRESSION_NO_LZ4)
endif()
if(MCAP_COMPRESSION_ZSTD)
    find_package(zstd REQUIRED CONFIG)
    target_link_libraries(mcap PRIVATE zstd::zstd)
else()
    target_compile_definitions(mcap PUBLIC MCAP_COMPRESSION_NO_ZSTD)
endif()

if(NOT BUILD_SHARED_LIBS)
    # Disable symbols exports
    target_compile_definitions(mcap PUBLIC MCAP_PUBLIC=)
endif()


install(TARGETS mcap
    EXPORT mcap
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
)

install(FILES ${public_sources} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/mcap)

install(EXPORT mcap NAMESPACE mcap DESTINATION lib/cmake/mcap)
