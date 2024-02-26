include(FetchContent)
FetchContent_Declare(
    Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.3.2
)
FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(
    nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.10.5
)
FetchContent_MakeAvailable(nlohmann_json)

set(LZ4_BUILD_CLI OFF)
FetchContent_Declare(
    lz4
        GIT_REPOSITORY https://github.com/lz4/lz4.git
        GIT_TAG v1.9.4
        SOURCE_SUBDIR build/cmake
)
set(BUILD_SHARED_LIBS OFF)
set(BUILD_STATIC_LIBS ON)
FetchContent_MakeAvailable(lz4)

FetchContent_Declare(
    zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.2
        SOURCE_SUBDIR build/cmake
)
set(ZSTD_BUILD_STATIC ON)
set(ZSTD_BUILD_SHARED OFF)
set(ZSTD_LZ4_SUPPORT ON)
set(ZSTD_BUILD_TESTS OFF)
FetchContent_MakeAvailable(zstd)
target_include_directories(libzstd_static PUBLIC $<BUILD_INTERFACE:${zstd_SOURCE_DIR}/lib>)

set(MCAP_DEPENDENCIES mcap::mcap lz4_static libzstd_static)

if (MSVC)
  add_compile_options(/W4 /WX
    /wd4251 # suppress warning about having template instances (such as std::string) as public class members
  )
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  add_compile_options(-Wall -Wextra -pedantic -Werror)
endif()

add_executable(streamed-reader-conformance streamed_reader_conformance.cpp)
target_link_libraries(streamed-reader-conformance ${MCAP_DEPENDENCIES} nlohmann_json::nlohmann_json)

add_executable(indexed-reader-conformance indexed_reader_conformance.cpp)
target_link_libraries(indexed-reader-conformance ${MCAP_DEPENDENCIES} nlohmann_json::nlohmann_json)

add_executable(streamed-writer-conformance streamed_writer_conformance.cpp)
target_link_libraries(streamed-writer-conformance ${MCAP_DEPENDENCIES} nlohmann_json::nlohmann_json)

add_executable(unit-tests unit_tests.cpp)
target_link_libraries(unit-tests Catch2::Catch2WithMain ${MCAP_DEPENDENCIES})
add_test(
    NAME unit-tests
    COMMAND unit-tests
)

add_executable(unit-tests-nocompress unit_tests.cpp)
target_link_libraries(unit-tests-nocompress PUBLIC Catch2::Catch2WithMain ${MCAP_DEPENDENCIES})
target_compile_definitions(unit-tests-nocompress PRIVATE MCAP_COMPRESSION_NO_LZ4 MCAP_COMPRESSION_NO_ZSTD)
add_test(
    NAME unit-tests-nocompress
    COMMAND unit-tests-nocompress
)
