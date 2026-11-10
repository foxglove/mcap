find_package(Catch2 REQUIRED)
find_package(nlohmann_json REQUIRED)

set(MCAP_TEST_LIBS Catch2::Catch2 nlohmann_json::nlohmann_json mcap::mcap mcap_warnings)

add_executable(streamed-reader-conformance streamed_reader_conformance.cpp)
target_link_libraries(streamed-reader-conformance ${MCAP_TEST_LIBS})

add_executable(indexed-reader-conformance indexed_reader_conformance.cpp)
target_link_libraries(indexed-reader-conformance ${MCAP_TEST_LIBS})

add_executable(streamed-writer-conformance streamed_writer_conformance.cpp)
target_link_libraries(streamed-writer-conformance ${MCAP_TEST_LIBS})

add_executable(unit-tests unit_tests.cpp)
target_link_libraries(unit-tests ${MCAP_TEST_LIBS})
add_test(NAME unit-tests COMMAND unit-tests)

add_executable(unit-tests-nocompress unit_tests.cpp)
target_link_libraries(unit-tests-nocompress ${MCAP_TEST_LIBS})
target_compile_definitions(unit-tests-nocompress PRIVATE MCAP_COMPRESSION_NO_LZ4 MCAP_COMPRESSION_NO_ZSTD)
add_test(NAME unit-tests-nocompress COMMAND unit-tests-nocompress)
