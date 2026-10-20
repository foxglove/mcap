find_package(benchmark REQUIRED)

add_executable(bench-tests run.cpp)
target_link_libraries(bench-tests benchmark::benchmark mcap::mcap mcap_warnings)
