find_package(mcap CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

add_executable(example_json_writer writer.cpp)
target_link_libraries(example_json_writer PRIVATE mcap::mcap nlohmann_json::nlohmann_json)
target_compile_definitions(example_json_writer PRIVATE _USE_MATH_DEFINES)
add_executable(example_json_reader reader.cpp)
target_link_libraries(example_json_reader PRIVATE mcap::mcap nlohmann_json::nlohmann_json)
