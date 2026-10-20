find_package(nlohmann_json REQUIRED)

set(MCAP_EXAMPLE_LIBS mcap::mcap nlohmann_json::nlohmann_json mcap_warnings)

add_executable(bag2mcap bag2mcap.cpp)
target_link_libraries(bag2mcap ${MCAP_EXAMPLE_LIBS})

add_executable(mcapdump mcapdump.cpp)
target_link_libraries(mcapdump ${MCAP_EXAMPLE_LIBS})

add_executable(rotatemcap rotatemcap.cpp)
target_link_libraries(rotatemcap ${MCAP_EXAMPLE_LIBS})

add_subdirectory(protobuf)
add_subdirectory(jsonschema)
