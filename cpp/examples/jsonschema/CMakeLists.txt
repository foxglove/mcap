add_executable(example_json_writer writer.cpp)
target_link_libraries(example_json_writer ${CONAN_LIBS})
add_executable(example_json_reader reader.cpp)
target_link_libraries(example_json_reader ${CONAN_LIBS})
