# Instantiates the library so hdoc sees every symbol via compile_commands.json.
add_library(docs-entrypoint docs-entrypoint.cpp)
target_link_libraries(docs-entrypoint mcap::mcap)
