add_library(mcap STATIC src/reader.cpp src/types.cpp src/writer.cpp)

target_include_directories(mcap PUBLIC include)

target_include_directories(mcap SYSTEM PRIVATE ../vendor/cryptopp-8.6.0)
target_include_directories(mcap SYSTEM PRIVATE ../vendor/lz4-1.9.3/lib)
target_include_directories(mcap SYSTEM PRIVATE ../vendor/zstd-1.5.2/lib)

target_link_libraries(mcap cryptopp-static fmt lz4_static libzstd_static)
