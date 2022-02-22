project(McapExamples CXX)

add_executable(writemcap writemcap.cpp)
target_link_libraries(writemcap mcap)

add_executable(mcapdump mcapdump.cpp)
target_link_libraries(mcapdump mcap)
