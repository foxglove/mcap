INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER proto/foxglove/PointCloud.proto)
ADD_LIBRARY(pointcloud_proto ${PROTO_HEADER} ${PROTO_SRC})

add_executable(protobuf_pointcloud_writer protobuf_pointcloud_writer.cpp)
target_link_libraries(protobuf_pointcloud_writer ${CONAN_LIBS} pointcloud_proto ${PROTOBUF_LIBRARY})