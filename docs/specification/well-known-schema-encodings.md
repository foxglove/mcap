## Well-known schema encodings

### `protobuf`

- schema: A binary [FileDescriptorSet](https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/descriptor.proto) as produced by `protoc --descriptor_set_out`.
- schema_name: Fully qualified name to the message within the descriptor set. For example, in a proto file containing `package foo.bar; message Baz {}` the fully qualified message name is `foo.bar.Baz`.

### `ros1msg`

- schema: Concatenated ROS1 .msg files
- schema_name: A valid [package resource name](http://wiki.ros.org/Names#Package_Resource_Names), e.g. `sensor_msgs/PointCloud2`

### `ros2msg`

- schema: Concatenated ROS2 .msg files
- schema_name: A valid [package resource name](http://wiki.ros.org/Names#Package_Resource_Names), e.g. `sensor_msgs/msg/PointCloud2`

### `jsonschema`

- schema: [JSON Schema](https://json-schema.org)
- schema_name: unspecified
