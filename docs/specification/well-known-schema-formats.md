## Well-known schema formats

### proto

- schema: [FileDescriptorSet](https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/descriptor.proto)
- schema_name: Fully qualified name to the message within the descriptor set.

A DescriptorSet for the following .proto file, the fully qualified name for `Baz` is `foo.bar.Baz`.

```
package foo.bar;
message Baz {}
```

### ros1msg

- schema: Concatenated ROS1 .msg files
- schema_name: A valid [package resource name](http://wiki.ros.org/Names#Package_Resource_Names), e.g. `sensor_msgs/PointCloud2`

### ros2msg

- schema: Concatenated ROS2 .msg files
- schema_name: A valid [package resource name](http://wiki.ros.org/Names#Package_Resource_Names), e.g. `sensor_msgs/msg/PointCloud2`

### jsonschema

- schema: [JSON Schema](https://json-schema.org)
- schema_name: unspecified
