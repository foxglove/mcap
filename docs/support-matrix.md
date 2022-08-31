# Library Features and Support Matrix

## Library Support

The Python, C++, Go, and Typescript MCAP libraries are actively developed. This means that Foxglove actively pursues bug fixes and ensures conformance with the MCAP specification.

**Note**: This does not mean that their APIs are stable.

The Swift MCAP library is experimental, and not actively developed. This means that PRs contributing bug-fixes are welcome, but GitHub Issues regarding it will not be prioritized.

## Feature Matrix

|  | Python | C++ | Go | Typescript | Swift |
| --- | --- | --- | --- | --- | --- |
| Indexed unordered message reading | Yes | Yes | Yes | Yes | No |
| Timestamp-ordered message reading | Yes | Partial [^1] | Yes | Yes | No |
| Indexed metadata reading | Yes | Yes [^2] | Yes [^2] | Yes [^2] | No |
| Indexed attachment reading | Yes | Yes [^2] | Yes [^2] | Yes [^2] | No |
| Non-materialized attachment reading | No | Yes [^3] | No | No | No |
| Non-indexed reading | Yes | Yes | Yes | Yes | Yes |
| CRC validation | No | No | Yes | Yes | No |
| ROS1 wrapper | Yes | No | No | No | No |
| ROS2 wrapper | Yes [^4] | Yes [^4] | No | No | No |
| Protobuf wrapper | Yes | No | No | No | No |
| Record writing | Yes | Yes | Yes | Yes | Yes |
| Easy chunked writing | Yes | Yes | Yes | Yes | Yes |
| Automatic summary writing | Yes [^5] | Yes [^5] | Yes [^5] | Yes [^5] | Yes [^5] |

[^1]: The C++ reader does not assume chunk indices are in order, but assumes all messages in a chunk are in order and chunk time ranges do not overlap.
[^2]: These readers don’t have a single call to read an attachment or metadata record by name, but do allow you to read the summary, seek to that location, read a record and parse it.
[^3]: Using the [MCAP Rosbag2 storage plugin](https://github.com/ros-tooling/rosbag2_storage_mcap).
[^4]: The C++ reader interface does not preclude one from backing it with a memory-mapped file. This could be used to implement message and attachment parsing without copying data into memory.
[^5]: All writers currently do not compute a CRC for the DataEnd record.
