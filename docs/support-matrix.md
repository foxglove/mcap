# Library Features and Support Matrix

## Library Support

The Python, C++, Go, and TypeScript MCAP libraries are actively developed. This means that Foxglove actively pursues bug fixes and ensures conformance with the MCAP specification.

**Note**: This does not mean that their APIs are stable.

The Swift MCAP library is experimental, and not actively developed. This means that PRs contributing bug-fixes are welcome, but GitHub Issues regarding it will not be prioritized.

## Feature Matrix

|  | Python | C++ | Go | TypeScript | Swift | Rust |
| --- | --- | --- | --- | --- | --- | --- |
| Indexed unordered message reading | Yes | Yes | Yes | Yes | No | No |
| Timestamp-ordered message reading | Yes | Yes | Yes | Yes | Yes | No |
| Indexed metadata reading | Yes | Yes [^1] | Yes [^1] | Yes [^1] | Yes [^1] | No |
| Indexed attachment reading | Yes | Yes [^1] | Yes [^1] | Yes [^1] | Yes [^1] | No |
| Non-materialized attachment reading | No | Yes [^2] | No | No | No | Yes |
| Non-indexed reading | Yes | Yes | Yes | Yes | Yes | Yes |
| CRC validation | No | No | Yes | Yes | Yes | No |
| ROS1 wrapper | Yes | No | No | No | No | No |
| ROS2 wrapper | Yes [^3] | Yes [^3] | No | No | No | No |
| Protobuf wrapper | Yes | No | No | No | No | No |
| Record writing | Yes | Yes | Yes | Yes | Yes | No |
| Easy chunked writing | Yes | Yes | Yes | Yes | Yes | No |
| Automatic summary writing | Yes [^4] | Yes [^4] | Yes [^4] | Yes [^4] | Yes [^4] | No |

[^1]: These readers don’t have a single call to read an attachment or metadata record by name, but do allow you to read the summary, seek to that location, read a record and parse it.
[^2]: Using the [MCAP Rosbag2 storage plugin](https://github.com/ros-tooling/rosbag2_storage_mcap).
[^3]: The C++ reader interface does not preclude one from backing it with a memory-mapped file. This could be used to implement message and attachment parsing without copying data into memory.
[^4]: All writers currently do not compute a CRC for the DataEnd record.
