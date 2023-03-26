# API Reference

MCAP libraries are available for [C++](https://github.com/foxglove/mcap/tree/main/cpp), [Go](https://github.com/foxglove/mcap/tree/main/go), [Python](https://github.com/foxglove/mcap/tree/main/python), [Rust](https://github.com/foxglove/mcap/tree/main/rust), [Swift](https://github.com/foxglove/mcap/tree/main/swift), and [TypeScript](https://github.com/foxglove/mcap/tree/main/typescript). All libraries are tested for conformance with the MCAP specification.

## Feature Matrix

|                                     | C++      | Go       | Python   | Rust | Swift    | TypeScript |
| ----------------------------------- | -------- | -------- | -------- | ---- | -------- | ---------- |
| Indexed unordered message reading   | Yes      | Yes      | Yes      | Yes  | No       | Yes        |
| Timestamp-ordered message reading   | Yes      | Yes      | Yes      | No   | Yes      | Yes        |
| Indexed metadata reading            | Yes [^1] | Yes [^1] | Yes      | Yes  | Yes [^1] | Yes [^1]   |
| Indexed attachment reading          | Yes [^1] | Yes [^1] | Yes      | Yes  | Yes [^1] | Yes [^1]   |
| Non-materialized attachment reading | Yes [^2] | No       | No       | Yes  | No       | No         |
| Non-indexed reading                 | Yes      | Yes      | Yes      | Yes  | Yes      | Yes        |
| CRC validation                      | No       | Yes      | No       | Yes  | Yes      | Yes        |
| ROS1 wrapper                        | No       | No       | Yes      | No   | No       | No         |
| ROS2 wrapper                        | Yes [^3] | No       | Yes [^3] | No   | No       | No         |
| Protobuf wrapper                    | No       | No       | Yes      | No   | No       | No         |
| Record writing                      | Yes      | Yes      | Yes      | Yes  | Yes      | Yes        |
| Easy chunked writing                | Yes      | Yes      | Yes      | Yes  | Yes      | Yes        |
| Automatic summary writing           | Yes [^4] | Yes [^4] | Yes [^4] | Yes  | Yes [^4] | Yes [^4]   |

[^1]: These readers don’t have a single call to read an attachment or metadata record by name, but do allow you to read the summary, seek to that location, read a record and parse it.
[^2]: Using the [MCAP Rosbag2 storage plugin](https://github.com/ros-tooling/rosbag2_storage_mcap).
[^3]: The C++ reader interface does not preclude one from backing it with a memory-mapped file. This could be used to implement message and attachment parsing without copying data into memory.
[^4]: All writers currently do not compute a CRC for the DataEnd record.
