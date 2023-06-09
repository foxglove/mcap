# API reference

MCAP libraries are available for [C++](https://github.com/foxglove/mcap/tree/main/cpp), [Go](https://github.com/foxglove/mcap/tree/main/go), [Python](https://github.com/foxglove/mcap/tree/main/python), [Rust](https://github.com/foxglove/mcap/tree/main/rust), [Swift](https://github.com/foxglove/mcap/tree/main/swift), and [TypeScript](https://github.com/foxglove/mcap/tree/main/typescript). All libraries are tested for conformance with the MCAP specification.

## Feature Matrix

|                                     | C++              | Go               | Python           | Rust | Swift            | TypeScript       |
| ----------------------------------- | ---------------- | ---------------- | ---------------- | ---- | ---------------- | ---------------- |
| Indexed unordered message reading   | Yes              | Yes              | Yes              | Yes  | No               | Yes              |
| Timestamp-ordered message reading   | Yes              | Yes              | Yes              | No   | Yes              | Yes              |
| Indexed metadata reading            | Yes <sup>1</sup> | Yes <sup>1</sup> | Yes              | Yes  | Yes <sup>1</sup> | Yes <sup>1</sup> |
| Indexed attachment reading          | Yes <sup>1</sup> | Yes <sup>1</sup> | Yes              | Yes  | Yes <sup>1</sup> | Yes <sup>1</sup> |
| Non-materialized attachment reading | Yes <sup>2</sup> | No               | No               | Yes  | No               | No               |
| Non-indexed reading                 | Yes              | Yes              | Yes              | Yes  | Yes              | Yes              |
| CRC validation                      | No               | Yes              | No               | Yes  | Yes              | Yes              |
| ROS1 wrapper                        | No               | No               | Yes              | No   | No               | No               |
| ROS2 wrapper                        | Yes <sup>3</sup> | No               | Yes <sup>3</sup> | No   | No               | No               |
| Protobuf wrapper                    | No               | No               | Yes              | No   | No               | No               |
| Record writing                      | Yes              | Yes              | Yes              | Yes  | Yes              | Yes              |
| Easy chunked writing                | Yes              | Yes              | Yes              | Yes  | Yes              | Yes              |
| Automatic summary writing           | Yes <sup>4</sup> | Yes <sup>4</sup> | Yes <sup>4</sup> | Yes  | Yes <sup>4</sup> | Yes <sup>4</sup> |

&nbsp;<sup>1</sup> These readers don’t have a single call to read an attachment or metadata record by name, but do allow you to read the summary, seek to that location, read a record and parse it.<br/>
&nbsp;<sup>2</sup> Using the [MCAP Rosbag2 storage plugin](https://github.com/ros2/rosbag2/tree/rolling/rosbag2_storage_mcap).<br/>
&nbsp;<sup>3</sup> The C++ reader interface does not preclude one from backing it with a memory-mapped file. This could be used to implement message and attachment parsing without copying data into memory.<br/>
&nbsp;<sup>4</sup> All writers currently do not compute a CRC for the DataEnd record.<br/>
