## ROS2

Profile: **ros2**

The `ros2` profile describes how to create mcap files for v2 of the [ROS](https://ros.org/) framework.

### Channel Info

| field | value |
| --- | --- |
| encoding | cdr |
| schema | [.msg](https://docs.ros.org/en/galactic/Concepts/About-ROS-Interfaces.html) |
| schema_name | `package/subtype/datatype`, e.g. `my_msgs/msg/Thing` |
| user_data | See below |

#### user_data

- offered_qos_profiles (required, string)
