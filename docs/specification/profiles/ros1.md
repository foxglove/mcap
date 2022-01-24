## ROS1

Profile: **ros1**

The `ros1` profile describes how to create mcap files for v1 of the [ROS](https://ros.org/) framework.

### Channel Info

| field       | value                                    |
| ----------- | ---------------------------------------- |
| encoding    | ros1                                     |
| schema      | [.msg](http://wiki.ros.org/msg)          |
| schema_name | `package/datatype`, e.g. `my_msgs/Thing` |
| user_data   | See below                                |

#### user_data

- callerid (optional, string)
- latching (optional, bool stringified as "true" or "false")
