## ROS1

Profile: **ros1**

The `ros1` profile describes how to create mcap files for v1 of the [ROS](https://ros.org/) framework.

### Channel Info

| field       | value            |
| ----------- | ---------------- |
| encoding    | ros1             |
| schema      | .MSG             |
| schema_name | package/datatype |
| user_data   | See below        |

#### user_data

- callerid (optional, string)
- latching (optional, bool stringified as "true" or "false")
