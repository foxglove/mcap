## ROS1

Profile: **`ros1`**

The `ros1` profile describes how to create mcap files for v1 of the [ROS](https://ros.org/) framework.

### Channel

#### message_encoding

MUST be `ros`

#### schema_format

MUST be `ros1msg`

#### user_data

- callerid (optional, string) <!-- cspell:disable-line -->
- latching (optional, bool stringified as "true" or "false")
