"""
This is a placeholder writer for ROS2 MCAP files.

Until mcap_ros2 supports writing, this hardcodes a single message and writes
several copies to an MCAP.
"""

import sys

from mcap.mcap0.writer import Writer as McapWriter


SCHEMA_NAME = "tf2_msgs/TFMessage"
SCHEMA_TEXT = """\
geometry_msgs/TransformStamped[] transforms
================================================================================
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id # the frame id of the child frame
Transform transform
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: geometry_msgs/Transform
Vector3 translation
Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w"""
PAYLOAD_HEX = (
    "0001000001000000286fae6169ddd73108000000747572746c6531000e000000747572746"
    "c65315f616865616400000000000000000000000000f03f00000000000000000000000000"
    "000000000000000000000000000000000000000000000000000000000000000000f03f"
)

with open(sys.argv[1], "wb") as f:
    writer = McapWriter(f)
    writer.start(profile="ros2")

    schema_id = writer.register_schema(
        name=SCHEMA_NAME,
        data=SCHEMA_TEXT.encode(),
        encoding="ros2msg",
    )
    channel_id = writer.register_channel(
        topic="/tf",
        message_encoding="cdr",
        schema_id=schema_id,
    )

    msg_payload = bytes.fromhex(PAYLOAD_HEX)

    for i in range(0, 10):
        log_time = 100 + i
        writer.add_message(channel_id, log_time, msg_payload, log_time, i)
    writer.finish()
