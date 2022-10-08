"""This example demonstrates creating a ROS2 MCAP file without a ROS2 environment."""

import sys

from mcap_ros2.writer import Writer as McapWriter

# In a ROS2 environment, you can access the schema name (datatype) of a message
# instance using `message._type` and the schema text (message definition) using
# `message.__class__._full_text`. We hardcode these values here to avoid
# requiring a ROS2 environment
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

with open(sys.argv[1], "wb") as f:
    writer = McapWriter(f)
    schema = writer.register_msgdef(SCHEMA_NAME, SCHEMA_TEXT)

    for i in range(0, 10):
        writer.write_message(
            topic="/tf",
            schema=schema,
            message={
                "transforms": [
                    {
                        "header": {
                            "stamp": {"sec": 0, "nanosec": i},
                            "frame_id": "parent_frame",
                        },
                        "child_frame_id": "child_frame",
                        "transform": {
                            "translation": {"x": 1.0, "y": 2.0, "z": 3.0},
                            "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                        },
                    }
                ]
            },
            log_time=i,
            publish_time=i,
            sequence=i,
        )
    writer.finish()
