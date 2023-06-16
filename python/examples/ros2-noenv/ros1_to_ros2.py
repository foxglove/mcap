#!/usr/bin/env python3

"""Convert a ROS1 MCAP file to a ROS2 MCAP file."""

import re
import sys
from typing import Dict

from mcap_ros1.reader import read_ros1_messages
from mcap_ros2.writer import Writer as Ros2McapWriter

from mcap.reader import make_reader
from mcap.records import Schema


def main():
    """Convert a ROS1 MCAP file to a ROS2 MCAP file."""
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <ros1.mcap> <output-ros2.mcap>")
        exit(1)

    ros1_filename = sys.argv[1]
    ros2_filename = sys.argv[2]

    # Provide additional instruction if a .bag was given as input
    if ros1_filename.endswith(".bag"):
        ros1_mcap_filename = ros1_filename.removesuffix(".bag") + ".mcap"
        print(
            f"Input file must be a mcap file, not rosbag. Convert the "
            f"file first using `mcap convert {ros1_filename} "
            f"{ros1_mcap_filename}`"
        )
        exit(1)

    # Check the input is a valid MCAP with "ros1" profile
    with open(ros1_filename, "rb") as f:
        reader = make_reader(f)
        profile = reader.get_header().profile
        if profile != "ros1":
            print(f'Input MCAP file must have a "ros1" profile, found "{profile}"')
            exit(1)

    # Open the output file
    with open(ros2_filename, "wb") as f:
        writer = Ros2McapWriter(f)
        datatypes_to_schemas: Dict[str, Schema] = {}

        for msg in read_ros1_messages(ros1_filename):
            # Register schemas as needed
            datatype = msg.schema.name
            if datatype not in datatypes_to_schemas:
                msgdef_text = ros1_msgdef_to_ros2(msg.schema.data.decode("utf-8"))
                schema = writer.register_msgdef(datatype, msgdef_text)
                datatypes_to_schemas[datatype] = schema
            else:
                schema = datatypes_to_schemas[datatype]

            # Encode the deserialized ROS1 message as ROS2 CDR and write a
            # Message record to the output file
            writer.write_message(
                msg.topic,
                schema,
                msg.ros_msg,
                msg.log_time_ns,
                msg.publish_time_ns,
                msg.sequence_count,
            )

        writer.finish()


def ros1_msgdef_to_ros2(msgdef: str) -> str:
    """Converts a concatenated ROS1 message definition to a concatenated ROS2 message definition."""
    COUNT = 9999

    # Replace "Header" with "std_msgs/Header"
    msgdef = re.sub(r"^Header", "std_msgs/Header", msgdef, COUNT, re.MULTILINE)
    # Replace time/duration with their ROS2 equivalents
    msgdef = re.sub(r"^time", "builtin_interfaces/Time", msgdef, COUNT, re.MULTILINE)
    msgdef = re.sub(
        r"^duration", "builtin_interfaces/Duration", msgdef, COUNT, re.MULTILINE
    )
    # Single letter uppercase variable names are not allowed in ROS2. Replace
    # them with their lowercase equivalents
    msgdef = re.sub(
        r" ([A-Z])($| #)",
        lambda pat: " " + pat.group(1).lower() + pat.group(2),
        msgdef,
        COUNT,
        re.MULTILINE,
    )
    return msgdef


if __name__ == "__main__":
    main()
