"""Enums listing the sets of well-known profiles, schema encodings and message encodings
available in the MCAP Specification:
https://github.com/foxglove/mcap/blob/main/docs/specification/appendix.md

NOTE: you don't need to use these profiles or encodings to use MCAP! custom profiles and
encodings are allowed with the `x-` prefix.
"""


class Profile:
    """Well-known MCAP profiles."""

    ROS1 = "ros1"
    ROS2 = "ros2"


class SchemaEncoding:
    """well-known encodings for schema records."""

    SelfDescribing = ""  # used for self-describing content, such as arbitrary JSON.
    Protobuf = "protobuf"
    Flatbuffer = "flatbuffer"
    ROS1 = "ros1msg"
    ROS2 = "ros2msg"
    JSONSchema = "jsonschema"


class MessageEncoding:
    """well-known message encodings for message records"""

    ROS1 = "ros1"
    CDR = "cdr"
    Protobuf = "protobuf"
    Flatbuffer = "flatbuffer"
    CBOR = "cbor"
    JSON = "json"
