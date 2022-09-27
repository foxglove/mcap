"""Enums listing the sets of well-known profiles, schema encodings and message encodings
available in the
`MCAP Specification <https://github.com/foxglove/mcap/blob/main/docs/specification/appendix.md>`_.

.. note:: You don't need to use these profiles or encodings to use MCAP! Custom profiles and
    encoding strings are allowed.
"""


class Profile:
    """Well-known MCAP profiles."""

    ROS1 = "ros1"
    ROS2 = "ros2"


class SchemaEncoding:
    """Well-known encodings for schema records."""

    SelfDescribing = ""  # used for self-describing content, such as arbitrary JSON.
    Protobuf = "protobuf"
    Flatbuffer = "flatbuffer"
    ROS1 = "ros1msg"
    ROS2 = "ros2msg"
    ROS2IDL = "ros2idl"
    JSONSchema = "jsonschema"


class MessageEncoding:
    """Well-known message encodings for message records"""

    ROS1 = "ros1"
    CDR = "cdr"
    Protobuf = "protobuf"
    Flatbuffer = "flatbuffer"
    CBOR = "cbor"
    JSON = "json"
