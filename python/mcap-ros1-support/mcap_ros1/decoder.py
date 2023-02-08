from typing import Dict, Any, Type

try:
    # If the user has genpy on their PATH from an existing ROS1 environment, use that.
    # This ensures that `isinstance(msg, genpy.Message)` succeeds on objects returned
    # by decode().
    from genpy import dynamic  # type: ignore
except ImportError:
    from ._vendor.genpy import dynamic  # type: ignore

from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import SchemaEncoding


class McapROS1DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS1 message."""

    pass


class Decoder:
    def __init__(self):
        """Decodes ROS1 messages from MCAP Message records."""
        self._types: Dict[int, Type[Any]] = {}

    def decode(self, schema: Schema, message: Message) -> Any:
        """Takes a Message record from an MCAP along with its associated Schema,
        and returns the decoded ROS1 message from within.

        :param schema: The message schema record from the MCAP.
        :type schema: mcap.records.Schema
        :param message: The message record containing content to be decoded.
        :type message: mcap.records.Message
        :raises McapROS1DecodeError: if the content could not be decoded as a ROS1 message with
            the given schema.
        :return: The decoded message content.
        """
        if schema.encoding != SchemaEncoding.ROS1:
            raise McapROS1DecodeError(
                f"can't decode schema with encoding {schema.encoding}"
            )
        generated_type = self._types.get(schema.id)
        if generated_type is None:
            type_dict = dynamic.generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            generated_type = type_dict[schema.name]
            self._types[schema.id] = generated_type

        ros_msg = generated_type()
        ros_msg.deserialize(message.data)
        return ros_msg
