from typing import Dict, Any

from mcap.mcap0.exceptions import McapError
from mcap.mcap0.records import Message, Schema
from mcap.mcap0.well_known import SchemaEncoding

from .dynamic import DecoderFunction, generate_dynamic


class McapROS2DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS2 message."""

    pass


class Decoder:
    """Decodes MCAP message records into ROS2 messages."""

    def __init__(self):
        """Decodes ROS2 messages from MCAP Message records."""
        self._decoders: Dict[int, DecoderFunction] = {}

    def decode(self, schema: Schema, message: Message) -> Any:
        """Take a Message record from an MCAP along with its associated Schema
        and returns the decoded ROS2 message from within.

        :param schema: The message schema record from the MCAP.
        :type schema: mcap.mcap0.records.Schema
        :param message: The message record containing content to be decoded.
        :type message: mcap.mcap0.records.Message
        :raises McapROS1DecodeError: if the content could not be decoded as a ROS2 message with
            the given schema.
        :return: The decoded message content.
        """
        decoder = self._decoders.get(schema.id)
        if decoder is None:
            if schema.encoding != SchemaEncoding.ROS2:
                raise McapROS2DecodeError(
                    f"can't decode schema with encoding {schema.encoding}"
                )
            type_dict = generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            decoder = type_dict[schema.name]
            if decoder is None:
                raise McapROS2DecodeError(f"schema decoding failed for {schema.name}")
            self._decoders[schema.id] = decoder

        ros_msg = decoder(message.data)
        return ros_msg
