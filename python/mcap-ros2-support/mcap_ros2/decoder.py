"""Decoder class for decoding ROS2 messages from MCAP files."""

from typing import Dict

from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import SchemaEncoding

from .dynamic import DecodedMessage, DecoderFunction, generate_dynamic


class McapROS2DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS2 message."""

    pass


class Decoder:
    """Decodes MCAP message records into ROS2 messages."""

    def __init__(self):
        """Decode ROS2 messages from MCAP Message records."""
        self._decoders: Dict[int, DecoderFunction] = {}

    def decode(self, schema: Schema, message: Message) -> DecodedMessage:
        """
        Decode a ROS2 message object from an MCAP message.

        :param schema: The message schema record from the MCAP.
        :param message: The message record containing content to be decoded.
        :raises McapROS1DecodeError: if the content could not be decoded as a ROS2 message with
            the given schema.
        :return: The decoded message content.
        """
        decoder = self._decoders.get(schema.id)
        if decoder is None:
            if schema.encoding != SchemaEncoding.ROS2:
                raise McapROS2DecodeError(
                    f'can\'t parse schema with encoding "{schema.encoding}"'
                )
            type_dict = generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            decoder = type_dict[schema.name]
            if decoder is None:
                raise McapROS2DecodeError(f'schema parsing failed for "{schema.name}"')
            self._decoders[schema.id] = decoder

        ros_msg = decoder(message.data)
        return ros_msg
