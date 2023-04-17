"""Decoder class for decoding ROS2 messages from MCAP files."""

from typing import Dict, Optional, Callable, Any

from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import SchemaEncoding, MessageEncoding
from mcap.decoder import DecoderFactory as McapDecoderFactory

from ._dynamic import DecodedMessage, DecoderFunction, generate_dynamic


class McapROS2DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS2 message."""

    pass


class DecoderFactory(McapDecoderFactory):
    def __init__(self):
        self._decoders: Dict[int, DecoderFunction] = {}

    def decoder_for(
        self, message_encoding: str, schema: Optional[Schema]
    ) -> Optional[Callable[[bytes], DecodedMessage]]:
        if (
            message_encoding != MessageEncoding.CDR
            or schema is None
            or schema.encoding != SchemaEncoding.ROS2
        ):
            return None

        decoder = self._decoders.get(schema.id)
        if decoder is not None:
            if schema.encoding != SchemaEncoding.ROS2:
                raise McapROS2DecodeError(
                    f'can\'t parse schema with encoding "{schema.encoding}"'
                )
            type_dict = generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            if schema.name not in type_dict:
                raise McapROS2DecodeError(f'schema parsing failed for "{schema.name}"')
            decoder = type_dict[schema.name]
            self._decoders[schema.id] = decoder
        return decoder
