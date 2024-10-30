"""Decoder class for decoding ROS2 messages from MCAP files."""

import warnings
from typing import Any, Callable, Dict, Optional

from mcap.decoder import DecoderFactory as McapDecoderFactory
from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import MessageEncoding, SchemaEncoding

from ._dynamic import DecodedMessage, DecoderFunction, generate_dynamic


class McapROS2DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS2 message."""

    pass


class DecoderFactory(McapDecoderFactory):
    """Provides functionality to an :py:class:`~mcap.reader.McapReader` to decode CDR-encoded
    messages. Requires valid `ros2msg` schema to decode messages. Schemas written in IDL are not
    currently supported.
    """

    def __init__(self) -> None:
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
        if decoder is None:
            type_dict = generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            if schema.name not in type_dict:
                raise McapROS2DecodeError(f'schema parsing failed for "{schema.name}"')
            decoder = type_dict[schema.name]
            self._decoders[schema.id] = decoder
        return decoder


class Decoder:
    """Decodes ROS 2 messages.

    .. deprecated:: 0.5.0
      Use :py:class:`~mcap_ros2.decoder.DecoderFactory` with :py:class:`~mcap.reader.McapReader`
      instead.
    """

    def __init__(self):
        warnings.warn(
            """The `mcap_ros2.decoder.Decoder` class is deprecated.
For similar functionality, instantiate the `mcap.reader.McapReader` with a
`mcap_ros2.decoder.DecoderFactory` instance.""",
            DeprecationWarning,
        )
        self._decoder_factory = DecoderFactory()

    def decode(self, schema: Schema, message: Message) -> Any:
        decoder = self._decoder_factory.decoder_for(MessageEncoding.CDR, schema)
        assert decoder is not None
        return decoder(message.data)
