import warnings
from typing import Any, Callable, Dict, Optional, Type

try:
    # If the user has genpy on their PATH from an existing ROS1 environment, use that.
    # This ensures that `isinstance(msg, genpy.Message)` succeeds on objects returned
    # by decode().
    from genpy import dynamic  # type: ignore
except ImportError:
    from ._vendor.genpy import dynamic  # type: ignore

from mcap.decoder import DecoderFactory as McapDecoderFactory
from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import MessageEncoding, SchemaEncoding


class McapROS1DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS1 message."""

    pass


class DecoderFactory(McapDecoderFactory):
    """Provides functionality to an :py:class:`~mcap.reader.McapReader` to decode ROS 1 messages.
    Requires a valid `ros1msg` schema to decode messages.
    """

    def __init__(self):
        self._types: Dict[int, Type[Any]] = {}

    def decoder_for(
        self, message_encoding: str, schema: Optional[Schema]
    ) -> Optional[Callable[[bytes], Any]]:
        if (
            message_encoding != MessageEncoding.ROS1
            or schema is None
            or schema.encoding != SchemaEncoding.ROS1
        ):
            return None
        generated_type = self._types.get(schema.id)
        if generated_type is None:
            type_dict: Dict[str, Type[Any]] = dynamic.generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            generated_type = type_dict[schema.name]
            self._types[schema.id] = generated_type

        def decoder(data: bytes):
            ros_msg = generated_type()
            ros_msg.deserialize(data)
            return ros_msg

        return decoder


class Decoder:
    """Decodes ROS 1 messages.

    .. deprecated:: 0.7.0
      Use :py:class:`~mcap_ros1.decoder.DecoderFactory` with :py:class:`~mcap.reader.McapReader`
      instead.
    """

    def __init__(self):
        warnings.warn(
            """The :py:class:`mcap_ros1.decoder.Decoder` class is deprecated.
For similar functionality, instantiate the :py:class:`mcap.reader.McapReader` with a
:py:class:`mcap_ros1.decoder.DecoderFactory` instance.""",
            DeprecationWarning,
        )
        self._decoder_factory = DecoderFactory()

    def decode(self, schema: Schema, message: Message) -> Any:
        decoder = self._decoder_factory.decoder_for(MessageEncoding.ROS1, schema)
        assert decoder is not None, "failed to construct a ROS1 decoder"
        return decoder(message.data)
