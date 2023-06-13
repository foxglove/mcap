from typing import Dict, Any, Type, Callable, Optional

try:
    # If the user has genpy on their PATH from an existing ROS1 environment, use that.
    # This ensures that `isinstance(msg, genpy.Message)` succeeds on objects returned
    # by decode().
    from genpy import dynamic  # type: ignore
except ImportError:
    from ._vendor.genpy import dynamic  # type: ignore

from mcap.exceptions import McapError
from mcap.records import Schema
from mcap.well_known import SchemaEncoding, MessageEncoding
from mcap.decoder import DecoderFactory as McapDecoderFactory


class McapROS1DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS1 message."""

    pass


class DecoderFactory(McapDecoderFactory):
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
