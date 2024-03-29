import warnings
from collections import Counter
from typing import Any, Callable, Dict, Optional, Type

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import GetMessageClassesForFiles

from mcap.decoder import DecoderFactory as McapDecoderFactory
from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import MessageEncoding, SchemaEncoding


class McapProtobufDecodeError(McapError):
    """Raised when a Message record cannot be decoded as a Protobuf message."""

    pass


class DecoderFactory(McapDecoderFactory):
    """Provides functionality to an :py:class:`~mcap.reader.McapReader` to decode protobuf
    messages. Requires valid `protobuf` schemas to decode messages.
    """

    def __init__(self):
        self._types: Dict[int, Type[Any]] = {}

    def decoder_for(
        self, message_encoding: str, schema: Optional[Schema]
    ) -> Optional[Callable[[bytes], Any]]:
        if (
            message_encoding != MessageEncoding.Protobuf
            or schema is None
            or schema.encoding != SchemaEncoding.Protobuf
        ):
            return None

        generated = self._types.get(schema.id)
        if generated is None:
            fds = FileDescriptorSet.FromString(schema.data)
            for name, count in Counter(fd.name for fd in fds.file).most_common(1):
                if count > 1:
                    raise McapError(
                        f"FileDescriptorSet contains {count} file descriptors for {name}"
                    )

            pool = DescriptorPool()
            for fd in fds.file:
                pool.Add(fd)
            messages = GetMessageClassesForFiles([fd.name for fd in fds.file], pool)

            for name, klass in messages.items():
                if name == schema.name:
                    self._types[schema.id] = klass
                    generated = klass
        if generated is None:
            raise McapError(
                f"FileDescriptorSet for type {schema.name} is missing that schema"
            )

        def decoder(data: bytes) -> Any:
            proto_msg = generated()
            proto_msg.ParseFromString(data)
            return proto_msg

        return decoder


class Decoder:
    """Decodes Protobuf messages.

    .. deprecated:: 0.3.0
      Use :py:class:`~mcap_protobuf.decoder.DecoderFactory` with :py:class:`~mcap.reader.McapReader`
      instead.
    """

    def __init__(self):
        warnings.warn(
            """The `mcap_protobuf.decoder.Decoder` class is deprecated.
For similar functionality, instantiate the `mcap.reader.McapReader` with a
`mcap_protobuf.decoder.DecoderFactory` instance.""",
            DeprecationWarning,
        )
        self._decoder_factory = DecoderFactory()

    def decode(self, schema: Schema, message: Message) -> Any:
        decoder = self._decoder_factory.decoder_for(MessageEncoding.Protobuf, schema)
        assert decoder is not None, "failed to construct a Protobuf decoder"
        return decoder(message.data)
