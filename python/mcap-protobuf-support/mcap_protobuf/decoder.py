from collections import Counter
from typing import Dict, Any, Type

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.message_factory import GetMessages
from mcap.mcap0.exceptions import McapError
from mcap.mcap0.records import Message, Schema
from mcap.mcap0.well_known import SchemaEncoding


class McapProtobufDecodeError(McapError):
    """Raised when a Message record cannot be decoded as a Protobuf message."""

    pass


class Decoder:
    def __init__(self):
        """Decodes Protobuf messages from MCAP message records."""
        self._types: Dict[int, Type[Any]] = {}

    def decode(self, schema: Schema, message: Message) -> Any:
        """Takes a Message record from an MCAP along with its associated Schema,
        and returns the decoded protobuf message from within.

        :param schema: The message schema record from the MCAP.
        :type schema: mcap.mcap0.records.Schema
        :param message: The message record containing content to be decoded.
        :type message: mcap.mcap0.records.Message
        :raises McapProtobufDecodeError: if the content could not be decoded as a protobuf message
            with the given schema.
        :return: The decoded message content.
        """
        if schema.encoding != SchemaEncoding.Protobuf:
            raise McapProtobufDecodeError(
                f"can't decode schema with encoding {schema.encoding}"
            )
        generated = self._types.get(schema.id)
        if generated is None:
            fds = FileDescriptorSet.FromString(schema.data)
            for name, count in Counter(fd.name for fd in fds.file).most_common(1):
                if count > 1:
                    raise McapError(
                        f"FileDescriptorSet contains {count} file descriptors for {name}"
                    )
            messages = GetMessages(fds.file)
            for name, klass in messages.items():
                if name == schema.name:
                    self._types[schema.id] = klass
                    generated = klass
        if generated is None:
            raise McapError(
                f"FileDescriptorSet for type {schema.name} is missing that schema"
            )
        proto_msg = generated()
        proto_msg.ParseFromString(message.data)
        return proto_msg
