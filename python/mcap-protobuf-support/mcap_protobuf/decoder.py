from collections import Counter
from typing import Dict, Any, Type, Iterable

from google.protobuf.descriptor_pb2 import FileDescriptorSet, FileDescriptorProto
from google.protobuf.message_factory import MessageFactory
from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import SchemaEncoding


class McapProtobufDecodeError(McapError):
    """Raised when a Message record cannot be decoded as a Protobuf message."""

    pass


class Decoder:
    def __init__(self):
        """Decodes Protobuf messages from MCAP message records."""
        self._types: Dict[int, Type[Any]] = {}
        self._factory = MessageFactory()

    def _get_message_classes(self, file_protos: Iterable[FileDescriptorProto]):
        """Adds protos to the message factory pool in topological order, then returns
        the message classes for all protos.


        """
        file_by_name = {file_proto.name: file_proto for file_proto in file_protos}

        def _add_file(file_proto: FileDescriptorProto):
            for dependency in file_proto.dependency:
                if dependency in file_by_name:
                    # Remove from elements to be visited, in order to cut cycles.
                    _add_file(file_by_name.pop(dependency))
            self._factory.pool.Add(file_proto)

        while file_by_name:
            _add_file(file_by_name.popitem()[1])

        return self._factory.GetMessages(
            [file_proto.name for file_proto in file_protos]
        )

    def decode(self, schema: Schema, message: Message) -> Any:
        """Takes a Message record from an MCAP along with its associated Schema,
        and returns the decoded protobuf message from within.

        :param schema: The message schema record from the MCAP.
        :type schema: mcap.records.Schema
        :param message: The message record containing content to be decoded.
        :type message: mcap.records.Message
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
            messages = self._get_message_classes(fds.file)
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
