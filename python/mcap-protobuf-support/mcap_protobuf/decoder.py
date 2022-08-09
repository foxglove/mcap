from typing import Iterator, Tuple, Any, Dict

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.message_factory import GetMessages
from mcap.mcap0.exceptions import McapError
from mcap.mcap0.records import Channel, Message, Schema
from mcap.mcap0.well_known import SchemaEncoding


def decode_proto_messages(
    message_iterator: Iterator[Tuple[Schema, Channel, Message]],
    ignore_non_protobuf_messages: bool = False,
) -> Iterator[Tuple[str, Any, int]]:
    """takes a stream of messages from a McapReader, and automatically parses the Protobuf
    messages using the definitions in the MCAP.

    :param message_iterator: an iterator of Schema, Channel, and Message records.
        Use :py:func:`mcap.mcap0.reader.McapReader.iter_messages()` to create this.
    :param ignore_non_protobuf_messages: if True, ignores non-protobuf messages in the MCAP rather
        than raising an exception.
    :returns: an iterator of (topic, protobuf message, log_time) tuples. Timestamps are provided
        as a nanosecond unix timestamp.
    """
    generated: Dict[str, Any] = {}
    for schema, channel, record in message_iterator:
        if schema.encoding != SchemaEncoding.Protobuf:
            if ignore_non_protobuf_messages:
                continue
            raise McapError(f"Can't decode schema with encoding {schema.encoding}")
        generated_type = generated.get(schema.name)
        if generated_type is None:
            fds = FileDescriptorSet.FromString(schema.data)
            messages = GetMessages(fds.file)
            for name, klass in messages.items():
                generated[name] = klass  # type: ignore
                if name == schema.name:
                    generated_type = klass
        if generated_type is None:
            raise McapError(
                f"FileDescriptorSet for type {schema.name} is missing that schema"
            )
        message = generated_type()
        message.ParseFromString(record.data)
        yield channel.topic, message, record.log_time
