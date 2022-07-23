from typing import Dict, Type

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.message_factory import GetMessages
from mcap.mcap0.exceptions import McapError
from mcap.mcap0.records import Channel, Message, Schema
from mcap.mcap0.stream_reader import StreamReader


class Decoder:
    def __init__(self, reader: StreamReader):
        self.__reader = reader

    @property
    def messages(self):
        channels: Dict[int, Channel] = {}
        schemas: Dict[int, Schema] = {}
        generated: Dict[str, Type[Message]] = {}
        for record in self.__reader.records:
            if isinstance(record, Schema):
                schemas[record.id] = record
                if record.encoding != "protobuf":
                    raise McapError(
                        f"Can't decode schema with encoding {record.encoding}"
                    )
                fds = FileDescriptorSet.FromString(record.data)
                messages = GetMessages(fds.file)
                for name, klass in messages.items():
                    generated[name] = klass  # type: ignore
            if isinstance(record, Channel):
                channels[record.id] = record
            if isinstance(record, Message):
                channel = channels[record.channel_id]
                schema = schemas[channel.schema_id]
                message = generated[schema.name]()  # type: ignore
                message.ParseFromString(record.data)  # type: ignore
                yield (channel.topic, message)
