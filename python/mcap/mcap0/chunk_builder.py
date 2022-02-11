from collections import defaultdict
from io import BytesIO
from typing import Dict

from .data_stream import WriteDataStream
from .records import Channel, Message, MessageIndex, Schema


class ChunkBuilder:
    def __init__(self) -> None:
        self.__message_end_time = 0
        self.__message_indices: Dict[int, MessageIndex] = defaultdict(
            lambda: MessageIndex(0, [])
        )
        self.__message_start_time = 0
        self.__buffer = BytesIO()
        self.__record_writer = WriteDataStream(self.__buffer)
        self.__total_message_count = 0

    @property
    def buffer(self):
        return self.__buffer.getvalue()

    @property
    def indices(self):
        return self.__message_indices.values()

    @property
    def num_messages(self):
        return self.__total_message_count

    def add_channel(self, channel: Channel):
        channel.write(self.__record_writer)

    def add_schema(self, schema: Schema):
        schema.write(self.__record_writer)

    def add_message(self, message: Message):
        if self.__message_start_time == 0:
            self.__message_start_time = message.log_time
        self.__message_end_time = message.log_time

        self.__message_indices[message.channel_id].records.append(
            (message.log_time, self.__record_writer.count)
        )

        self.__total_message_count += 1
        message.write(self.__record_writer)

    def reset(self):
        self.__message_end_time = 0
        self.__message_indices.clear()
        self.__message_start_time = 0
        self.__buffer = BytesIO()
        self.__record_writer = WriteDataStream(self.__buffer)
        self._total_mssage_count = 0
