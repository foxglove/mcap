from io import BytesIO
from typing import Dict

from .data_stream import WriteDataStream
from .records import Channel, Message, MessageIndex, Schema


class ChunkBuilder:
    def __init__(self) -> None:
        self.message_end_time = 0
        self.message_indices: Dict[int, MessageIndex] = {}
        self.message_start_time = 0
        self.__buffer = BytesIO()
        self.record_writer = WriteDataStream(self.__buffer)
        self.num_messages = 0

    def data(self):
        return self.__buffer.getvalue()

    def add_channel(self, channel: Channel):
        channel.write(self.record_writer)

    def add_schema(self, schema: Schema):
        schema.write(self.record_writer)

    def add_message(self, message: Message):
        if self.message_start_time == 0:
            self.message_start_time = message.log_time
        self.message_end_time = message.log_time

        if not self.message_indices.get(message.channel_id):
            self.message_indices[message.channel_id] = MessageIndex(
                channel_id=message.channel_id, records=[]
            )
        self.message_indices[message.channel_id].records.append(
            (message.log_time, self.record_writer.count)
        )

        self.num_messages += 1
        message.write(self.record_writer)

    def reset(self):
        self.message_end_time = 0
        self.message_indices.clear()
        self.message_start_time = 0
        self.buffer = BytesIO()
        self.record_writer = WriteDataStream(self.buffer)
        self.num_messages = 0
