from typing import Dict

from .data_stream import RecordBuilder
from .records import Channel, Message, MessageIndex, Schema


class ChunkBuilder:
    def __init__(self) -> None:
        self.message_end_time = 0
        self.message_indices: Dict[int, MessageIndex] = {}
        self.message_start_time = 0
        self.record_writer = RecordBuilder()
        self.num_messages = 0

    @property
    def count(self):
        return self.record_writer.count

    def end(self):
        return self.record_writer.end()

    def add_channel(self, channel: Channel):
        channel.write(self.record_writer)

    def add_schema(self, schema: Schema):
        schema.write(self.record_writer)

    def add_message(self, message: Message):
        if self.num_messages == 0:
            self.message_start_time = message.log_time
        else:
            self.message_start_time = min(self.message_start_time, message.log_time)
        self.message_end_time = max(self.message_end_time, message.log_time)

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
        self.record_writer.end()
        self.num_messages = 0
