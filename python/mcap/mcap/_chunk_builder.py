from typing import Dict, List, Tuple

from .data_stream import RecordBuilder
from .opcode import Opcode
from .records import (
    MESSAGE_FIELDS_SIZE,
    MESSAGE_RECORD,
    Channel,
    Message,
    MessageIndex,
    Schema,
)

_OPCODE_MESSAGE = int(Opcode.MESSAGE)


class ChunkBuilder:
    def __init__(self) -> None:
        self.message_end_time = 0
        self.message_indices: Dict[int, MessageIndex] = {}
        self.message_start_time = 0
        self.record_writer = RecordBuilder()
        self.num_messages = 0
        # Each MessageIndex's records list by channel id, saving the hot path a lookup.
        self._index_records: Dict[int, List[Tuple[int, int]]] = {}

    @property
    def count(self):
        return self.record_writer.count

    def end(self) -> bytearray:
        return self.record_writer.end()

    def add_channel(self, channel: Channel):
        channel.write(self.record_writer)

    def add_schema(self, schema: Schema):
        schema.write(self.record_writer)

    def add_message(self, message: Message):
        self.add_message_fields(
            message.channel_id,
            message.sequence,
            message.log_time,
            message.publish_time,
            message.data,
        )

    def add_message_fields(
        self,
        channel_id: int,
        sequence: int,
        log_time: int,
        publish_time: int,
        data: bytes,
    ) -> int:
        """Appends a message record built from its fields, without a Message object.
        Returns the size of the chunk so far."""
        if self.num_messages == 0:
            self.message_start_time = log_time
        elif log_time < self.message_start_time:
            self.message_start_time = log_time
        if log_time > self.message_end_time:
            self.message_end_time = log_time

        records = self._index_records.get(channel_id)
        if records is None:
            index = MessageIndex(channel_id=channel_id, records=[])
            self.message_indices[channel_id] = index
            records = self._index_records[channel_id] = index.records

        buffer = self.record_writer.buffer
        records.append((log_time, len(buffer)))
        self.num_messages += 1
        # Byte count, not len(): len() counts elements for numpy and array.array.
        buffer += MESSAGE_RECORD.pack(
            _OPCODE_MESSAGE,
            MESSAGE_FIELDS_SIZE + memoryview(data).nbytes,  # cspell:disable-line
            channel_id,
            sequence,
            log_time,
            publish_time,
        )
        buffer += data
        return len(buffer)

    def reset(self):
        self.message_end_time = 0
        self.message_indices.clear()
        self._index_records.clear()
        self.message_start_time = 0
        self.record_writer.end()
        self.num_messages = 0
