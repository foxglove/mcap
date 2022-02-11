from collections import defaultdict
from enum import Enum, Flag, auto
from io import BufferedWriter, BytesIO, RawIOBase
from typing import Dict, Union

from .chunk_builder import ChunkBuilder
from .data_stream import WriteDataStream
from .records import (
    Attachment,
    AttachmentIndex,
    Channel,
    Chunk,
    DataEnd,
    Footer,
    Header,
    Message,
    Metadata,
    Schema,
    Statistics,
)


class CompressionType(Enum):
    NONE = auto()
    ZSTD = auto()
    LZ4 = auto()


class IndexType(Flag):
    NONE = auto()
    ATTACHMENT = auto()
    CHUNK = auto()
    MESSAGE = auto()
    METDATA = auto()
    ALL = ATTACHMENT | CHUNK | MESSAGE | METDATA


class StreamWriter:
    def __init__(
        self,
        output: Union[str, BytesIO, BufferedWriter],
        profile: str,
        chunk_size: int = 1024 * 768,
        compression: CompressionType = CompressionType.ZSTD,
        index_types: IndexType = IndexType.ALL,
        metadata: Dict[str, str] = {},
        use_chunking: bool = True,
        use_statistics: bool = True,
    ):
        if isinstance(output, str):
            self.__stream = WriteDataStream(open(output, "wb"))
        elif isinstance(output, RawIOBase):
            self.__stream = WriteDataStream(BufferedWriter(output))
        else:
            self.__stream = WriteDataStream(output)
        self.__attachment_indexes: list[AttachmentIndex] = []
        self.__channels: Dict[int, Channel] = {}
        self.__chunk_builder = ChunkBuilder() if use_chunking else None
        self.__chunk_size = chunk_size
        self.__index_types = index_types
        self.__statistics = Statistics(
            attachment_count=0,
            channel_count=0,
            channel_message_counts=defaultdict(int),
            chunk_count=0,
            message_count=0,
            metadata_count=0,
            message_start_time=0,
            message_end_time=0,
            schema_count=0,
        )
        self.__use_statistics = use_statistics

    def add_attachment(self, attachment: Attachment):
        self.__statistics.attachment_count += 1
        if self.__index_types & IndexType.ATTACHMENT:
            index = AttachmentIndex(
                offset=self.__stream.count,
                length=0,
                log_time=attachment.log_time,
                data_size=len(attachment.data),
                name=attachment.name,
                content_type=attachment.content_type,
            )
            self.__attachment_indexes.append(index)
        attachment.write(self.__stream)

    def add_message(self, message: Message):
        if self.__statistics.message_start_time == 0:
            self.__statistics.message_start_time = message.log_time
        self.__statistics.message_end_time = message.log_time
        self.__statistics.channel_message_counts[message.channel_id] += 1
        self.__statistics.message_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_message(message)
        else:
            message.write(self.__stream)

    def add_metadata(self, metadata: Metadata):
        metadata.write(self.__stream)

    def add_schema(self, schema: Schema):
        self.__statistics.schema_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_schema(schema)
        else:
            schema.write(self.__stream)

    def finish(self):
        DataEnd(0).write(self.__stream)
        if self.__use_statistics or self.__index_types != IndexType.NONE:
            summary_start = self.__stream.count
        else:
            summary_start = 0
        if self.__use_statistics:
            self.__statistics.write(self.__stream)
        self.__write_indexes()
        Footer(
            summary_start=summary_start,
            summary_offset_start=0,
            summary_crc=0,
        ).write(self.__stream)
        self.__stream.write_magic()

    def register_channel(
        self,
        topic: str,
        message_encoding: str,
        metadata: Dict[str, str],
    ) -> int:
        channel_id = len(self.__channels) + 1
        channel = Channel(
            id=channel_id,
            topic=topic,
            message_encoding=message_encoding,
            schema_id=1,
            metadata=metadata,
        )
        self.__statistics.channel_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_channel(channel)
        else:
            channel.write(self.__stream)
        return channel_id

    def start(self, profile: str, library: str):
        self.__stream.write_magic()
        Header(profile, library).write(self.__stream)

    def start_chunk(self, compression: str = ""):
        self.__chunk = Chunk(
            compression=compression,
            message_start_time=0,
            message_end_time=0,
            uncompressed_crc=0,
            uncompressed_size=0,
            data=bytes(),
        )
        self.__chunk_stream = WriteDataStream(BytesIO())

    def __write_indexes(self):
        for index in self.__attachment_indexes:
            index.write(self.__stream)
