from collections import defaultdict
from enum import Enum, Flag, auto
from io import BufferedWriter, BytesIO, RawIOBase
from typing import Dict, List, OrderedDict, Union

import zstd

from .chunk_builder import ChunkBuilder
from .data_stream import WriteDataStream
from .opcode import Opcode
from .records import (
    Attachment,
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    DataEnd,
    Footer,
    Header,
    Message,
    Metadata,
    Schema,
    Statistics,
    SummaryOffset,
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


class Writer:
    def __init__(
        self,
        output: Union[str, BytesIO, BufferedWriter],
        chunk_size: int = 1024 * 768,
        compression: CompressionType = CompressionType.NONE,
        index_types: IndexType = IndexType.ALL,
        repeat_channels: bool = True,
        repeat_schemas: bool = True,
        use_chunking: bool = True,
        use_statistics: bool = True,
        use_summary_offsets: bool = True,
    ):
        if isinstance(output, str):
            self.__stream = WriteDataStream(open(output, "wb"))
        elif isinstance(output, RawIOBase):
            self.__stream = WriteDataStream(BufferedWriter(output))
        else:
            self.__stream = WriteDataStream(output)
        self.__attachment_indexes: list[AttachmentIndex] = []
        self.__channels: OrderedDict[int, Channel] = OrderedDict()
        self.__chunk_builder = ChunkBuilder() if use_chunking else None
        self.__chunk_indices: List[ChunkIndex] = []
        self.__chunk_size = chunk_size
        self.__compression = compression
        self.__index_types = index_types
        self.__repeat_channels = repeat_channels
        self.__repeat_schemas = repeat_schemas
        self.__schemas: OrderedDict[int, Schema] = OrderedDict()
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
        self.__summary_offsets: List[SummaryOffset] = []
        self.__use_statistics = use_statistics
        self.__use_summary_offsets = use_summary_offsets

    def add_attachment(self, attachment: Attachment):
        offset = self.__stream.count
        self.__statistics.attachment_count += 1
        attachment.write(self.__stream)
        if self.__index_types & IndexType.ATTACHMENT:
            index = AttachmentIndex(
                offset=offset,
                length=self.__stream.count - offset,
                create_time=attachment.create_time,
                log_time=attachment.log_time,
                data_size=len(attachment.data),
                name=attachment.name,
                content_type=attachment.content_type,
            )
            self.__attachment_indexes.append(index)

    def add_message(self, message: Message):
        if self.__statistics.message_start_time == 0:
            self.__statistics.message_start_time = message.log_time
        self.__statistics.message_end_time = message.log_time
        self.__statistics.channel_message_counts[message.channel_id] += 1
        self.__statistics.message_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_message(message)
            self.__maybe_finalize_chunk()
        else:
            message.write(self.__stream)

    def add_metadata(self, metadata: Metadata):
        metadata.write(self.__stream)

    def finish(self):
        self.__finalize_chunk()

        DataEnd(0).write(self.__stream)

        summary_start = self.__stream.count

        if self.__repeat_schemas:
            group_start = self.__stream.count
            for _id, schema in self.__schemas.items():
                schema.write(self.__stream)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.SCHEMA,
                    group_start=group_start,
                    group_length=self.__stream.count - summary_start,
                )
            )

        if self.__repeat_channels:
            group_start = self.__stream.count
            for _id, channel in self.__channels.items():
                channel.write(self.__stream)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.CHANNEL,
                    group_start=group_start,
                    group_length=self.__stream.count - group_start,
                )
            )

        if self.__use_statistics:
            statistics_start = self.__stream.count
            self.__statistics.write(self.__stream)
            statistics_end = self.__stream.count
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.STATISTICS,
                    group_start=statistics_start,
                    group_length=statistics_end - statistics_start,
                )
            )

        self.__write_indexes()

        summary_offset_start = self.__stream.count if self.__use_summary_offsets else 0
        if self.__use_summary_offsets:
            for offset in self.__summary_offsets:
                offset.write(self.__stream)

        summary_length = self.__stream.count - summary_start

        Footer(
            summary_start=0 if summary_length == 0 else summary_start,
            summary_offset_start=summary_offset_start,
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
        self.__channels[channel_id] = channel
        self.__statistics.channel_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_channel(channel)
            self.__maybe_finalize_chunk()
        else:
            channel.write(self.__stream)
        return channel_id

    def register_schema(self, name: str, encoding: str, data: bytes):
        schema_id = len(self.__schemas) + 1
        schema = Schema(id=schema_id, data=data, encoding=encoding, name=name)
        self.__schemas[schema_id] = schema
        self.__statistics.schema_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_schema(schema)
            self.__maybe_finalize_chunk()
        else:
            schema.write(self.__stream)
        return schema_id

    def start(self, profile: str, library: str):
        self.__stream.write_magic()
        Header(profile, library).write(self.__stream)

    def __finalize_chunk(self):
        if not self.__chunk_builder:
            return

        if self.__chunk_builder.num_messages == 0:
            return

        self.__statistics.chunk_count += 1

        chunk_data = self.__chunk_builder.data()
        if self.__compression == CompressionType.LZ4:
            compression = "lz4"
            compressed_data: bytes = lz4.frame.compress(self.__chunk_builder.data())  # type: ignore
        elif self.__compression == CompressionType.ZSTD:
            compression = "zstd"
            compressed_data: bytes = zstd.ZSTD_compress(self.__chunk_builder.data())  # type: ignore
        else:
            compression = ""
            compressed_data = self.__chunk_builder.data()
        chunk = Chunk(
            compression=compression,
            data=compressed_data,
            message_start_time=self.__chunk_builder.message_start_time,
            message_end_time=self.__chunk_builder.message_end_time,
            uncompressed_crc=0,
            uncompressed_size=len(chunk_data),
        )

        chunk_start_offset = self.__stream.count
        chunk.write(self.__stream)
        chunk_size = self.__stream.count - chunk_start_offset

        chunk_index = ChunkIndex(
            message_start_time=chunk.message_start_time,
            message_end_time=chunk.message_end_time,
            chunk_start_offset=chunk_start_offset,
            chunk_length=chunk_size,
            message_index_offsets={},
            message_index_length=0,
            compression=chunk.compression,
            compressed_size=chunk.uncompressed_size,
            uncompressed_size=chunk.uncompressed_size,
        )

        start_position = self.__stream.count

        if self.__index_types & IndexType.MESSAGE:
            for id, index in self.__chunk_builder.message_indices.items():
                chunk_index.message_index_offsets[id] = self.__stream.count
                index.write(self.__stream)

        chunk_index.message_index_length = self.__stream.count - start_position

        self.__chunk_indices.append(chunk_index)
        self.__chunk_builder.reset()

    def __maybe_finalize_chunk(self):
        if (
            self.__chunk_builder
            and len(self.__chunk_builder.data()) > self.__chunk_size
        ):
            self.__finalize_chunk()

    def __write_indexes(self):
        if self.__index_types & IndexType.CHUNK:
            summary_start = self.__stream.count
            for index in self.__chunk_indices:
                index.write(self.__stream)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.CHUNK_INDEX,
                    group_start=summary_start,
                    group_length=self.__stream.count - summary_start,
                )
            )

        if self.__index_types & IndexType.ATTACHMENT:
            summary_start = self.__stream.count
            for index in self.__attachment_indexes:
                index.write(self.__stream)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.ATTACHMENT_INDEX,
                    group_start=summary_start,
                    group_length=self.__stream.count - summary_start,
                )
            )
