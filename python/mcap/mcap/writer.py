import struct
import zlib
from collections import defaultdict
from enum import Enum, Flag, auto
from io import BufferedWriter, RawIOBase
from typing import IO, Any, Dict, List, OrderedDict, Union

import lz4.frame  # type: ignore
import zstandard

from mcap import __version__

from ._chunk_builder import ChunkBuilder
from .data_stream import RecordBuilder
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
    MetadataIndex,
    Schema,
    Statistics,
    SummaryOffset,
)

MCAP0_MAGIC = struct.pack("<8B", 137, 77, 67, 65, 80, 48, 13, 10)
LIBRARY_IDENTIFIER = f"python mcap {__version__}"


class CompressionType(Enum):
    NONE = auto()
    LZ4 = auto()
    ZSTD = auto()


class IndexType(Flag):
    """Determines what indexes should be written to the MCAP file. If in doubt, choose ALL."""

    NONE = auto()
    ATTACHMENT = auto()
    CHUNK = auto()
    MESSAGE = auto()
    METADATA = auto()
    ALL = ATTACHMENT | CHUNK | MESSAGE | METADATA


class Writer:
    """
    Writes MCAP data.

    :param output: A filename or stream to write to.
    :param chunk_size: The maximum size of individual data chunks in a chunked file.
    :param compression: Compression to apply to chunk data, if any.
    :param index_types: Indexes to write to the file. See IndexType for possibilities.
    :param repeat_channels: Repeat channel information at the end of the file.
    :param repeat_schemas: Repeat schemas at the end of the file.
    :param use_chunking: Group data in chunks.
    :param use_statistics: Write statistics record.
    :param use_summary_offsets: Write summary offset records.
    """

    def __init__(
        self,
        output: Union[str, IO[Any], BufferedWriter],
        chunk_size: int = 1024 * 1024,
        compression: CompressionType = CompressionType.ZSTD,
        index_types: IndexType = IndexType.ALL,
        repeat_channels: bool = True,
        repeat_schemas: bool = True,
        use_chunking: bool = True,
        use_statistics: bool = True,
        use_summary_offsets: bool = True,
        enable_crcs: bool = True,
        enable_data_crcs: bool = False,
    ):
        self.__should_close = False
        if isinstance(output, str):
            self.__stream = open(output, "wb")
            self.__should_close = True
        elif isinstance(output, RawIOBase):
            self.__stream = BufferedWriter(output)
        else:
            self.__stream = output
        self.__record_builder = RecordBuilder()
        self.__attachment_indexes: list[AttachmentIndex] = []
        self.__metadata_indexes: list[MetadataIndex] = []
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
        self.__enable_crcs = enable_crcs
        self.__enable_data_crcs = enable_data_crcs
        self.__data_section_crc = 0

    def add_attachment(
        self, create_time: int, log_time: int, name: str, media_type: str, data: bytes
    ):
        """
        Adds an attachment to the file.

        :param log_time: Time at which the attachment was recorded.
        :param create_time: Time at which the attachment was created. If not available,
            must be set to zero.
        :param name: Name of the attachment, e.g "scene1.jpg".
        :param media_type: Media Type (e.g "text/plain").
        :param data: Attachment data.
        """
        self.__flush()
        offset = self.__stream.tell()
        self.__statistics.attachment_count += 1
        attachment = Attachment(
            create_time=create_time,
            log_time=log_time,
            name=name,
            media_type=media_type,
            data=data,
        )
        attachment.write(self.__record_builder)
        if self.__index_types & IndexType.ATTACHMENT:
            index = AttachmentIndex(
                offset=offset,
                length=self.__record_builder.count,
                create_time=attachment.create_time,
                log_time=attachment.log_time,
                data_size=len(attachment.data),
                name=attachment.name,
                media_type=attachment.media_type,
            )
            self.__attachment_indexes.append(index)
        self.__flush()

    def add_message(
        self,
        channel_id: int,
        log_time: int,
        data: bytes,
        publish_time: int,
        sequence: int = 0,
    ):
        """
        Adds a new message to the file. If chunking is enabled the message will be added to the
        current chunk.

        :param channel_id: The id of the channel to which the message should be added.
        :param sequence: Optional message counter assigned by publisher.
        :param log_time: Time at which the message was recorded as nanoseconds since a
            user-understood epoch (i.e unix epoch, robot boot time, etc.).
        :param publish_time: Time at which the message was published as nanoseconds since a
            user-understood epoch (i.e unix epoch, robot boot time, etc.).
        :param data: Message data, to be decoded according to the schema of the channel.
        """
        message = Message(
            channel_id=channel_id,
            log_time=log_time,
            data=data,
            publish_time=publish_time,
            sequence=sequence,
        )
        if self.__statistics.message_count == 0:
            self.__statistics.message_start_time = log_time
        else:
            self.__statistics.message_start_time = min(
                log_time, self.__statistics.message_start_time
            )
        self.__statistics.message_end_time = max(
            log_time, self.__statistics.message_end_time
        )
        self.__statistics.channel_message_counts[message.channel_id] += 1
        self.__statistics.message_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_message(message)
            self.__maybe_finalize_chunk()
        else:
            message.write(self.__record_builder)
            self.__flush()

    def add_metadata(self, name: str, data: Dict[str, str]):
        """
        Adds key-value metadata to the file.

        :param name: A name to associate with the metadata.
        :param data: Key-value metadata.
        """
        self.__flush()
        offset = self.__stream.tell()
        self.__statistics.metadata_count += 1
        metadata = Metadata(name=name, metadata=data)
        metadata.write(self.__record_builder)
        if self.__index_types & IndexType.METADATA:
            index = MetadataIndex(
                offset=offset, length=self.__record_builder.count, name=name
            )
            self.__metadata_indexes.append(index)
        self.__flush()

    def finish(self):
        """
        Writes any final indexes, summaries etc to the file. Note that it does
        not close the underlying output stream.
        """
        self.__finalize_chunk()

        DataEnd(self.__data_section_crc).write(self.__record_builder)
        self.__flush()

        summary_start = self.__stream.tell()
        summary_builder = RecordBuilder()

        if self.__repeat_schemas:
            group_start = summary_builder.count
            for schema in self.__schemas.values():
                schema.write(summary_builder)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.SCHEMA,
                    group_start=summary_start + group_start,
                    group_length=summary_builder.count - group_start,
                )
            )

        if self.__repeat_channels:
            group_start = summary_builder.count
            for channel in self.__channels.values():
                channel.write(summary_builder)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.CHANNEL,
                    group_start=summary_start + group_start,
                    group_length=summary_builder.count - group_start,
                )
            )

        if self.__use_statistics:
            group_start = summary_builder.count
            self.__statistics.write(summary_builder)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.STATISTICS,
                    group_start=summary_start + group_start,
                    group_length=summary_builder.count - group_start,
                )
            )

        if self.__index_types & IndexType.CHUNK:
            group_start = summary_builder.count
            for index in self.__chunk_indices:
                index.write(summary_builder)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.CHUNK_INDEX,
                    group_start=summary_start + group_start,
                    group_length=summary_builder.count - group_start,
                )
            )

        if self.__index_types & IndexType.ATTACHMENT:
            group_start = summary_builder.count
            for index in self.__attachment_indexes:
                index.write(summary_builder)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.ATTACHMENT_INDEX,
                    group_start=summary_start + group_start,
                    group_length=summary_builder.count - group_start,
                )
            )

        if self.__index_types & IndexType.METADATA:
            group_start = summary_builder.count
            for index in self.__metadata_indexes:
                index.write(summary_builder)
            self.__summary_offsets.append(
                SummaryOffset(
                    group_opcode=Opcode.METADATA_INDEX,
                    group_start=summary_start + group_start,
                    group_length=summary_builder.count - group_start,
                )
            )

        summary_offset_start = (
            summary_start + summary_builder.count if self.__use_summary_offsets else 0
        )
        if self.__use_summary_offsets:
            for offset in self.__summary_offsets:
                offset.write(summary_builder)

        summary_data = summary_builder.end()
        summary_length = len(summary_data)

        summary_crc = 0
        if self.__enable_crcs:
            summary_crc = zlib.crc32(summary_data)
            summary_crc = zlib.crc32(
                struct.pack(
                    "<BQQQ",  # cspell:disable-line
                    Opcode.FOOTER,
                    8 + 8 + 4,
                    0 if summary_length == 0 else summary_start,
                    summary_offset_start,
                ),
                summary_crc,
            )

        self.__stream.write(summary_data)

        Footer(
            summary_start=0 if summary_length == 0 else summary_start,
            summary_offset_start=summary_offset_start,
            summary_crc=summary_crc,
        ).write(self.__record_builder)

        self.__flush()
        self.__stream.write(MCAP0_MAGIC)
        if self.__should_close:
            self.__stream.close()

    def register_channel(
        self,
        topic: str,
        message_encoding: str,
        schema_id: int,
        metadata: Dict[str, str] = {},
    ) -> int:
        """
        Registers a new message channel. Returns the numeric id of the new channel.

        :param schema_id: The schema for messages on this channel. A schema_id of 0 indicates there
            is no schema for this channel.
        :param topic: The channel topic.
        :param message_encoding: Encoding for messages on this channel. See the list of well-known
            message encodings for common values.
        :param metadata: Metadata about this channel.
        """
        channel_id = len(self.__channels) + 1
        channel = Channel(
            id=channel_id,
            topic=topic,
            message_encoding=message_encoding,
            schema_id=schema_id,
            metadata=metadata,
        )
        self.__channels[channel_id] = channel
        self.__statistics.channel_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_channel(channel)
            self.__maybe_finalize_chunk()
        else:
            channel.write(self.__record_builder)
        return channel_id

    def register_schema(self, name: str, encoding: str, data: bytes):
        """
        Registers a new message schema. Returns the new integer schema id.

        :param name: An identifier for the schema.
        :param encoding: Format for the schema. See the list of well-known schema encodings for
            common values. An empty string indicates no schema is available.
        :param data: Schema data. Must conform to the schema encoding. If `encoding` is an empty
            string, `data` should be 0 length.
        """
        schema_id = len(self.__schemas) + 1
        schema = Schema(id=schema_id, data=data, encoding=encoding, name=name)
        self.__schemas[schema_id] = schema
        self.__statistics.schema_count += 1
        if self.__chunk_builder:
            self.__chunk_builder.add_schema(schema)
            self.__maybe_finalize_chunk()
        else:
            schema.write(self.__record_builder)
        return schema_id

    def start(self, profile: str = "", library: str = LIBRARY_IDENTIFIER):
        """
        Starts writing to the output stream.

        :param profile: The profile is used for indicating requirements for fields
            throughout the file (encoding, user_data, etc).
        :param library: Free-form string for writer to specify its name, version, or other
            information for use in debugging.
        """
        self.__stream.write(MCAP0_MAGIC)
        if self.__enable_data_crcs:
            self.__data_section_crc = zlib.crc32(MCAP0_MAGIC, self.__data_section_crc)
        Header(profile, library).write(self.__record_builder)
        self.__flush()

    def __flush(self):
        data = self.__record_builder.end()
        if self.__enable_data_crcs:
            self.__data_section_crc = zlib.crc32(data, self.__data_section_crc)
        self.__stream.write(data)

    def __finalize_chunk(self):
        if not self.__chunk_builder:
            return

        if self.__chunk_builder.num_messages == 0:
            return

        self.__statistics.chunk_count += 1

        chunk_data = self.__chunk_builder.end()
        if self.__compression == CompressionType.LZ4:
            compression = "lz4"
            compressed_data: bytes = lz4.frame.compress(chunk_data)  # type: ignore
        elif self.__compression == CompressionType.ZSTD:
            compression = "zstd"
            compressed_data: bytes = zstandard.compress(chunk_data)  # type: ignore
        else:
            compression = ""
            compressed_data = chunk_data
        chunk = Chunk(
            compression=compression,
            data=compressed_data,
            message_start_time=self.__chunk_builder.message_start_time,
            message_end_time=self.__chunk_builder.message_end_time,
            uncompressed_crc=zlib.crc32(chunk_data) if self.__enable_crcs else 0,
            uncompressed_size=len(chunk_data),
        )

        self.__flush()
        chunk_start_offset = self.__stream.tell()
        chunk.write(self.__record_builder)
        chunk_size = self.__record_builder.count

        chunk_index = ChunkIndex(
            message_start_time=chunk.message_start_time,
            message_end_time=chunk.message_end_time,
            chunk_start_offset=chunk_start_offset,
            chunk_length=chunk_size,
            message_index_offsets={},
            message_index_length=0,
            compression=chunk.compression,
            compressed_size=len(compressed_data),
            uncompressed_size=chunk.uncompressed_size,
        )

        self.__flush()
        message_index_start_offset = self.__stream.tell()

        if self.__index_types & IndexType.MESSAGE:
            for id, index in self.__chunk_builder.message_indices.items():
                chunk_index.message_index_offsets[id] = (
                    message_index_start_offset + self.__record_builder.count
                )
                index.write(self.__record_builder)

        chunk_index.message_index_length = self.__record_builder.count

        self.__flush()

        self.__chunk_indices.append(chunk_index)
        self.__chunk_builder.reset()

    def __maybe_finalize_chunk(self):
        if self.__chunk_builder and self.__chunk_builder.count > self.__chunk_size:
            self.__finalize_chunk()


__all__ = ["CompressionType", "IndexType", "Writer"]
