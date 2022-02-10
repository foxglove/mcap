from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from .data_stream import ReadDataStream, WriteDataStream
from .opcode import Opcode


@dataclass
class McapRecord:
    def write(self, stream: WriteDataStream):
        pass


@dataclass
class Attachment(McapRecord):
    name: str
    created_at: int
    log_time: int
    content_type: str
    data: bytes

    def write(self, stream: WriteDataStream):
        stream.start_record(Opcode.ATTACHMENT)
        stream.write_prefixed_string(self.name)
        stream.write8(self.created_at)
        stream.write8(self.log_time)
        stream.write_prefixed_string(self.content_type)
        stream.write8(len(self.data))
        stream.write(self.data)
        stream.write4(0)  # TODO: crc
        stream.finish_record()

    @staticmethod
    def read(stream: ReadDataStream):
        name = stream.read_prefixed_string()
        created_at = stream.read8()
        log_time = stream.read8()
        content_type = stream.read_prefixed_string()
        data_length = stream.read8()
        data = stream.read(data_length)
        _crc = stream.read4()
        return Attachment(
            name=name,
            created_at=created_at,
            log_time=log_time,
            content_type=content_type,
            data=data,
        )


@dataclass
class AttachmentIndex(McapRecord):
    offset: int
    length: int
    log_time: int
    data_size: int
    name: str
    content_type: str

    @staticmethod
    def read(stream: ReadDataStream):
        offset = stream.read8()
        length = stream.read8()
        log_time = stream.read8()
        data_size = stream.read8()
        name = stream.read_prefixed_string()
        content_type = stream.read_prefixed_string()
        return AttachmentIndex(
            offset=offset,
            length=length,
            log_time=log_time,
            data_size=data_size,
            name=name,
            content_type=content_type,
        )


@dataclass
class Channel(McapRecord):
    id: int = field(metadata={"value_type": ["int"]})
    topic: str
    message_encoding: str
    metadata: Dict[str, str]
    schema_id: int = field(metadata={"value_type": ["int"]})

    def write(self, stream: WriteDataStream):
        stream.start_record(Opcode.CHANNEL)
        stream.write2(self.id)
        stream.write2(self.schema_id)
        stream.write_prefixed_string(self.topic)
        stream.write_prefixed_string(self.message_encoding)
        for k, v in self.metadata.items():
            stream.write_prefixed_string(k)
            stream.write_prefixed_string(v)
        stream.finish_record()

    @staticmethod
    def read(stream: ReadDataStream):
        id = stream.read2()
        schema_id = stream.read2()
        topic = stream.read_prefixed_string()
        message_encoding = stream.read_prefixed_string()
        metadata_length = stream.read4()
        metadata_end = stream.count + metadata_length
        metadata: Dict[str, str] = {}
        while stream.count < metadata_end:
            key = stream.read_prefixed_string()
            value = stream.read_prefixed_string()
            metadata[key] = value
        return Channel(
            id=id,
            topic=topic,
            message_encoding=message_encoding,
            metadata=metadata,
            schema_id=schema_id,
        )


@dataclass
class Chunk(McapRecord):
    compression: str
    data: bytes = field(repr=False)
    message_end_time: int
    message_start_time: int
    uncompressed_crc: int
    uncompressed_size: int

    @staticmethod
    def read(stream: ReadDataStream):
        message_start_time = stream.read8()
        message_end_time = stream.read8()
        uncompressed_size = stream.read8()
        uncompressed_crc = stream.read4()
        compression_length = stream.read4()
        compression = str(stream.read(compression_length), "utf-8")
        data_length = stream.read8()
        data = stream.read(data_length)
        return Chunk(
            compression=compression,
            data=data,
            message_end_time=message_end_time,
            message_start_time=message_start_time,
            uncompressed_crc=uncompressed_crc,
            uncompressed_size=uncompressed_size,
        )


@dataclass
class ChunkIndex(McapRecord):
    chunk_length: int
    chunk_start_offset: int
    compression: str
    compressed_size: int
    message_end_time: int
    message_index_length: int
    message_index_offsets: Dict[int, int] = field(
        metadata={"value_type": ["int", "long"]}
    )
    message_start_time: int
    uncompressed_size: int

    @staticmethod
    def read(stream: ReadDataStream):
        message_start_time = stream.read8()
        message_end_time = stream.read8()
        chunk_start_offset = stream.read8()
        chunk_length = stream.read8()
        message_index_offsets_length = stream.read4()
        message_index_offsets: Dict[int, int] = {}
        offsets_end = stream.count + message_index_offsets_length
        while stream.count < offsets_end:
            channel_id = stream.read2()
            channel_offset = stream.read8()
            message_index_offsets[channel_id] = channel_offset
        message_index_length = stream.read8()
        compression = stream.read_prefixed_string()
        compressed_size = stream.read8()
        uncompressed_size = stream.read8()
        return ChunkIndex(
            message_index_offsets=message_index_offsets,
            chunk_start_offset=chunk_start_offset,
            chunk_length=chunk_length,
            compression=compression,
            compressed_size=compressed_size,
            message_end_time=message_end_time,
            message_index_length=message_index_length,
            message_start_time=message_start_time,
            uncompressed_size=uncompressed_size,
        )


@dataclass
class DataEnd(McapRecord):
    data_section_crc: int

    @staticmethod
    def read(stream: ReadDataStream):
        data_section_crc = stream.read4()
        return DataEnd(data_section_crc=data_section_crc)


@dataclass
class Footer(McapRecord):
    summary_start: int
    summary_offset_start: int
    summary_crc: int = field(metadata={"value_type": ["int"]})

    def write(self, stream: WriteDataStream):
        stream.start_record(Opcode.FOOTER)
        stream.write8(self.summary_start)
        stream.write8(self.summary_offset_start)
        stream.write4(self.summary_crc)
        stream.finish_record()

    @staticmethod
    def read(stream: ReadDataStream):
        summary_start = stream.read8()
        summary_offset_start = stream.read8()
        summary_crc = stream.read4()
        return Footer(
            summary_start=summary_start,
            summary_offset_start=summary_offset_start,
            summary_crc=summary_crc,
        )


@dataclass
class Header(McapRecord):
    profile: str
    library: str

    def write(self, stream: WriteDataStream):
        stream.start_record(Opcode.HEADER)
        stream.write_prefixed_string(self.profile)
        stream.write_prefixed_string(self.library)
        stream.finish_record()

    @staticmethod
    def read(stream: ReadDataStream):
        profile = stream.read_prefixed_string()
        library = stream.read_prefixed_string()
        return Header(profile, library)


@dataclass
class Message(McapRecord):
    channel_id: int = field(metadata={"value_type": ["int"]})
    log_time: int
    data: bytes
    publish_time: int
    sequence: int = field(metadata={"value_type": ["int"]})

    def write(self, stream: WriteDataStream):
        stream.start_record(Opcode.MESSAGE)
        stream.write2(self.channel_id)
        stream.write4(self.sequence)
        stream.write8(self.log_time)
        stream.write8(self.publish_time)
        stream.write(self.data)
        stream.finish_record()

    @staticmethod
    def read(stream: ReadDataStream, length: int):
        channel_id = stream.read2()
        sequence = stream.read4()
        log_time = stream.read8()
        publish_time = stream.read8()
        data = stream.read(length - 22)
        return Message(
            channel_id=channel_id,
            log_time=log_time,
            data=data,
            publish_time=publish_time,
            sequence=sequence,
        )


@dataclass
class MessageIndex(McapRecord):
    channel_id: int = field(metadata={"value_type": ["int"]})
    records: List[Tuple[int, int]] = field(metadata={"value_type": ["long", "long"]})

    @staticmethod
    def read(stream: ReadDataStream):
        channel_id = stream.read2()
        records_length = stream.read4()
        entries: list[Tuple[int, int]] = []
        records_end = stream.count + records_length
        while stream.count < records_end:
            timestamp = stream.read8()
            offset = stream.read8()
            entries.append((timestamp, offset))
        return MessageIndex(channel_id, entries)


@dataclass
class MetadataIndex(McapRecord):
    offset: int
    length: int
    name: str

    @staticmethod
    def read(stream: ReadDataStream):
        offset = stream.read8()
        length = stream.read8()
        name = stream.read_prefixed_string()
        return MetadataIndex(offset=offset, length=length, name=name)


@dataclass
class Schema(McapRecord):
    id: int = field(metadata={"value_type": ["int"]})
    data: bytes
    encoding: str
    name: str

    def write(self, stream: WriteDataStream):
        stream.start_record(Opcode.SCHEMA)
        stream.write2(self.id)
        stream.write_prefixed_string(self.name)
        stream.write_prefixed_string(self.encoding)
        stream.write4(len(self.data))
        stream.write(self.data)
        stream.finish_record()

    @staticmethod
    def read(stream: ReadDataStream):
        id = stream.read2()
        name = stream.read_prefixed_string()
        encoding = stream.read_prefixed_string()
        data_length = stream.read4()
        data = stream.read(data_length)
        return Schema(id=id, name=name, encoding=encoding, data=data)


@dataclass
class Statistics(McapRecord):
    attachment_count: int = field(metadata={"value_type": ["int"]})
    channel_count: int = field(metadata={"value_type": ["int"]})
    channel_message_counts: Dict[int, int] = field(
        metadata={"value_type": ["int", "long"]}
    )
    chunk_count: int = field(metadata={"value_type": ["int"]})
    message_count: int
    metadata_count: int = field(metadata={"value_type": ["int"]})
    schema_count: int = field(metadata={"value_type": ["int"]})

    @staticmethod
    def read(stream: ReadDataStream):
        message_count = stream.read8()
        schema_count = stream.read2()
        channel_count = stream.read4()
        attachment_count = stream.read4()
        metadata_count = stream.read4()
        chunk_count = stream.read4()
        channel_message_counts_length = stream.read4()
        message_counts: Dict[int, int] = {}
        counts_end = stream.count + channel_message_counts_length
        while stream.count < counts_end:
            channel_id = stream.read2()
            message_count = stream.read8()
            message_counts[channel_id] = message_count
        return Statistics(
            attachment_count=attachment_count,
            channel_count=channel_count,
            channel_message_counts=message_counts,
            chunk_count=chunk_count,
            message_count=message_count,
            metadata_count=metadata_count,
            schema_count=schema_count,
        )


@dataclass
class SummaryOffset(McapRecord):
    group_opcode: int = field(metadata={"value_type": ["int"]})
    group_start: int
    group_length: int

    @staticmethod
    def read(stream: ReadDataStream):
        group_opcode = stream.read1()
        group_start = stream.read8()
        group_length = stream.read8()
        return SummaryOffset(
            group_opcode=group_opcode,
            group_start=group_start,
            group_length=group_length,
        )
