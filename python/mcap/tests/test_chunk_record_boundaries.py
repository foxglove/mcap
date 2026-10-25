import struct
import zlib
from io import BytesIO
from typing import Type, Union

import lz4.frame
import pytest
import zstandard

from mcap.data_stream import RecordBuilder
from mcap.exceptions import EndOfFile, McapError
from mcap.reader import NonSeekingReader, SeekingReader
from mcap.records import (
    Channel,
    Chunk,
    ChunkIndex,
    DataEnd,
    Footer,
    Header,
    McapRecord,
    Message,
    Schema,
)
from mcap.stream_reader import breakup_chunk
from mcap.writer import MCAP0_MAGIC

SCHEMA = Schema(id=1, name="sample", encoding="jsonschema", data=b"true")
CHANNEL = Channel(
    id=1,
    schema_id=1,
    topic="sample_topic",
    message_encoding="json",
    metadata={"key": "value"},
)
MESSAGE = Message(
    channel_id=1, log_time=42, publish_time=43, sequence=7, data=b'{"sample":"test"}'
)


def serialize(record: McapRecord, extension: bytes = b"") -> bytes:
    builder = RecordBuilder()
    record.write(builder)
    data = builder.end()
    return (
        data[:1]
        + struct.pack("<Q", len(data) - 9 + len(extension))
        + data[9:]
        + extension
    )


def make_chunk(data: bytes, compression: str) -> Chunk:
    if compression == "zstd":
        compressed = zstandard.compress(data)
    elif compression == "lz4":
        compressed = lz4.frame.compress(data)
    else:
        compressed = data
    return Chunk(
        compression=compression,
        data=compressed,
        message_start_time=MESSAGE.log_time,
        message_end_time=MESSAGE.log_time,
        uncompressed_size=len(data),
        uncompressed_crc=zlib.crc32(data),
    )


def make_indexed_file(chunk: Chunk) -> BytesIO:
    builder = RecordBuilder()
    builder.write(MCAP0_MAGIC)
    Header(profile="", library="test").write(builder)
    chunk_offset = builder.count
    chunk.write(builder)
    chunk_length = builder.count - chunk_offset
    DataEnd(data_section_crc=0).write(builder)
    summary_start = builder.count
    SCHEMA.write(builder)
    CHANNEL.write(builder)
    ChunkIndex(
        chunk_start_offset=chunk_offset,
        chunk_length=chunk_length,
        message_start_time=chunk.message_start_time,
        message_end_time=chunk.message_end_time,
        message_index_offsets={},
        message_index_length=0,
        compression=chunk.compression,
        compressed_size=len(chunk.data),
        uncompressed_size=chunk.uncompressed_size,
    ).write(builder)
    Footer(summary_start=summary_start, summary_offset_start=0, summary_crc=0).write(
        builder
    )
    builder.write(MCAP0_MAGIC)
    return BytesIO(builder.end())


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
@pytest.mark.parametrize("compression", ["", "zstd", "lz4"])
@pytest.mark.parametrize("extended", ["none", "schema", "channel", "both"])
def test_chunk_record_extensions(
    reader_cls: Union[Type[SeekingReader], Type[NonSeekingReader]],
    compression: str,
    extended: str,
):
    extension = b"\x00future fields"
    data = (
        serialize(SCHEMA, extension if extended in ("schema", "both") else b"")
        + serialize(CHANNEL, extension if extended in ("channel", "both") else b"")
        + serialize(MESSAGE)
    )
    reader = reader_cls(
        make_indexed_file(make_chunk(data, compression)), validate_crcs=True
    )
    assert list(reader.iter_messages()) == [(SCHEMA, CHANNEL, MESSAGE)]


@pytest.mark.parametrize("record", [SCHEMA, CHANNEL])
@pytest.mark.parametrize("compression", ["", "zstd", "lz4"])
def test_chunk_record_known_fields_exceed_length(record: McapRecord, compression: str):
    data = serialize(record)
    data = data[:1] + struct.pack("<Q", len(data) - 10) + data[9:]
    with pytest.raises(McapError, match="declared length"):
        breakup_chunk(make_chunk(data + serialize(MESSAGE), compression))


@pytest.mark.parametrize("record", [SCHEMA, CHANNEL])
@pytest.mark.parametrize("compression", ["", "zstd", "lz4"])
def test_chunk_record_field_length_exceeds_boundary(
    record: Union[Schema, Channel], compression: str
):
    data = serialize(record)
    field_length = len(record.data) if isinstance(record, Schema) else len("value")
    length_offset = len(data) - field_length - 4
    data = (
        data[:length_offset]
        + struct.pack("<I", field_length + 1)
        + data[length_offset + 4 :]
    )
    with pytest.raises(McapError, match="declared length"):
        breakup_chunk(make_chunk(data, compression))


@pytest.mark.parametrize("record", [SCHEMA, CHANNEL])
@pytest.mark.parametrize("compression", ["", "zstd", "lz4"])
@pytest.mark.parametrize("extension", [b"", b"\x00future fields"])
def test_chunk_record_truncated(record: McapRecord, compression: str, extension: bytes):
    data = serialize(record, extension)[:-1]
    with pytest.raises(EndOfFile):
        breakup_chunk(make_chunk(data, compression))
