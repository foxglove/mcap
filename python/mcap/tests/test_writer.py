import contextlib
import json
from io import BytesIO
from tempfile import NamedTemporaryFile, TemporaryFile
from typing import List
import zlib
import cbor2
import subprocess

import lz4.frame
import pytest

from mcap.writer import CompressionType, Writer
from mcap.records import Chunk, ChunkIndex, Statistics
from mcap.stream_reader import StreamReader


@contextlib.contextmanager
def generate_sample_data(compression: CompressionType):
    file = TemporaryFile("w+b")
    writer = Writer(file, compression=compression)
    writer.start(library="test")
    schema_id = writer.register_schema(
        name="sample",
        encoding="jsonschema",
        data=json.dumps(
            {
                "type": "object",
                "properties": {
                    "sample": {
                        "type": "string",
                    }
                },
            }
        ).encode(),
    )

    channel_id = writer.register_channel(
        schema_id=schema_id,
        topic="sample_topic",
        message_encoding="json",
    )

    writer.add_message(
        channel_id=channel_id,
        log_time=0,
        data=json.dumps({"sample": "test"}).encode(encoding="utf-8"),
        publish_time=0,
    )

    writer.finish()
    file.seek(0)

    yield file


def test_lz4_chunks():
    """tests that compression metadata is correctly written to chunks and chunk indices."""
    chunks: List[Chunk] = []
    chunk_indexes: List[ChunkIndex] = []
    with generate_sample_data(CompressionType.LZ4) as t:
        for record in StreamReader(t, emit_chunks=True).records:
            if isinstance(record, Chunk):
                chunks.append(record)
            elif isinstance(record, ChunkIndex):
                chunk_indexes.append(record)

    assert len(chunks) == 1
    assert len(chunk_indexes) == 1

    for chunk, index in zip(chunks, chunk_indexes):
        assert index.compressed_size == len(chunk.data)
        assert index.uncompressed_size == chunk.uncompressed_size
        uncompressed_data: bytes = lz4.frame.decompress(chunk.data)
        assert chunk.uncompressed_size == len(uncompressed_data)
        assert chunk.uncompressed_crc == zlib.crc32(uncompressed_data)


@pytest.mark.parametrize(
    "compression_type,length", [(CompressionType.ZSTD, 741), (CompressionType.LZ4, 779)]
)
def test_decode_read(compression_type, length):
    """tests that chunk compression is happening when writing."""
    with generate_sample_data(compression_type) as t:
        data = t.read()
        assert len(data) == length


def test_out_of_order_messages():
    io = BytesIO()
    writer = Writer(io, compression=CompressionType.ZSTD)
    writer.start(library="test")
    schema_id = writer.register_schema("schema", "unknown", b"")
    channel_id = writer.register_channel("topic", "enc", schema_id)
    writer.add_message(channel_id, 100, b"a", 100)
    writer.add_message(channel_id, 0, b"b", 0)
    writer.add_message(channel_id, 1, b"c", 1)
    writer.finish()

    io.seek(0)
    reader = StreamReader(io)
    records = list(reader.records)

    statistics = next(r for r in records if isinstance(r, Statistics))
    assert statistics == Statistics(
        attachment_count=0,
        channel_count=1,
        channel_message_counts={channel_id: 3},
        chunk_count=1,
        message_count=3,
        schema_count=1,
        metadata_count=0,
        message_start_time=0,
        message_end_time=100,
    )

    chunk_index = next(r for r in records if isinstance(r, ChunkIndex))
    assert chunk_index.message_start_time == 0
    assert chunk_index.message_end_time == 100

@pytest.mark.parametrize(
    "null_schema,encoding", [(True, "cbor"), (False, "cbor"), (False, "json")]
)
def test_generate_sample_schemaless_data(null_schema: bool, encoding: str):
    file = NamedTemporaryFile("w+b")
    writer = Writer(file, compression=CompressionType.ZSTD)
    writer.start(library="test")
    schema_id = writer.register_schema(
        name="sample",
        encoding="",
        data="".encode()
    )

    if encoding == "cbor":
        channel_id = writer.register_channel(
            schema_id=0 if null_schema else schema_id,
            topic="sample_topic",
            message_encoding="cbor",
        )
        writer.add_message(
            channel_id=channel_id,
            log_time=0,
            data=cbor2.dumps({"sample": "test"}),
            publish_time=0,
        )
    elif encoding == "json":
        channel_id = writer.register_channel(
            schema_id=0 if null_schema else schema_id,
            topic="sample_topic",
            message_encoding="json",
        )
        writer.add_message(
            channel_id=channel_id,
            log_time=0,
            data=json.dumps({"sample": "test"}).encode("utf-8"),
            publish_time=0,
        )
    else:
        raise AssertionError()

    writer.finish()
    file.seek(0)
    result = subprocess.run(['mcap', 'cat', '--json', file.name], stdout=subprocess.PIPE)
    assert result.stdout == b'{"topic":"sample_topic","sequence":0,"log_time":0.000000000,"publish_time":0.000000000,"data":{"sample":"test"}}\n'