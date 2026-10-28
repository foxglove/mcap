from io import BytesIO

import pytest

from mcap.exceptions import EndOfFile
from mcap.records import Chunk, Message
from mcap.stream_reader import StreamReader, breakup_chunk
from mcap.writer import CompressionType, Writer


def write_chunk() -> Chunk:
    output = BytesIO()
    writer = Writer(output, compression=CompressionType.NONE)
    writer.start()
    channel_id = writer.register_channel(
        topic="/t", message_encoding="raw", schema_id=0
    )
    writer.add_message(channel_id, log_time=1, data=b"first", publish_time=1)
    writer.add_message(channel_id, log_time=2, data=b"second", publish_time=2)
    writer.finish()
    output.seek(0)
    records = StreamReader(output, emit_chunks=True).records
    return next(r for r in records if isinstance(r, Chunk))


def test_breakup_chunk_raises_on_truncated_last_record():
    chunk = write_chunk()
    messages = [r for r in breakup_chunk(chunk) if isinstance(r, Message)]
    assert [m.data for m in messages] == [b"first", b"second"]

    # Cut the chunk a few bytes into the last message's payload.
    chunk.data = chunk.data[:-3]
    chunk.uncompressed_size = len(chunk.data)
    with pytest.raises(EndOfFile):
        breakup_chunk(chunk)
