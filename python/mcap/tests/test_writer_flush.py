from io import BytesIO

from mcap.exceptions import EndOfFile
from mcap.reader import make_reader
from mcap.records import Message
from mcap.stream_reader import StreamReader
from mcap.writer import Writer


def test_flush_finishes_the_open_chunk_without_reaching_chunk_size():
    output = BytesIO()
    writer = Writer(output, chunk_size=1024 * 1024)
    writer.start()
    schema_id = writer.register_schema(name="s", encoding="jsonschema", data=b"{}")
    channel_id = writer.register_channel(
        topic="/t", message_encoding="json", schema_id=schema_id
    )
    for i in range(10):
        writer.add_message(channel_id, log_time=i, data=b"{}", publish_time=i)
    before = len(output.getvalue())
    writer.flush()
    after = len(output.getvalue())
    assert (
        after > before
    ), "flush() must write the open chunk even though chunk_size was not reached"
    # The bytes written so far are a readable prefix: a crash after this point loses nothing
    # of these ten messages. flush() writes no footer, so neither reader convenience class
    # applies (both require one, to find the summary section or to know the stream ended) --
    # read the low-level record stream directly and stop at the expected truncation.
    prefix = StreamReader(BytesIO(output.getvalue()), emit_chunks=False)
    messages = []
    try:
        for record in prefix.records:
            if isinstance(record, Message):
                messages.append(record)
    except EndOfFile:
        pass
    assert len(messages) == 10
    writer.finish()


def test_flush_with_no_open_chunk_writes_no_chunk():
    output = BytesIO()
    writer = Writer(output)
    writer.start()
    before = len(output.getvalue())
    writer.flush()
    assert len(output.getvalue()) == before
    writer.finish()
    summary = make_reader(BytesIO(output.getvalue())).get_summary()
    assert summary is not None and summary.statistics is not None
    assert summary.statistics.chunk_count == 0
