from io import BytesIO

from mcap.data_stream import RecordBuilder
from mcap.records import Statistics
from mcap.stream_reader import StreamReader


def test_statistics_serialization():
    s = Statistics(
        attachment_count=1,
        channel_count=2,
        channel_message_counts={
            3: 4,
            5: 6,
        },
        chunk_count=7,
        message_count=8,
        message_end_time=9,
        message_start_time=10,
        metadata_count=11,
        schema_count=12,
    )
    builder = RecordBuilder()

    s.write(builder)
    buf = builder.end()
    assert len(buf) > 0

    reader = StreamReader(input=BytesIO(buf), skip_magic=True)
    new_s = next(reader.records)
    assert s == new_s
