import json
from io import BytesIO

from mcap.records import ChunkIndex
from mcap.stream_reader import StreamReader
from mcap.writer import Writer


def test_json_schema():
    output = BytesIO()
    writer = Writer(output)

    writer.start()

    schema_id = writer.register_schema(
        name="foxglove.FrameTransform",
        encoding="jsonschema",
        data=json.dumps(
            {
                "type": "object",
                "properties": {
                    "data": {
                        "type": "string",
                    },
                },
            }
        ).encode(),
    )

    channel_id = writer.register_channel(
        schema_id=schema_id,
        topic="/test",
        message_encoding="json",
    )

    writer.add_message(
        channel_id=channel_id,
        log_time=0,
        data=json.dumps(
            {
                "data": "testA",
            }
        ).encode("utf-8"),
        publish_time=0,
    )

    writer.add_message(
        channel_id=channel_id,
        log_time=1000000000,
        data=json.dumps(
            {
                "data": "testB",
            }
        ).encode("utf-8"),
        publish_time=1000000000,
    )

    writer.finish()

    output.seek(0)
    reader = StreamReader(output)
    chunk_index = [r for r in reader.records if isinstance(r, ChunkIndex)][0]

    assert chunk_index.message_start_time == 0
