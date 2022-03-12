import contextlib
import json
from tempfile import TemporaryFile

from mcap.mcap0.writer import CompressionType, Writer


@contextlib.contextmanager
def generate_sample_data(compression: CompressionType):
    file = TemporaryFile("w+b")
    writer = Writer(file, compression=compression)
    writer.start(profile="x-json", library="test")
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
        data=json.dumps({"sample": "test"}).encode("utf-8"),
        publish_time=0,
    )

    writer.finish()
    file.seek(0)

    yield file


def test_raw_read():
    with generate_sample_data(CompressionType.LZ4) as t:
        data = t.read()
        assert len(data) == 785


def test_decode_read():
    with generate_sample_data(CompressionType.ZSTD) as t:
        data = t.read()
        assert len(data) == 747
