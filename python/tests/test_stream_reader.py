import contextlib
import time
from io import BytesIO, RawIOBase
from tempfile import TemporaryFile
from typing import cast

from mcap.mcap0.records import Channel, Message, Schema
from mcap.mcap0.stream_reader import StreamReader
from mcap.mcap0.writer import Writer
from std_msgs.msg import String  # type: ignore


@contextlib.contextmanager
def generate_sample_data():
    file = TemporaryFile("w+b")
    writer = Writer(file)
    writer.start(profile="ros1", library="test")
    string_schema_id = writer.register_schema(
        name=String._type, encoding="ros1", data=String._full_text.encode()  # type: ignore
    )
    string_channel_id = writer.register_channel(
        topic="chatter", message_encoding="ros1", schema_id=string_schema_id
    )

    for i in range(1, 11):
        s = String(data=f"string message {i}")
        buff = BytesIO()
        s.serialize(buff)  # type: ignore
        writer.add_message(
            channel_id=string_channel_id,
            log_time=time.time_ns(),
            data=buff.getvalue(),
            publish_time=time.time_ns(),
        )
    writer.finish()
    file.seek(0)

    yield file


def test_raw_read():
    with generate_sample_data() as t:
        reader = StreamReader(cast(RawIOBase, t))
        records = [r for r in reader.records]
        schemas = [r for r in records if isinstance(r, Schema)]
        assert len(schemas) == 2
        channels = [r for r in records if isinstance(r, Channel)]
        assert len(channels) == 2
        messages = [r for r in records if isinstance(r, Message)]
        assert len(messages) == 10


def test_decode_read():
    with generate_sample_data() as t:
        reader = StreamReader(cast(RawIOBase, t))
        messages = [r for r in reader.decoded_messages]
        assert len(messages) == 10
        assert messages[0].data == "string message 1"
