import contextlib
import time
from io import BytesIO
from tempfile import TemporaryFile

from mcap.mcap0.writer import Writer
from std_msgs.msg import String  # type: ignore


@contextlib.contextmanager
def generate_sample_data():
    file = TemporaryFile("w+b")
    writer = Writer(file)
    writer.start(profile="ros1", library="test")
    string_schema_id = writer.register_schema(
        name=String._type, encoding="ros1msg", data=String._full_text.encode()  # type: ignore
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
