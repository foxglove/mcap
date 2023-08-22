import contextlib
from io import BytesIO
from tempfile import TemporaryFile

from mcap.writer import Writer


class String:
    _type = "std_msgs/msg/String"
    _full_text = "string data"

    def __init__(self, data: str):
        self.data = data

    def serialize(self, buff: BytesIO):
        buff.write(b"\x00\x03")  # CDR header (little-endian, 3)
        buff.write(b"\x00\x00")  # Alignment padding
        buff.write((len(self.data) + 1).to_bytes(4, "little"))  # String length
        buff.write(self.data.encode())  # String data
        buff.write(b"\x00")  # Null terminator


class Empty:
    _type = "std_msgs/msg/Empty"
    _full_text = ""

    def __init__(self):
        pass

    def serialize(self, buff: BytesIO):
        buff.write(b"\x00\x03")  # CDR header (little-endian, 3)
        buff.write(b"\x00\x00")  # Alignment padding
        buff.write(b"\x00")  # uint8 structure_needs_at_least_one_member


@contextlib.contextmanager
def generate_sample_data():
    file = TemporaryFile("w+b")
    writer = Writer(file)
    writer.start(profile="ros2", library="test")
    string_schema_id = writer.register_schema(
        name=String._type, encoding="ros2msg", data=String._full_text.encode()  # type: ignore
    )
    string_channel_id = writer.register_channel(
        topic="/chatter", message_encoding="cdr", schema_id=string_schema_id
    )

    for i in range(10):
        e = String(data=f"string message {i}")
        buff = BytesIO()
        e.serialize(buff)  # type: ignore
        writer.add_message(
            channel_id=string_channel_id,
            log_time=i * 1000,
            data=buff.getvalue(),
            publish_time=i * 1000,
        )

    empty_schema_id = writer.register_schema(
        name=Empty._type, encoding="ros2msg", data=Empty._full_text.encode()  # type: ignore
    )
    empty_channel_id = writer.register_channel(
        topic="/empty", message_encoding="cdr", schema_id=empty_schema_id
    )

    for i in range(10):
        e = Empty()
        buff = BytesIO()
        e.serialize(buff)  # type: ignore
        writer.add_message(
            channel_id=empty_channel_id,
            log_time=i * 1000,
            data=buff.getvalue(),
            publish_time=i * 1000,
        )

    writer.finish()
    file.seek(0)

    yield file
