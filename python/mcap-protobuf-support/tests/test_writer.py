from io import BytesIO

from mcap_protobuf.writer import Writer
from mcap_protobuf.decoder import decode_proto_messages
from mcap.mcap0.reader import make_reader

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration


def test_write_one():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10), log_time=15)
    io.seek(0)

    messages = list(decode_proto_messages(make_reader(io).iter_messages()))
    assert len(messages) == 1
    topic, message, timestamp = messages[0]
    assert message.seconds == 5
    assert message.nanos == 10
    assert timestamp == 15
    assert topic == "timestamps"


def test_write_wrong_schema():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10))
        with pytest.raises(ValueError):
            writer.write_message("timestamps", Duration(seconds=5, nanos=10))
