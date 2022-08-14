from io import BytesIO

from mcap_protobuf.writer import Writer
from mcap_protobuf.reader import read_protobuf_messages

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration


def test_write_one():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10), log_time=15)
    io.seek(0)

    messages = list(read_protobuf_messages(io))
    assert len(messages) == 1
    message = messages[0]
    assert message.proto_msg.seconds == 5
    assert message.proto_msg.nanos == 10
    assert message.log_time_ns == 15
    assert message.topic == "timestamps"


def test_write_wrong_schema():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10))
        with pytest.raises(ValueError):
            writer.write_message("timestamps", Duration(seconds=5, nanos=10))
