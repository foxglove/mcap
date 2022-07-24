from io import BytesIO
from pathlib import Path

from mcap_protobuf.writer import Writer
from mcap_protobuf.decoder import Decoder
from mcap.mcap0.stream_reader import StreamReader

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration


def test_write_one():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10))
    io.seek(0)

    messages = list(Decoder(StreamReader(io)).messages)
    assert len(messages) == 1
    topic, message = messages[0]
    assert message.seconds == 5
    assert message.nanos == 10
    assert topic == "timestamps"


def test_write_wrong_schema():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10))
        with pytest.raises(ValueError):
            writer.write_message("timestamps", Duration(seconds=5, nanos=10))
