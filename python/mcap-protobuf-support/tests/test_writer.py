from io import BytesIO

from mcap.reader import make_reader
from mcap_protobuf.writer import Writer
from mcap_protobuf.decoder import DecoderFactory

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration


def read_protobuf_messages(stream: BytesIO):
    return make_reader(
        stream, decoder_factories=[DecoderFactory()]
    ).iter_decoded_messages()


def test_write_one():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10), log_time=15)
    io.seek(0)

    items = list(read_protobuf_messages(io))
    assert len(items) == 1
    _, channel, message, decoded_message = items[0]
    assert decoded_message.seconds == 5
    assert decoded_message.nanos == 10
    assert message.log_time == 15
    assert channel.topic == "timestamps"


def test_write_wrong_schema():
    io = BytesIO()
    with Writer(io) as writer:
        writer.write_message("timestamps", Timestamp(seconds=5, nanos=10))
        with pytest.raises(ValueError):
            writer.write_message("timestamps", Duration(seconds=5, nanos=10))
