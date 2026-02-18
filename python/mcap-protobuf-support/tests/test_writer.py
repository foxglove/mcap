from io import BytesIO

import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from mcap_protobuf.decoder import DecoderFactory
from mcap_protobuf.writer import Writer

from mcap.reader import make_reader


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


def test_write_metadata():
    output = BytesIO()
    writer = Writer(output=output)
    writer.add_metadata("test_metadata", {"key": "value"})
    writer.finish()

    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    for metadata in reader.iter_metadata():
        assert metadata.name == "test_metadata"
        assert metadata.metadata == {"key": "value"}


def test_write_attachment():
    output = BytesIO()
    writer = Writer(output=output)
    writer.add_attachment(10, 10, "test_attachment", "text/plain", b"test_data")
    writer.finish()

    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    for attachment in reader.iter_attachments():
        assert attachment.name == "test_attachment"
        assert attachment.media_type == "text/plain"
        assert attachment.data == b"test_data"
