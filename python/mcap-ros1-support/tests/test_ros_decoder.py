from io import RawIOBase
from typing import cast
from mcap.mcap0.stream_reader import StreamReader
from mcap_ros1.decoder import Decoder

from .generate import generate_sample_data


def test_ros_decoder_from_stream():
    with generate_sample_data() as m:
        stream_reader = StreamReader(cast(RawIOBase, m))
        ros_reader = Decoder(stream_reader)
        messages = [m for m in ros_reader.messages]
        assert len(messages) == 10


def test_ros_decoder_from_bytes():
    with generate_sample_data() as m:
        data = m.read()
        messages = [m for m in Decoder(data).messages]
        assert len(messages) == 10
