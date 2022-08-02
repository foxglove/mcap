from io import BytesIO, RawIOBase
from typing import cast

from mcap.mcap0.stream_reader import StreamReader
from mcap_protobuf.decoder import Decoder

from .generate import generate_sample_data


def test_protobuf_decoder():
    output = BytesIO()
    generate_sample_data(output)
    reader = StreamReader(cast(RawIOBase, output))
    decoder = Decoder(reader)
    messages = list(decoder.messages)
    assert len(messages) == 20
