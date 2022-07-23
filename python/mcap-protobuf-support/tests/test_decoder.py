import hashlib
from io import BytesIO, RawIOBase
from typing import cast

from mcap.mcap0.stream_reader import StreamReader
from mcap_protobuf.decoder import Decoder

from .generate import generate_sample_data


def test_protobuf_decoder():
    output = BytesIO()
    generate_sample_data(output)
    assert (
        hashlib.sha256(output.getvalue()).hexdigest()
        == "6d656a7fe9f2e591c97d51f872519bd159a9dcd1b4b1610809b4d8efb3b6ec5a"
    )
    reader = StreamReader(cast(RawIOBase, output))
    decoder = Decoder(reader)
    messages = [m for m in decoder.messages]
    assert len(messages) == 20
