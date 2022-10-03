from io import BytesIO

from mcap.reader import make_reader
from mcap_protobuf.decoder import Decoder

from .generate import generate_sample_data


def test_protobuf_decoder():
    output = BytesIO()
    generate_sample_data(output)

    decoder = Decoder()
    reader = make_reader(output)
    count = 0
    for schema, channel, message in reader.iter_messages():
        proto_msg = decoder.decode(schema, message)
        count += 1
        if channel.topic == "/complex_message":
            assert proto_msg.intermediate1.simple.data.startswith("Field A")
            assert proto_msg.intermediate2.simple.data.startswith("Field B")
        elif channel.topic == "/simple_message":
            assert proto_msg.data.startswith("Hello MCAP protobuf world")
        else:
            raise AssertionError(f"unrecognized topic {channel.topic}")

    assert count == 20
