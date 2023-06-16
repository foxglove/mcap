from io import BytesIO

from mcap_protobuf.decoder import DecoderFactory

from mcap.reader import make_reader

from .generate import generate_sample_data


def test_protobuf_decoder():
    output = BytesIO()
    generate_sample_data(output)

    decoder = DecoderFactory()
    reader = make_reader(output)
    count = 0
    for schema, channel, message in reader.iter_messages():
        proto_msg = decoder.decoder_for("protobuf", schema)(message.data)
        count += 1
        if channel.topic == "/complex_message":
            assert proto_msg.intermediate1.simple.data.startswith("Field A")
            assert proto_msg.intermediate2.simple.data.startswith("Field B")
        elif channel.topic == "/simple_message":
            assert proto_msg.data.startswith("Hello MCAP protobuf world")
        else:
            raise AssertionError(f"unrecognized topic {channel.topic}")

    assert count == 20


def test_decode_twice():
    output = BytesIO()
    generate_sample_data(output)
    # ensure that two decoders can exist and decode the same set of schemas
    # without failing with "A file with this name is already in the pool.".
    decoder_1 = DecoderFactory()
    decoder_2 = DecoderFactory()
    reader = make_reader(output)
    for schema, _, message in reader.iter_messages():
        decoder_1.decoder_for("protobuf", schema)(message.data)
        decoder_2.decoder_for("protobuf", schema)(message.data)
