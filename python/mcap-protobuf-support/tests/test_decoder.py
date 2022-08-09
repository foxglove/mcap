from io import BytesIO

from mcap.mcap0.reader import make_reader
from mcap_protobuf.decoder import decode_proto_messages

from .generate import generate_sample_data


def test_protobuf_decoder():
    output = BytesIO()
    generate_sample_data(output)

    reader = make_reader(output)
    message_iterator = reader.iter_messages(topics=["/complex_messages"])
    proto_iterator = decode_proto_messages(message_iterator)
    for index, (topic, message, timestamp) in enumerate(proto_iterator):
        assert topic == "/complex_messages"
        assert message.fieldA.startswith("Field A")
        assert message.fieldB.startswith("Field B")
        assert timestamp == index * 1000
