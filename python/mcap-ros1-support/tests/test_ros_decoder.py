from mcap.reader import make_reader
from mcap_ros1.decoder import DecoderFactory

from .generate import generate_sample_data


def test_ros_decoder():
    with generate_sample_data() as m:
        reader = make_reader(m, decoder_factories=[DecoderFactory()])
        count = 0

        for index, (_, _, _, ros_msg) in enumerate(reader.iter_decoded_messages()):
            assert ros_msg.data == f"string message {index}"
            count += 1
        assert count == 10
