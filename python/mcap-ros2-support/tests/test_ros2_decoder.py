from mcap.reader import make_reader
from mcap_ros2.decoder import Decoder

from .generate import generate_sample_data


def test_ros2_decoder():
    with generate_sample_data() as m:
        reader = make_reader(m)
        decoder = Decoder()
        count = 0
        for index, (schema, _, message) in enumerate(reader.iter_messages()):
            ros_msg = decoder.decode(schema, message)
            assert ros_msg.data == f"string message {index}"
            count += 1
        assert count == 10
