from mcap.reader import make_reader
from mcap_ros2.decoder import Decoder

from .generate import generate_sample_data


def test_ros2_decoder():
    with generate_sample_data() as m:
        reader = make_reader(m)
        decoder = Decoder()
        count = 0
        for index, (schema, _, message) in enumerate(reader.iter_messages()):
            assert schema is not None
            ros_msg = decoder.decode(schema, message)
            assert ros_msg.data == f"string message {index}"
            assert ros_msg._type == "std_msgs/String"
            assert ros_msg._full_text == "# std_msgs/String\nstring data"
            count += 1
        assert count == 10
