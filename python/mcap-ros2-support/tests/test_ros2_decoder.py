from mcap_ros2.decoder import DecoderFactory

from mcap.reader import make_reader

from .generate import generate_sample_data


def test_ros2_decoder():
    with generate_sample_data() as m:
        reader = make_reader(m, decoder_factories=[DecoderFactory()])
        count = 0
        for index, (_, _, _, ros_msg) in enumerate(
            reader.iter_decoded_messages("/chatter")
        ):
            assert ros_msg.data == f"string message {index}"
            assert ros_msg._type == "std_msgs/String"
            assert ros_msg._full_text == "# std_msgs/String\nstring data"
            count += 1
        assert count == 10

        count = 0
        for _, _, _, ros_msg in reader.iter_decoded_messages("/empty"):
            assert ros_msg._type == "std_msgs/Empty"
            assert ros_msg._full_text == "# std_msgs/Empty"
            count += 1
        assert count == 10
