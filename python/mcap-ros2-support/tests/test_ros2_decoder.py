from mcap_ros2.decoder import DecoderFactory

from mcap.reader import make_reader
from mcap.records import Schema

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


def test_ros2_decoder_msg_eq():
    with generate_sample_data() as m:
        reader = make_reader(m, decoder_factories=[DecoderFactory()])

        decoded_messages = reader.iter_decoded_messages("/chatter")
        _, _, _, msg0 = next(decoded_messages)
        _, _, _, msg1 = next(decoded_messages)
        assert msg0.data == "string message 0"
        assert msg1.data == "string message 1"
        assert msg0 == msg0 and msg1 == msg1
        assert msg0 != msg1 and msg1 != msg0


def test_ros2_decoder_char_is_unsigned():
    # std_msgs/msg/Char with data=200, as ROS 2 serializes it
    schema = Schema(
        id=1, name="std_msgs/msg/Char", encoding="ros2msg", data=b"char data"
    )
    decoder = DecoderFactory().decoder_for("cdr", schema)
    assert decoder(b"\x00\x01\x00\x00\xc8").data == 200
