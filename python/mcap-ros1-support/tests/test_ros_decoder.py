from mcap.mcap0.reader import make_reader
from mcap_ros1.decoder import decode_ros1_messages

from .generate import generate_sample_data


def test_ros_decoder():
    with generate_sample_data() as m:
        reader = make_reader(m)
        ros_message_iterator = decode_ros1_messages(reader.iter_messages())
        count = 0
        for index, (topic, message, log_time) in enumerate(ros_message_iterator):
            assert topic == "/chatter"
            assert message.data == f"string message {index}"
            assert log_time == index * 1000
            count += 1
        assert count == 10
